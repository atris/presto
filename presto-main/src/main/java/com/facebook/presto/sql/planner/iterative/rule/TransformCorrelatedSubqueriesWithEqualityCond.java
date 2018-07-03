/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.esri.core.geometry.Operator;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.*;
import com.facebook.presto.sql.tree.*;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.Patterns.*;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.collect.Iterables.getOnlyElement;

public class TransformCorrelatedSubqueriesWithEqualityCond
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(TransformCorrelatedSubqueriesWithEqualityCond::isSupportedAggregationNode)
            .with(source().matching(join().capturedAs(JOIN)));

    //NOT WORKING
    /*private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(TransformCorrelatedSubqueriesWithEqualityCond::isSupportedAggregationNode)
            .with(source().matching(join().capturedAs(JOIN_NODE)));*/

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    private static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        // Don't split streaming aggregations
        if (aggregationNode.isStreamable()) {
            return false;
        }

        if (aggregationNode.getHashSymbol().isPresent()) {
            // TODO: add support for hash symbol in aggregation node
            return false;
        }

        return true;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        JoinNode join = captures.get(JOIN);
        if(join.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        List<JoinNode.EquiJoinClause> joinCriteria = join.getCriteria();
        if(joinCriteria.isEmpty()) {
            return Result.empty();
        }

        if(joinCriteria.size() > 1) {
            return Result.empty();
        }

        JoinNode.EquiJoinClause currentClause = joinCriteria.get(0);
        ComparisonExpression currentExpression = currentClause.toExpression();

        if(currentExpression.getType() != ComparisonExpressionType.EQUAL) {
            return Result.empty();
        }

        SemiJoinNode replacement = new SemiJoinNode(context.getIdAllocator().getNextId(),
                join.getLeft(),
                join.getRight(),
                Symbol.from(currentExpression.getLeft()),
                Symbol.from(currentExpression.getRight()),
                getOnlyElement(aggregationNode.getOutputSymbols()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Assignments assignments = Assignments.builder()
                .putIdentities(aggregationNode.getOutputSymbols())
                .build();

        ProjectNode resultProjectNode = new ProjectNode(
                                            context.getIdAllocator().getNextId(),
                                            replacement,
                                            assignments);

        return Result.ofPlanNode(aggregationNode.replaceChildren(ImmutableList.of(resultProjectNode)));
    }
}