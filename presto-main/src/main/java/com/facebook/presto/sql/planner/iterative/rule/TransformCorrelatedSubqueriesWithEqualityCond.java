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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class TransformCorrelatedSubqueriesWithEqualityCond
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    private static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        if (aggregationNode.isStreamable()) {
            return false;
        }

        if (aggregationNode.getHashSymbol().isPresent()) {
            return false;
        }

        return aggregationNode.getStep() == SINGLE && aggregationNode.getGroupingSets().size() == 1;
    }

    private static boolean checkJoinPreconditions(JoinNode joinNode)
    {
        if (joinNode.getType() != JoinNode.Type.INNER) {
            return false;
        }

        List<JoinNode.EquiJoinClause> joinCriteria = joinNode.getCriteria();
        if (joinCriteria.isEmpty()) {
            return false;
        }

        for (int i = 0; i < joinCriteria.size(); i++) {
            JoinNode.EquiJoinClause currentClause = joinCriteria.get(i);
            ComparisonExpression currentExpression = currentClause.toExpression();

            if (currentExpression.getType() != ComparisonExpressionType.EQUAL) {
                return false;
            }
        }

        return true;
    }

    // This method should be called only after getAggregationNode has returned the node
    // getAggregationNode should have ensured that the aggregation node is a source of the join node
    private static boolean isAggregateOnLeftSideOfJoin(JoinNode joinNode, AggregationNode aggregationNode, Lookup lookup)
    {
        PlanNode leftSource = lookup.resolve(joinNode.getLeft());

        if (leftSource.equals(aggregationNode)) {
            return true;
        }

        return false;
    }

    private static AggregationNode getAggregationNode(JoinNode joinNode, Lookup lookup)
    {
        PlanNode leftSource = lookup.resolve(joinNode.getLeft());
        PlanNode rightSource = lookup.resolve(joinNode.getRight());

        if (leftSource instanceof AggregationNode) {
            return (AggregationNode) leftSource;
        }
        else if (rightSource instanceof AggregationNode) {
            return (AggregationNode) rightSource;
        }

        return null;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        AggregationNode aggregationNode = getAggregationNode(joinNode, context.getLookup());
        if (aggregationNode == null) {
            return Result.empty();
        }

        if (!(isSupportedAggregationNode(aggregationNode)) ||
                !(checkJoinPreconditions(joinNode))) {
            return Result.empty();
        }

        List<JoinNode.EquiJoinClause> joinCriteria = joinNode.getCriteria();
        List<Expression> joinCriteriaExpressions = new ArrayList<Expression>();
        for (int i = 0; i < joinCriteria.size(); i++) {
            JoinNode.EquiJoinClause currentClause = joinCriteria.get(i);
            ComparisonExpression currentExpression = currentClause.toExpression();

            joinCriteriaExpressions.add(currentExpression);
        }

        ComparisonExpression finalJoinCriteriaExpression = (ComparisonExpression) combineConjuncts(joinCriteriaExpressions);

        Symbol semiJoinResult = context.getSymbolAllocator().newSymbol("semijoin_result", BOOLEAN);

        SemiJoinNode replacement;
        boolean isAggregationOnLeftSideOfJoin = isAggregateOnLeftSideOfJoin(joinNode, aggregationNode, context.getLookup());

        if (isAggregationOnLeftSideOfJoin) {
            replacement = new SemiJoinNode(context.getIdAllocator().getNextId(),
                    aggregationNode.getSource(),
                    joinNode.getRight(),
                    Symbol.from(finalJoinCriteriaExpression.getLeft()),
                    Symbol.from(finalJoinCriteriaExpression.getRight()),
                    semiJoinResult,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }
        else {
            replacement = new SemiJoinNode(context.getIdAllocator().getNextId(),
                    joinNode.getLeft(),
                    aggregationNode.getSource(),
                    Symbol.from(finalJoinCriteriaExpression.getLeft()),
                    Symbol.from(finalJoinCriteriaExpression.getRight()),
                    semiJoinResult,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                replacement,
                new SimpleCaseExpression(
                        semiJoinResult.toSymbolReference(),
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, TRUE_LITERAL)),
                        Optional.empty()));

        Assignments.Builder assignments = Assignments.builder();
        assignments.putIdentities(joinNode.getOutputSymbols());

        PlanNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                assignments.build());

        return Result.ofPlanNode(projectNode);
    }
}
