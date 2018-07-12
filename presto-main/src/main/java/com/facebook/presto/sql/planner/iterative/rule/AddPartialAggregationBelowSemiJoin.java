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

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.SymbolStatsEstimate;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isAllowOptimizationForSemiJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;

public class AddPartialAggregationBelowSemiJoin
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin();
    private static Map<PlanNodeId, QueryId> optimizedSemiJoinNodes = new HashMap<>();

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowOptimizationForSemiJoin(session);
    }

    @Override
    public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
    {
        QueryId associatedQueryId = optimizedSemiJoinNodes.get(semiJoinNode.getId());
        if (associatedQueryId == (context.getSession().getQueryId())) {
            return Result.empty();
        }

        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(semiJoinNode.getFilteringSource());
        SymbolStatsEstimate filteringSourceStatsEstimate = sourceStats.getSymbolStatistics(semiJoinNode.getFilteringSourceJoinSymbol());

        if (sourceStats.getOutputRowCount() == filteringSourceStatsEstimate.getDistinctValuesCount()) {
            return Result.empty();
        }

        AggregationNode aggregationNode = new AggregationNode(
                                            context.getIdAllocator().getNextId(),
                                            semiJoinNode.getFilteringSource(),
                                            ImmutableMap.of(),
                                            ImmutableList.of(ImmutableList.of(semiJoinNode.getFilteringSourceJoinSymbol())),
                                            ImmutableList.of(),
                                            AggregationNode.Step.SINGLE,
                                            Optional.empty(),
                                            Optional.empty());

        optimizedSemiJoinNodes.put(semiJoinNode.getId(), context.getSession().getQueryId());

        return Result.ofPlanNode(semiJoinNode.replaceChildren(ImmutableList.of(semiJoinNode.getSource(), aggregationNode)));
    }
}
