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
package com.facebook.presto.operator.singleoperator;

import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateMaterializationJoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import io.airlift.log.Logger;

public class SingleOperatorUtils
{
    private static final Logger LOGGER = Logger.get(SingleOperatorUtils.class);

    private SingleOperatorUtils()
    {
    }

    //Logic to determine
    public static boolean canBeReplacedBySingleOperator(PlanNode planNode, int threshold)
    {
        Result result = planNode.accept(new Visitor(), null);
        return result.canBeReplaced() && result.hasJoinOperator() || result.getTotalOperator() > threshold;
    }

    private static class Visitor
            extends PlanVisitor<Result, Void>
    {
        @Override
        protected Result visitPlan(PlanNode node, Void context)
        {
            LOGGER.info("Not implemented for the following node " + node.getClass());
            return new Result(false, false, -1);
        }

        @Override
        public Result visitAggregation(AggregationNode aggregationNode, Void context)
        {
            Result result = aggregationNode.getSource().accept(this, context);
            if (aggregationNode.getGroupingKeys().isEmpty()) {
                return visitPlan(aggregationNode, context);
            }
            return new Result(true, result.hasJoinOperator(), result.getTotalOperator() + 1);
        }

        @Override
        public Result visitTableScan(TableScanNode tableScanNode, Void context)
        {
            return new Result(true, false, 1);
        }

        @Override
        public Result visitRemoteSource(RemoteSourceNode remoteSource, Void context)
        {
            return new Result(true, false, 1);
        }

        @Override
        public Result visitFilter(FilterNode filterNode, Void context)
        {
            Result result = filterNode.getSource().accept(this, context);
            return new Result(result.canBeReplaced(), result.hasJoinOperator(), result.getTotalOperator() + 1);
        }

        @Override
        public Result visitProject(ProjectNode projectNode, Void context)
        {
            Result result = projectNode.getSource().accept(this, context);
            return new Result(result.canBeReplaced(), result.hasJoinOperator(), result.getTotalOperator() + 1);
        }

        @Override
        public Result visitJoin(JoinNode joinNode, Void context)
        {
            if (joinNode.isCrossJoin() || joinNode.getType().equals(JoinNode.Type.RIGHT) || joinNode.getType().equals(JoinNode.Type.FULL)) {
                return new Result(false, false, -1);
            }
            Result result = joinNode.getLeft().accept(this, context);
            return new Result(result.canBeReplaced, true, result.getTotalOperator() + 1);
        }

        @Override
        public Result visitLateMaterializationJoin(LateMaterializationJoinNode joinNode, Void context)
        {
            return joinNode.getJoinNode().accept(this, context);
        }

        @Override
        public Result visitTopN(TopNNode topNNode, Void context)
        {
            Result result = topNNode.getSource().accept(this, context);
            return new Result(result.canBeReplaced(), result.hasJoinOperator(), result.getTotalOperator() + 1);
        }

        @Override
        public Result visitLimit(LimitNode limitNode, Void context)
        {
            Result result = limitNode.getSource().accept(this, context);
            return new Result(result.canBeReplaced(), result.hasJoinOperator(), result.getTotalOperator() + 1);
        }
    }

    private static class Result
    {
        private final boolean canBeReplaced;
        private final boolean hasJoinOperator;
        private final int totalOperator;

        public Result(boolean canBeReplaced, boolean hasJoinOperator, int totalOperator)
        {
            this.canBeReplaced = canBeReplaced;
            this.hasJoinOperator = hasJoinOperator;
            this.totalOperator = totalOperator;
        }

        public boolean canBeReplaced()
        {
            return canBeReplaced;
        }

        public boolean hasJoinOperator()
        {
            return hasJoinOperator;
        }

        public int getTotalOperator()
        {
            return totalOperator;
        }
    }
}
