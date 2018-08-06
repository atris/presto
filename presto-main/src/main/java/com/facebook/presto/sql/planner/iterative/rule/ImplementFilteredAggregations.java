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
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.base.Verify.verify;

/**
 * Implements filtered aggregations by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        F1(...) FILTER (WHERE C1(...)),
 *        F2(...) FILTER (WHERE C2(...))
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *        F1(...) mask ($0)
 *        F2(...) mask ($1)
 *     - Project
 *            &lt;identity projections for existing fields&gt;
 *            $0 = C1(...)
 *            $1 = C2(...)
 *         - X
 * </pre>
 */
public class ImplementFilteredAggregations
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(aggregation -> hasFilters(aggregation));

    private static boolean hasFilters(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .anyMatch(e -> e.getCall().getFilter().isPresent() &&
                        !e.getMask().isPresent()); // can't handle filtered aggregations with DISTINCT (conservatively, if they have a mask)
    }

    @Override
    public Pattern<AggregationNode> getPattern() {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        Assignments.Builder newAssignments = Assignments.builder();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        ImmutableList.Builder<Expression> filterClauses = ImmutableList.builder();
        ImmutableList.Builder<PlanNode> aggregationsExpressions = ImmutableList.builder();

        for (Map.Entry<Symbol, Aggregation> entry : aggregation.getAggregations().entrySet()) {
            Symbol output = entry.getKey();

            Assignments.Builder newLocalAssignments = Assignments.builder();
            ImmutableMap.Builder<Symbol, Aggregation> localAggregations = ImmutableMap.builder();
            // strip the filters
            FunctionCall call = entry.getValue().getCall();
            Optional<Symbol> mask = entry.getValue().getMask();

            if (call.getFilter().isPresent()) {
                Expression filter = call.getFilter().get();
                filterClauses.add(filter);
                Symbol symbol = context.getSymbolAllocator().newSymbol(filter, BOOLEAN);
                verify(!mask.isPresent(), "Expected aggregation without mask symbols, see Rule pattern");
                newAssignments.put(symbol, filter);
                newLocalAssignments.put(symbol, filter);
                mask = Optional.of(symbol);
            }
            aggregations.put(output, new Aggregation(
                    new FunctionCall(call.getName(), call.getWindow(), Optional.empty(), call.getOrderBy(), call.isDistinct(), call.getArguments()),
                    entry.getValue().getSignature(),
                    mask));

            localAggregations.put(output, new Aggregation(
                    new FunctionCall(call.getName(), call.getWindow(), Optional.empty(), call.getOrderBy(), call.isDistinct(), call.getArguments()),
                    entry.getValue().getSignature(),
                    mask));

            Expression filter = call.getFilter().get();
            PlanNode newSource = aggregation.getSource();
            if (!(aggregationsExpressions.build().isEmpty()))
            {
                newSource = cloneAndAddNewId(aggregation.getSource(), context);
            }

            newLocalAssignments.putIdentities(aggregation.getSource().getOutputSymbols());
            AggregationNode aggregationNode = new AggregationNode(
                    context.getIdAllocator().getNextId(),
                    new FilterNode(
                            context.getIdAllocator().getNextId(),
                            new ProjectNode(
                                    context.getIdAllocator().getNextId(),
                                    newSource,
                                    newLocalAssignments.build()),
                            filter),
                    localAggregations.build(),
                    aggregation.getGroupingSets(),
                    aggregation.getStep(),
                    aggregation.getHashSymbol(),
                    aggregation.getGroupIdSymbol());

            aggregationsExpressions.add(aggregationNode);
        }

        // identity projection for all existing inputs
        newAssignments.putIdentities(aggregation.getSource().getOutputSymbols());

        UnionNode unionNode = union(
                aggregationsExpressions.build(),
                aggregations.build().keySet(),
                context
        );

        PlanNode newSource = cloneAndAddNewId(aggregation.getSource(), context);
        if (unionNode.getSources().size() < 2) {
            return Result.ofPlanNode(
                    new AggregationNode(
                            context.getIdAllocator().getNextId(),
                            new FilterNode(
                                    context.getIdAllocator().getNextId(),
                                    new ProjectNode(
                                            context.getIdAllocator().getNextId(),
                                            newSource,
                                            newAssignments.build()),
                                    combineConjuncts(filterClauses.build())),
                            aggregations.build(),
                            aggregation.getGroupingSets(),
                            aggregation.getStep(),
                            aggregation.getHashSymbol(),
                            aggregation.getGroupIdSymbol()));
        }

        return Result.ofPlanNode(unionNode);
    }

    private UnionNode union(ImmutableList<PlanNode> nodes, ImmutableSet<Symbol> outputs, Context context)
    {
        ImmutableList<Symbol> outputSymbolsAsList = outputs.asList();
        ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputs = ImmutableListMultimap.builder();
        for (int i = 0; i < outputSymbolsAsList.size(); i++) {
            for (PlanNode source : nodes) {
                for (int j = 0; j < source.getOutputSymbols().size(); j++) {
                    outputsToInputs.put(outputSymbolsAsList.get(i), source.getOutputSymbols().get(j));
                }
            }
        }

        return new UnionNode(context.getIdAllocator().getNextId(), nodes, outputsToInputs.build(), outputSymbolsAsList);
    }

    private PlanNode cloneAndAddNewId(PlanNode planNode, Context context)
    {
        PlanNode unaliasedPlanNode = planNode;
        if (planNode instanceof GroupReference)
        {
            unaliasedPlanNode = context.getLookup().resolve(planNode);
        }
        if (unaliasedPlanNode instanceof ProjectNode)
        {
            ProjectNode projectNode = (ProjectNode) unaliasedPlanNode;
            return new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    cloneAndAddNewId(projectNode.getSource(), context),
                    projectNode.getAssignments()
            );
        }
        else if (unaliasedPlanNode instanceof ValuesNode)
        {
            ValuesNode valuesNode = (ValuesNode) unaliasedPlanNode;
            return new ValuesNode(
                    context.getIdAllocator().getNextId(),
                    valuesNode.getOutputSymbols(),
                    valuesNode.getRows()
                    );
        }

        TableScanNode tableScanNode = (TableScanNode) unaliasedPlanNode;
        return new TableScanNode(
                context.getIdAllocator().getNextId(),
                tableScanNode.getTable(),
                tableScanNode.getOutputSymbols(),
                tableScanNode.getAssignments(),
                tableScanNode.getLayout(),
                tableScanNode.getCurrentConstraint(),
                tableScanNode.getOriginalConstraint()
        );
    }
}
