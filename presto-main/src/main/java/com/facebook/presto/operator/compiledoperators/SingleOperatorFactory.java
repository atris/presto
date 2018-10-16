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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.operator.ChannelSet;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.operator.LookupJoinOperatorFactory;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.ReferenceCount;
import com.facebook.presto.operator.SetBuilderOperator;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateMaterializationJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SingleOperatorFactory
        implements SourceOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> types;
    private final PlanNodeId sourceNodeId;
    private final PlanNode sourceNode;
    private final Supplier<PageProcessor> pageProcessor;
    private final Optional<List<Information>> dataManagersList;
    private final Optional<List<ListenableFuture<ChannelSet>>> channelSetList;
    private final ReferenceCount operatorReferenceCount;
    private boolean closed;
    private final PageInputSupplier pageInputSupplier;

    public SingleOperatorFactory(int operatorId,
            PlanNodeId planNodeId,
            List<Type> types,
            PlanNode sourceNode,
            PageSourceProvider pageSourceProvider,
            ExchangeClientSupplier exchangeClientSupplier,
            PagesSerdeFactory serdeFactory,
            Supplier<PageProcessor> pageProcessor,
            Optional<List<LocalExecutionPlanner.JoinParameters>> lookupSourceFactoryManagers,
            Optional<List<SetBuilderOperator.SetSupplier>> setSuppliersList)
    {
        this.pageProcessor = pageProcessor;

        this.sourceNode = sourceNode;
        this.operatorId = operatorId;
        this.planNodeId = planNodeId;
        this.types = ImmutableList.copyOf(types);
        List<PlanNodeId> sourceIdList = new ArrayList<>();
        List<ColumnHandle> list = new ArrayList<>();
        if (lookupSourceFactoryManagers.isPresent()) {
            ImmutableList.Builder<Information> builder = ImmutableList.builder();
            for (LocalExecutionPlanner.JoinParameters parameters : lookupSourceFactoryManagers.get()) {
                builder.add(new Information(parameters.getJoinType(), new LookupJoinOperatorFactory.PerLifespanDataManager(parameters.getJoinType(), parameters.getManager())));
            }
            this.dataManagersList = Optional.of(builder.build());
        }
        else {
            this.dataManagersList = Optional.empty();
        }
        if (setSuppliersList.isPresent()) {
            ImmutableList.Builder<ListenableFuture<ChannelSet>> builder = ImmutableList.builder();
            for (SetBuilderOperator.SetSupplier supplier : setSuppliersList.get()) {
                builder.add(supplier.getChannelSet());
            }
            this.channelSetList = Optional.of(builder.build());
        }
        else {
            this.channelSetList = Optional.empty();
        }
        this.pageInputSupplier = sourceNode.accept(new PlanVisitor<PageInputSupplier, List<PlanNodeId>>()
        {
            @Override
            protected PageInputSupplier visitPlan(PlanNode node, List<PlanNodeId> context)
            {
                return Iterables.getOnlyElement(node.getSources()).accept(this, context);
            }

            @Override
            public PageInputSupplier visitJoin(JoinNode node, List<PlanNodeId> context)
            {
                return node.getLeft().accept(this, context);
            }

            @Override
            public PageInputSupplier visitSemiJoin(SemiJoinNode node, List<PlanNodeId> context)
            {
                return node.getSource().accept(this, context);
            }

            @Override
            public PageInputSupplier visitLateMaterializationJoin(LateMaterializationJoinNode joinNode, List<PlanNodeId> context)
            {
                return joinNode.getJoinNode().getLeft().accept(this, context);
            }

            @Override
            public PageInputSupplier visitTableScan(TableScanNode node, List<PlanNodeId> context)
            {
                context.add(node.getId());
                List<ColumnHandle> columns = node.getOutputSymbols().stream().map(x -> node.getAssignments().get(x)).collect(toImmutableList());
                return new ConnectorPageInputSupplier(pageSourceProvider, columns);
            }

            @Override
            public PageInputSupplier visitRemoteSource(RemoteSourceNode node, List<PlanNodeId> context)
            {
                context.add(node.getId());
                return new RemoteSourceInputSupplier(exchangeClientSupplier, serdeFactory);
            }
        }, sourceIdList);
        this.sourceNodeId = Iterables.getOnlyElement(sourceIdList);
        this.operatorReferenceCount = new ReferenceCount(1);
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceNodeId;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public SourceOperator createOperator(DriverContext driverContext)
    {
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceNodeId, SingleOperator.class.getSimpleName());
        Optional<List<SingleOperator.JoinInformation>> factories = Optional.empty();
        Runnable runnable;
        if (dataManagersList.isPresent()) {
            ImmutableList.Builder<SingleOperator.JoinInformation> factoryBuilder = ImmutableList.builder();
            ImmutableList.Builder<ReferenceCount> probeReferenceCount = ImmutableList.builder();
            for (Information information : dataManagersList.get()) {
                factoryBuilder.add(new SingleOperator.JoinInformation(information.getJoinNodeType(), information.getManager().getLookupSourceFactory(driverContext.getLifespan())));
                probeReferenceCount.add(information.getManager().getProbeReferenceCount(driverContext.getLifespan()));
                information.getManager().getProbeReferenceCount(driverContext.getLifespan()).retain();
            }
            factories = Optional.of(factoryBuilder.build());
            List<ReferenceCount> referenceCounts = probeReferenceCount.build();
            runnable = () -> referenceCounts.forEach(ReferenceCount::release);
        }
        else {
            runnable = () -> {};
        }
        operatorReferenceCount.retain();
        return new SingleOperator(sourceNodeId, types, operatorContext, pageInputSupplier.get(operatorContext), pageProcessor.get(), factories, channelSetList, runnable);
    }

    @Override
    public void noMoreOperators()
    {
        if (closed) {
            return;
        }
        closed = true;
        dataManagersList.ifPresent(x -> x.forEach(y -> y.getManager().noMoreLifespan()));
        this.operatorReferenceCount.release();
    }

    @Override
    public void noMoreOperators(Lifespan lifespan)
    {
        dataManagersList.ifPresent(x -> x.forEach(y -> y.getManager().getProbeReferenceCount(lifespan).release()));
    }

    private static class Information
    {
        private final LookupJoinOperators.JoinType joinNodeType;
        private final LookupJoinOperatorFactory.PerLifespanDataManager manager;

        public Information(LookupJoinOperators.JoinType joinNodeType, LookupJoinOperatorFactory.PerLifespanDataManager manager)
        {
            this.joinNodeType = requireNonNull(joinNodeType);
            this.manager = requireNonNull(manager);
        }

        public LookupJoinOperatorFactory.PerLifespanDataManager getManager()
        {
            return manager;
        }

        public LookupJoinOperators.JoinType getJoinNodeType()
        {
            return joinNodeType;
        }
    }
}
