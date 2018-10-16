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

import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.ChannelSet;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.LookupSourceFactory;
import com.facebook.presto.operator.LookupSourceProvider;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.util.Objects.requireNonNull;

public class SingleOperator
        implements SourceOperator
{
    private static final Logger LOGGER = Logger.get(SourceOperator.class);
    private final List<Type> outputTypes;
    private final PlanNodeId planNodeId;
    private final OperatorContext operatorContext;
    private final PageInputProvider pageInputProvider;
    private final PageProcessor pageProcessor;
    private final boolean noJoins;
    private final ListenableFuture future;
    private final Runnable onClose;
    private List<ListenableFuture<LookupSourceProvider>> lookupSourceProviderFuture;
    private List<ListenableFuture<ChannelSet>> channelSetFuture;
    private List<LookupSourceProvider> lookupSourceProvider;
    private List<LookupJoinOperators.JoinType> joinTypes;
    private boolean finished;
    private boolean loaded;
    private BlockingQueue<Page> initialBuffer = new ArrayBlockingQueue<>(1000);
    private long bufferSize;

    public SingleOperator(PlanNodeId planNodeId,
            List<Type> outputTypes,
            OperatorContext operatorContext,
            PageInputProvider pageInputProvider,
            PageProcessor pageProcessor,
            Optional<List<JoinInformation>> joinInformation,
            Optional<List<ListenableFuture<ChannelSet>>> channelSetFuture,
            Runnable onclose)
    {
        this.planNodeId = requireNonNull(planNodeId, "PlanNodeId is null");
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "OuputTypes is null"));
        this.operatorContext = requireNonNull(operatorContext, "OperatorContext is null");
        this.pageInputProvider = pageInputProvider;
        this.pageProcessor = pageProcessor;
        noJoins = !joinInformation.isPresent() && !channelSetFuture.isPresent();
        if (!noJoins) {
            ListenableFuture<?> forJoin = NOT_BLOCKED;
            ListenableFuture<?> forSemiJoin = NOT_BLOCKED;
            if (joinInformation.isPresent()) {
                this.lookupSourceProviderFuture = requireNonNull(joinInformation.get().stream().map(JoinInformation::getLookupSourceFactory).map(LookupSourceFactory::createLookupSourceProvider).collect(toImmutableList()));
                this.joinTypes = requireNonNull(joinInformation.get().stream().map(JoinInformation::getJoinType).collect(toImmutableList()));
                forJoin = Futures.allAsList(this.lookupSourceProviderFuture);
            }
            if (channelSetFuture.isPresent()) {
                this.channelSetFuture = channelSetFuture.get();
                forSemiJoin = Futures.allAsList(this.channelSetFuture);
            }
            this.future = Futures.allAsList(forJoin, forSemiJoin);
        }
        else {
            this.future = NOT_BLOCKED;
        }
        this.onClose = onclose;
        this.finished = false;
        this.loaded = false;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        return pageInputProvider.addSplit(split);
    }

    @Override
    public void noMoreSplits()
    {
        pageInputProvider.noMoreSplits();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return outputTypes;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    private boolean tryFetchLookupSourceProvider()
    {
        if (!noJoins && !loaded) {
            if (!future.isDone()) {
                return false;
            }
            lookupSourceProvider = new ArrayList<>();
            if (lookupSourceProviderFuture != null) {
                for (ListenableFuture<LookupSourceProvider> future : lookupSourceProviderFuture) {
                    lookupSourceProvider.add(requireNonNull(getDone(future)));
                }
                LookupSource[] lookupSources = new LookupSource[lookupSourceProvider.size()];
                for (int i = 0; i < lookupSources.length; i++) {
                    lookupSources[i] = lookupSourceProvider.get(i).withLease(lookupSourceLease -> lookupSourceLease.getLookupSource());
                    if (lookupSources[i].getJoinPositionCount() == 0 && joinTypes.get(i) == LookupJoinOperators.JoinType.INNER) {
                        finish();
                        break;
                    }
                }
                pageProcessor.setLookupSource(lookupSources);
            }
            if (channelSetFuture != null) {
                ChannelSet[] channelSets = new ChannelSet[channelSetFuture.size()];
                for (int i = 0; i < channelSets.length; i++) {
                    channelSets[i] = getDone(channelSetFuture.get(i));
                }
                pageProcessor.setChannelSet(channelSets);
            }
            loaded = true;
        }
        return true;
    }

    @Override
    public Page getOutput()
    {
        if (isFinished()) {
            return null;
        }

        Page page = pageInputProvider.getPage();
        if (page != null && initialBuffer.size() < 1000) {
            initialBuffer.add(page);
            bufferSize = bufferSize + page.getSizeInBytes();
        }

        if (!tryFetchLookupSourceProvider() || isFinished()) {
            return null;
        }
        Page page2 = initialBuffer.poll();
        if (page2 != null) {
            bufferSize = bufferSize - page2.getSizeInBytes();
            pageProcessor.addInputPage(page2);
        }

        if (pageInputProvider.isFinished() && initialBuffer.isEmpty()) {
            pageProcessor.noMorePage();
            finished = true;
        }

        return pageProcessor.getOutputPage();
    }

    @Override
    public void finish()
    {
        pageInputProvider.close();
    }

    @Override
    public boolean isFinished()
    {
        return pageProcessor.isFinished() || (finished && pageInputProvider.isFinished() && initialBuffer.size() == 0) && pageProcessor.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (noJoins || (bufferSize < DEFAULT_MAX_PAGE_SIZE_IN_BYTES * 16 && initialBuffer.size() < 1000 && !pageInputProvider.isFinished())) {
            return Futures.allAsList(NOT_BLOCKED, pageInputProvider.isBlocked());
        }
        return Futures.allAsList(future, pageInputProvider.isBlocked());
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            if (!noJoins && lookupSourceProvider != null) {
                for (LookupSourceProvider provider : lookupSourceProvider) {
                    closer.register(provider::close);
                }
            }
            closer.register(onClose::run);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class JoinInformation
    {
        private final LookupJoinOperators.JoinType joinType;
        private final LookupSourceFactory lookupSourceFactory;

        public JoinInformation(LookupJoinOperators.JoinType joinType, LookupSourceFactory lookupSourceFactory)
        {
            this.joinType = requireNonNull(joinType);
            this.lookupSourceFactory = requireNonNull(lookupSourceFactory);
        }

        public LookupJoinOperators.JoinType getJoinType()
        {
            return joinType;
        }

        public LookupSourceFactory getLookupSourceFactory()
        {
            return lookupSourceFactory;
        }
    }
}
