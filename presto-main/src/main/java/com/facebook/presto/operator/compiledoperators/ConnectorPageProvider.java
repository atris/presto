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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.SplitOperatorInfo;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.split.EmptySplit;
import com.facebook.presto.split.EmptySplitPageSource;
import com.facebook.presto.split.PageSourceProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.MoreFutures;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ConnectorPageProvider
        implements PageInputProvider
{
    private final PageSourceProvider pageSourceProvider;
    private Split split;
    private boolean finished;
    private final OperatorContext operatorContext;
    private final SettableFuture<?> blocked = SettableFuture.create();
    private ConnectorPageSource pageSource;
    private final LocalMemoryContext systemMemoryContext;
    private long completedBytes;
    private long readTimeNanos;
    private final List<ColumnHandle> columns;

    public ConnectorPageProvider(PageSourceProvider pageSourceProvider, OperatorContext operatorContext, List<ColumnHandle> columns)
    {
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "PageSourceProvider is null");
        this.operatorContext = requireNonNull(operatorContext, "OperatorContext is null");
        this.systemMemoryContext = this.operatorContext.newLocalSystemMemoryContext();
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "ColumnHandles are null"));
    }

    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkState(this.split == null, "Table scan split already set");
        if (this.finished) {
            return Optional::empty;
        }
        else {
            this.split = split;
            Object var2 = split.getInfo();
            if (var2 != null) {
                this.operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(var2));
            }

            this.blocked.set(null);
            if (split.getConnectorSplit() instanceof EmptySplit) {
                this.pageSource = new EmptySplitPageSource();
            }

            return () -> this.pageSource instanceof UpdatablePageSource ? Optional.of((UpdatablePageSource) this.pageSource) : Optional.empty();
        }
    }

    public Page getPage()
    {
        if (this.split == null) {
            return null;
        }
        else {
            if (this.pageSource == null) {
                this.pageSource = this.pageSourceProvider.createPageSource(this.operatorContext.getSession(), this.split, this.columns);
            }

            Page page = this.pageSource.getNextPage();
            if (page != null) {
                page.assureLoaded();
                long var2 = this.pageSource.getCompletedBytes();
                long var4 = this.pageSource.getReadTimeNanos();
                this.operatorContext.recordGeneratedInput(var2 - this.completedBytes, (long) page.getPositionCount(), var4 - this.readTimeNanos);
                this.completedBytes = var2;
                this.readTimeNanos = var4;
            }

            this.systemMemoryContext.setBytes(this.pageSource.getSystemMemoryUsage());
            return page;
        }
    }

    public void noMoreSplits()
    {
        if (this.split == null) {
            this.finished = true;
        }

        this.blocked.set(null);
    }

    public boolean isFinished()
    {
        if (!this.finished) {
            this.finished = this.pageSource != null && this.pageSource.isFinished();
            if (this.pageSource != null) {
                this.systemMemoryContext.setBytes(this.pageSource.getSystemMemoryUsage());
            }
        }

        return this.finished;
    }

    public ListenableFuture<?> isBlocked()
    {
        if (!this.blocked.isDone()) {
            return this.blocked;
        }
        else if (this.pageSource != null) {
            CompletableFuture future = this.pageSource.isBlocked();
            return future.isDone() ? Operator.NOT_BLOCKED : MoreFutures.toListenableFuture(future);
        }
        else {
            return Operator.NOT_BLOCKED;
        }
    }

    public void close()
    {
        this.finished = true;
        this.blocked.set(null);
        if (this.pageSource != null) {
            try {
                this.pageSource.close();
            }
            catch (IOException var2) {
                throw new UncheckedIOException(var2);
            }
            this.systemMemoryContext.setBytes(this.pageSource.getSystemMemoryUsage());
        }
    }
}
