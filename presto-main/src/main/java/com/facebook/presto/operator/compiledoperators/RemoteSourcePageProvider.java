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

import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.split.RemoteSplit;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RemoteSourcePageProvider
        implements PageInputProvider
{
    private final ExchangeClient exchangeClient;
    private final PagesSerde serde;
    private final OperatorContext operatorContext;

    public RemoteSourcePageProvider(ExchangeClient exchangeClient, PagesSerde pagesSerde, OperatorContext operatorContext)
    {
        this.exchangeClient = requireNonNull(exchangeClient, "ExchangeClient is null");
        this.serde = requireNonNull(pagesSerde, "PageSerDe is null");
        this.operatorContext = requireNonNull(operatorContext, "OperatorContext is null");
    }

    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getConnectorId().equals(ExchangeOperator.REMOTE_CONNECTOR_ID), "split is not a remote split");
        URI uri = ((RemoteSplit) split.getConnectorSplit()).getLocation();
        this.exchangeClient.addLocation(uri);
        return Optional::empty;
    }

    public Page getPage()
    {
        SerializedPage page = this.exchangeClient.pollPage();
        if (page == null) {
            return null;
        }
        else {
            this.operatorContext.recordGeneratedInput(page.getSizeInBytes(), page.getPositionCount());
            return this.serde.deserialize(page);
        }
    }

    public void noMoreSplits()
    {
        this.exchangeClient.noMoreLocations();
    }

    public boolean isFinished()
    {
        return this.exchangeClient.isFinished();
    }

    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture var1 = this.exchangeClient.isBlocked();
        return var1.isDone() ? Operator.NOT_BLOCKED : var1;
    }

    public void close()
    {
        this.exchangeClient.close();
    }
}
