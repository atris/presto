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

import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.operator.OperatorContext;

import static java.util.Objects.requireNonNull;

public class RemoteSourceInputSupplier
        implements PageInputSupplier
{
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final PagesSerdeFactory pagesSerdeFactory;
    private ExchangeClient exchangeClient;

    public RemoteSourceInputSupplier(ExchangeClientSupplier exchangeClientSupplier, PagesSerdeFactory pagesSerdeFactory)
    {
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "ExchangeClientSupplier is null");
        this.pagesSerdeFactory = requireNonNull(pagesSerdeFactory, "PageSerDeFactory is null");
    }

    public PageInputProvider get(OperatorContext operatorContext)
    {
        if (this.exchangeClient == null) {
            this.exchangeClient = this.exchangeClientSupplier.get(operatorContext.newLocalSystemMemoryContext());
        }

        return new RemoteSourcePageProvider(this.exchangeClient, this.pagesSerdeFactory.createPagesSerde(), operatorContext);
    }
}
