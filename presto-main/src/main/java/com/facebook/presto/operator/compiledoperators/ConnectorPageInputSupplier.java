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

import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.split.PageSourceProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ConnectorPageInputSupplier
        implements PageInputSupplier
{
    private final PageSourceProvider pageSourceProvider;
    private final List<ColumnHandle> columns;

    public ConnectorPageInputSupplier(PageSourceProvider pageSourceProvider, List<ColumnHandle> columns)
    {
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "PageSourceProvider is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "Columns is null"));
    }

    public PageInputProvider get(OperatorContext operatorContext)
    {
        return new ConnectorPageProvider(this.pageSourceProvider, operatorContext, this.columns);
    }
}
