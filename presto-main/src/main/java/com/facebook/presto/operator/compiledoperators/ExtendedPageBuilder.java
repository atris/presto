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

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.util.List;

public class ExtendedPageBuilder
        extends PageBuilder
{
    private final BlockBuilder blockBuilder;

    public ExtendedPageBuilder(List<? extends Type> types, BlockBuilder blockBuilder)
    {
        super(1, types);
        this.blockBuilder = blockBuilder;
    }

    public BlockBuilder getBlockBuilder(int position)
    {
        return this.blockBuilder;
    }
}
