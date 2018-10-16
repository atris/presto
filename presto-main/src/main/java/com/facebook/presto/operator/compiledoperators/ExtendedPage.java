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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;

public class ExtendedPage
{
    private final Block[] blocks;
    private final VariableBlockBuilder blockBuilder;
    private final int positionCount;

    public ExtendedPage(Page basePage, int maxVariable)
    {
        this.blocks = new Block[basePage.getChannelCount() + 1];

        for (int i = 0; i < basePage.getChannelCount(); ++i) {
            this.blocks[i] = basePage.getBlock(i);
        }

        this.blockBuilder = new VariableBlockBuilder(new BlockBuilderStatus(), maxVariable, maxVariable * 32);
        this.blocks[basePage.getChannelCount()] = this.blockBuilder;
        this.positionCount = basePage.getPositionCount();
    }

    public ExtendedPage(Block[] baseBlocks)
    {
        this.blocks = new Block[baseBlocks.length];
        for (int i = 0; i < baseBlocks.length; ++i) {
            this.blocks[i] = baseBlocks[i];
        }

        this.blockBuilder = null;
        this.positionCount = baseBlocks[0].getPositionCount();
    }

    public Block getBlock(int channel)
    {
        return this.blocks[channel];
    }

    public BlockBuilder getBlockBuilder()
    {
        return this.blockBuilder;
    }

    public Block[] getBlocks()
    {
        return this.blocks;
    }

    public void reset(int position)
    {
        this.blockBuilder.resetFrom(position);
    }

    public int getPositionCount()
    {
        return this.positionCount;
    }
}
