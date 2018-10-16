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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

public class PositionalBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RunLengthEncodedBlock.class).instanceSize();

    private final int position;
    private final Block block;
    private final int positionCount;
    private final long retainedSizeInBytes;

    public PositionalBlock(int positionCount, int position, Block block)
    {
        this.position = position;
        this.block = block;
        this.positionCount = positionCount;
        this.retainedSizeInBytes = INSTANCE_SIZE + block.getRetainedSizeInBytes();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return block.getSingleValueBlock(position);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return block.getRegionSizeInBytes(position, 1);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return this.block.getRegionSizeInBytes(this.position, 1);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
    }

    @Override
    public BlockEncoding getEncoding()
    {
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        return new PositionalBlock(length, position, block);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return new PositionalBlock(length, position, block);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        return new PositionalBlock(length, this.position, block);
    }

    @Override
    public boolean isNull(int position)
    {
        return block.isNull(this.position);
    }

    @Override
    public int getSliceLength(int position)
    {
        return block.getSliceLength(this.position);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return block.getByte(this.position, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return block.getShort(this.position, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return block.getInt(this.position, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return block.getLong(this.position, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return block.getSlice(this.position, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        return block.getObject(this.position, clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return block.bytesEqual(this.position, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return block.bytesCompare(this.position, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        this.block.writeBytesTo(this.position, offset, length, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        blockBuilder.writePositionTo(this.position, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return block.equals(this.position, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        return block.hash(this.position, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return block.compareTo(this.position, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("positionCount=").append(positionCount);
        sb.append(", value=").append(block);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void assureLoaded()
    {
        block.assureLoaded();
    }
}
