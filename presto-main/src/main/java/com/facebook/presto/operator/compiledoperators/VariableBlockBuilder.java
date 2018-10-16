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

import com.facebook.presto.spi.block.AbstractVariableWidthBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlock;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.ceil;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class VariableBlockBuilder
        extends AbstractVariableWidthBlock
        implements BlockBuilder
{
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
    private static final double BLOCK_RESET_SKEW = 1.25;

    private static final int DEFAULT_CAPACITY = 64;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableBlockBuilder.class).instanceSize();

    private BlockBuilderStatus blockBuilderStatus;

    private boolean initialized;
    private int initialEntryCount;
    private int initialSliceOutputSize;

    private SliceOutput sliceOutput = new DynamicSliceOutput(0);

    // it is assumed that the offsets array is one position longer than the valueIsNull array
    private boolean[] valueIsNull = new boolean[0];
    private int[] offsets = new int[1];

    private int positions;
    private int currentEntrySize;

    private long arraysRetainedSizeInBytes;

    public VariableBlockBuilder(int expectedEntries, int expectedBytes)
    {
        this(new BlockBuilderStatus(), expectedEntries, expectedBytes);
    }

    public VariableBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this.blockBuilderStatus = blockBuilderStatus;

        initialEntryCount = expectedEntries;
        initialSliceOutputSize = min(expectedBytes, MAX_ARRAY_SIZE);

        updateArraysDataSize();
    }

    @Override
    protected int getPositionOffset(int position)
    {
        if (position >= positions) {
            throw new IllegalArgumentException("position " + position + " must be less than position count " + positions);
        }
        return getOffset(position);
    }

    @Override
    public int getSliceLength(int position)
    {
        if (position >= positions) {
            throw new IllegalArgumentException("position " + position + " must be less than position count " + positions);
        }
        return getOffset((position + 1)) - getOffset(position);
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        return sliceOutput.getUnderlyingSlice();
    }

    @Override
    public int getPositionCount()
    {
        return positions;
    }

    @Override
    public long getSizeInBytes()
    {
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positions;
        return sliceOutput.size() + arraysSizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " length " + length + " in block with " + positionCount + " positions");
        }
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        return getOffset(positionOffset + length) - getOffset(positionOffset) + arraysSizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sliceOutput.getRetainedSize() + arraysRetainedSizeInBytes;
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(sliceOutput, sliceOutput.getRetainedSize());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkValidPositions(positions, offset, length, this.positions);
        int finalLength = Arrays.stream(positions, offset, offset + length).map(this::getSliceLength).sum();
        SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
        int[] newOffsets = new int[length + 1];
        boolean[] newValueIsNull = new boolean[length];

        for (int i = 0; i < length; ++i) {
            int position = positions[offset + i];
            if (this.isEntryNull(position)) {
                newValueIsNull[i] = true;
            }
            else {
                newSlice.appendBytes(this.sliceOutput.getUnderlyingSlice().getBytes(this.getPositionOffset(position), this.getSliceLength(position)));
            }

            newOffsets[i + 1] = newSlice.size();
        }

        return new VariableWidthBlock(length, newSlice.slice(), newOffsets, newValueIsNull);
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeByte(value);
        currentEntrySize += SIZE_OF_BYTE;
        return this;
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeShort(value);
        currentEntrySize += SIZE_OF_SHORT;
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeInt(value);
        currentEntrySize += SIZE_OF_INT;
        return this;
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeLong(value);
        currentEntrySize += SIZE_OF_LONG;
        return this;
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeBytes(source, sourceIndex, length);
        currentEntrySize += length;
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        entryAdded(currentEntrySize, false);
        currentEntrySize = 0;
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        entryAdded(0, true);
        return this;
    }

    private void entryAdded(int bytesWritten, boolean isNull)
    {
        if (!initialized) {
            initializeCapacity();
        }
        if (valueIsNull.length <= positions) {
            growCapacity();
        }

        valueIsNull[positions] = isNull;
        offsets[positions + 1] = sliceOutput.size();

        positions++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_OF_BYTE + SIZE_OF_INT + bytesWritten);
        }
    }

    private void growCapacity()
    {
        int newSize = calculateNewArraySize(valueIsNull.length);
        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateArraysDataSize();
    }

    private void initializeCapacity()
    {
        if (positions != 0 || currentEntrySize != 0) {
            throw new IllegalStateException(getClass().getSimpleName() + " was used before initialization");
        }
        initialized = true;
        valueIsNull = new boolean[initialEntryCount];
        offsets = new int[initialEntryCount + 1];
        sliceOutput = new DynamicSliceOutput(initialSliceOutputSize);
        updateArraysDataSize();
    }

    private void updateArraysDataSize()
    {
        arraysRetainedSizeInBytes = sizeOf(valueIsNull) + sizeOf(offsets);
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull[position];
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException("Not yet supported");
    }

    public void resetFrom(int startOffset)
    {
        this.positions = startOffset;
        this.sliceOutput.reset(this.offsets[positions]);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        int[] newOffsets = Arrays.copyOfRange(offsets, positionOffset, positionOffset + length + 1);
        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        return new VariableWidthBlock(length, sliceOutput.slice(), newOffsets, newValueIsNull);
    }

    @Override
    public Block build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return new VariableWidthBlock(positions, sliceOutput.slice(), offsets, valueIsNull);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        int currentSizeInBytes = positions == 0 ? positions : (getOffset(positions) - getOffset(0));
        return new com.facebook.presto.spi.block.VariableWidthBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positions), calculateBlockResetBytes(currentSizeInBytes));
    }

    private int getOffset(int position)
    {
        return offsets[position];
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlockBuilder{");
        sb.append("positionCount=").append(positions);
        sb.append(", size=").append(sliceOutput.size());
        sb.append('}');
        return sb.toString();
    }

    static int calculateBlockResetSize(int currentSize)
    {
        long newSize = (long) ceil(currentSize * BLOCK_RESET_SKEW);

        // verify new size is within reasonable bounds
        if (newSize < DEFAULT_CAPACITY) {
            newSize = DEFAULT_CAPACITY;
        }
        else if (newSize > MAX_ARRAY_SIZE) {
            newSize = MAX_ARRAY_SIZE;
        }
        return (int) newSize;
    }

    static int calculateBlockResetBytes(int currentBytes)
    {
        long newBytes = (long) ceil(currentBytes * BLOCK_RESET_SKEW);
        if (newBytes > MAX_ARRAY_SIZE) {
            return MAX_ARRAY_SIZE;
        }
        return (int) newBytes;
    }

    static int calculateNewArraySize(int currentSize)
    {
        // grow array by 50%
        long newSize = currentSize + (currentSize >> 1);

        // verify new size is within reasonable bounds
        if (newSize < DEFAULT_CAPACITY) {
            newSize = DEFAULT_CAPACITY;
        }
        else if (newSize > MAX_ARRAY_SIZE) {
            newSize = MAX_ARRAY_SIZE;
            if (newSize == currentSize) {
                throw new IllegalArgumentException("Can not grow array beyond " + MAX_ARRAY_SIZE);
            }
        }
        return (int) newSize;
    }

    static void checkValidPositions(int[] positions, int offset, int length, int positionCount)
    {
        checkValidPositionsArray(positions, offset, length);

        for (int i = offset; i < offset + length; ++i) {
            int position = positions[i];
            if (position > positionCount) {
                throw new IllegalArgumentException(String.format("Invalid position '%s' in block with '%s' positions", position, positionCount));
            }
        }
    }

    static void checkValidPositionsArray(int[] positions, int offset, int length)
    {
        requireNonNull(positions, "positions array is null");
        if (offset >= 0 && offset <= positions.length) {
            if (length < 0 || offset + length > positions.length) {
                throw new IndexOutOfBoundsException(String.format("Invalid length '%s' for positions array with '%s' elements and offset: '%s", length, positions.length, offset));
            }
        }
        else {
            throw new IndexOutOfBoundsException(String.format("Invalid offset '%s' for positions array with '%s' elements", offset, positions.length));
        }
    }
}
