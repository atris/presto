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
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * This page builder creates pages with dictionary blocks:
 * normal dictionary blocks for the probe side and the original blocks for the build side.
 * <p>
 * TODO use dictionary blocks (probably extended kind) to avoid data copying for build side
 */
public class CodegenPageBuilder
{
    private final IntArrayList indexBuilder = new IntArrayList();
    private final int[] baseChannels;
    private final PageBuilder pageBuilder;
    private final int outputChannelCount;
    private int estimatedProbeBlockBytes;
    private Page page;
    private boolean isSequentialProbeIndices = true;

    public CodegenPageBuilder(List<Type> types, int[] baseChannels)
    {
        this.pageBuilder = new PageBuilder(requireNonNull(types, "buildTypes is null"));
        this.outputChannelCount = types.size();
        this.baseChannels = baseChannels;
    }

    public PageBuilder getPageBuilder()
    {
        return pageBuilder;
    }

    public BlockBuilder getBlockBuilder(int channel)
    {
        return pageBuilder.getBlockBuilder(channel);
    }

    public boolean isFull()
    {
        return estimatedProbeBlockBytes + pageBuilder.getSizeInBytes() >= DEFAULT_MAX_PAGE_SIZE_IN_BYTES || pageBuilder.isFull();
    }

    public boolean isEmpty()
    {
        return indexBuilder.isEmpty() && pageBuilder.isEmpty();
    }

    public void reset()
    {
        // be aware that probeIndexBuilder will not clear its capacity
        indexBuilder.clear();
        pageBuilder.reset();
        estimatedProbeBlockBytes = 0;
        isSequentialProbeIndices = true;
        this.page = null;
    }

    public void processPage(Page page)
    {
        this.page = page;
    }

    public Page buildPageWithoutFilter()
    {
        pageBuilder.declarePositions(page.getPositionCount());
        Block[] blocks = new Block[baseChannels.length + outputChannelCount];
        for (int i = 0; i < baseChannels.length; i++) {
            blocks[i] = page.getBlock(baseChannels[i]);
        }

        Page buildPage = pageBuilder.build();
        int offset = baseChannels.length;
        for (int i = 0; i < outputChannelCount; i++) {
            blocks[offset + i] = buildPage.getBlock(i);
        }
        return new Page(pageBuilder.getPositionCount(), blocks);
    }

    public Page build()
    {
        if (page == null) {
            return null;
        }
        int[] probeIndices = indexBuilder.toIntArray();
        int length = probeIndices.length;
        verify(pageBuilder.getPositionCount() == length);
        Block[] blocks = new Block[baseChannels.length + outputChannelCount];
        for (int i = 0; i < baseChannels.length; i++) {
            Block block = page.getBlock(baseChannels[i]);
            if (!isSequentialProbeIndices || length == 0) {
                blocks[i] = new DictionaryBlock(probeIndices.length, block, probeIndices);
            }
            else if (length == block.getPositionCount()) {
                // probeIndices are a simple covering of the block
                verify(probeIndices[0] == 0);
                verify(probeIndices[length - 1] == length - 1);
                blocks[i] = block;
            }
            else {
                // probeIndices are sequential without holes
                verify(probeIndices[length - 1] - probeIndices[0] == length - 1);
                blocks[i] = block.getRegion(probeIndices[0], length);
            }
        }

        Page buildPage = pageBuilder.build();
        int offset = baseChannels.length;
        for (int i = 0; i < outputChannelCount; i++) {
            blocks[offset + i] = buildPage.getBlock(i);
        }
        page = null;
        return new Page(pageBuilder.getPositionCount(), blocks);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("estimatedSize", estimatedProbeBlockBytes + pageBuilder.getSizeInBytes())
                .add("positionCount", pageBuilder.getPositionCount())
                .toString();
    }

    public void declarePosition()
    {
        pageBuilder.declarePosition();
    }

    public void declarePositions(int positions)
    {
        pageBuilder.declarePositions(positions);
    }

    public void appendRow(int position)
    {
        verify(position >= 0);
        int previousPosition = indexBuilder.isEmpty() ? -1 : indexBuilder.get(indexBuilder.size() - 1);
        // positions to be appended should be in ascending order
        verify(previousPosition <= position);
        isSequentialProbeIndices &= position == previousPosition + 1 || previousPosition == -1;

        // Update probe indices and size
        indexBuilder.add(position);
        estimatedProbeBlockBytes += Integer.BYTES;
        pageBuilder.declarePosition();

        // Update memory usage for probe side.
        //
        // The size of the probe cannot be easily calculated given
        // (1) the structure of Block is recursive,
        // (2) an inner block can serve as multiple views (e.g., in a dictionary block).
        //     Without a dedup at the granularity of rows, we cannot tell if we are overcounting, and
        // (3) even we are able to dedup magically, calling getRegionSizeInBytes can be expensive.
        //     For example, consider a dictionary block inside an array block;
        //     calling getRegionSizeInBytes(p, 1) of the array block can lead to calling getRegionSizeInBytes with an arbitrary length for the dictionary block,
        //     which is very expensive.
        //
        // To workaround the memory accounting complexity yet having a relatively reasonable estimation, we use sizeInBytes / positionCount as the size for each row.
        // It can be shown that the output page is bounded within range [buildPageBuilder.getSizeInBytes(), buildPageBuilder.getSizeInBytes + probe.getPage().getSizeInBytes()].
        //
        // This is under the assumption that the position of a probe is non-decreasing.
        // if position > previousPosition, we know it is a new row to append and we accumulate the estimated row size (sizeInBytes / positionCount);
        // otherwise we do not count because we know it is duplicated with the previous appended row.
        // So in the worst case, we can only accumulate up to the sizeInBytes of the probe page.
        //
        // On the other hand, we do not want to produce a page that is too small if the build size is too small (e.g., the build side is with all nulls).
        // That means we only appended a few small rows in the probe and reached the probe end.
        // But that is going to happen anyway because we have to flush the page whenever we reach the probe end.
        // So with or without precise memory accounting, the output page is small anyway.

        if (previousPosition == position) {
            return;
        }
    }
}
