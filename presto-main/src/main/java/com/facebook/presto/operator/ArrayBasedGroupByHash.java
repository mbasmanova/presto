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
package com.facebook.presto.operator;

import com.facebook.presto.array.BooleanBigArray;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ArrayBasedGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBasedGroupByHash.class).instanceSize();

    private static final List<Type> TYPES = ImmutableList.of(BIGINT);

    private final int channel;
    private int maxGroupId = -1;
    private final BooleanBigArray present = new BooleanBigArray(false);

    public ArrayBasedGroupByHash(int channel)
    {
        this.channel = channel;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public long getHashCollisions()
    {
        return 0;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return 0;
    }

    @Override
    public List<Type> getTypes()
    {
        return TYPES;
    }

    @Override
    public int getGroupCount()
    {
        return maxGroupId + 1;
    }

    @Override
    public IntIterator getGroupIds()
    {
        int[] groupIds = new int[maxGroupId + 1];
        int groupCount = 0;
        for (int i = 0; i <= maxGroupId; i++) {
            if (present.get(i)) {
                groupIds[groupCount++] = i;
            }
        }
        return IntIterators.wrap(groupIds, 0, groupCount);
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        BIGINT.writeLong(pageBuilder.getBlockBuilder(outputChannelOffset), groupId);
    }

    @Override
    public Work<?> addPage(Page page)
    {
        return new AddPageWork(page);
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        return new GetGroupIdsWork(page);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        return false;
    }

    @Override
    public long getRawHash(int groupId)
    {
        return 0;
    }

    @Override
    public int getCapacity()
    {
        return 0;
    }

    private class AddPageWork
            implements Work<Void>
    {
        private final Page page;

        private AddPageWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            Block block = page.getBlock(channel);
            if (block.mayHaveNull()) {
                throw new UnsupportedOperationException();
            }
            else {
                for (int i = 0; i < positionCount; i++) {
                    int groupId = toIntExact(BIGINT.getLong(block, i));
                    assert groupId >= 0;
                    if (groupId > maxGroupId) {
                        maxGroupId = groupId;
                        present.ensureCapacity(maxGroupId + 1);
                    }
                    present.set(groupId, true);
                }
            }

            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    private class GetGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final Page page;

        private GetGroupIdsWork(Page page)
        {
            this.page = page;
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            Block block = page.getBlock(channel);
            if (block.mayHaveNull()) {
                throw new UnsupportedOperationException();
            }
            else {
                for (int i = 0; i < positionCount; i++) {
                    int groupId = toIntExact(BIGINT.getLong(block, i));
                    assert groupId >= 0;
                    if (groupId > maxGroupId) {
                        maxGroupId = groupId;
                        present.ensureCapacity(maxGroupId + 1);
                    }
                    present.set(groupId, true);
                }
            }
            return true;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            return new GroupByIdBlock(maxGroupId, page.getBlock(channel));
        }
    }
}
