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

import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.type.TypeUtils.NULL_HASH_CODE;
import static com.facebook.presto.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class BigintGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintGroupByHash.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;

    private final GroupByKeyProducer groupByKeyProducer;
    private final boolean outputRawHash;
    private final int inputHashChannel;

    private int hashCapacity;
    private int maxFill;
    private int mask;

    // the hash table from values to groupIds
    private LongBigArray values;
    private IntBigArray groupIds;

    // groupId for the null value
    private int nullGroupId = -1;

    // reverse index from the groupId back to the value
    private final LongBigArray valuesByGroupId;
    private final LongBigArray rawHashesByGroupId;

    private int nextGroupId;
    private long hashCollisions;
    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    public abstract static class GroupByKeyProducer
    {
        public abstract long getKey(Page page, int position);

        public abstract boolean isNull(Page page, int position);

        public abstract List<? extends Type> getTypes();

        public abstract void appendNullTo(PageBuilder pageBuilder, int outputChannelOffset);

        public abstract void appendValuesTo(long key, PageBuilder pageBuilder, int outputChannelOffset);
    }

    public static final class SingleGroupByKeyProducer
            extends GroupByKeyProducer
    {
        private final int channel;

        public SingleGroupByKeyProducer(int channel)
        {
            this.channel = channel;
        }

        public long getKey(Page page, int position)
        {
            return BIGINT.getLong(page.getBlock(channel), position);
        }

        public boolean isNull(Page page, int position)
        {
            return page.getBlock(channel).isNull(position);
        }

        public List<? extends Type> getTypes()
        {
            return ImmutableList.of(BIGINT);
        }

        public void appendNullTo(PageBuilder pageBuilder, int outputChannelOffset)
        {
            pageBuilder.getBlockBuilder(outputChannelOffset).appendNull();
        }

        public void appendValuesTo(long key, PageBuilder pageBuilder, int outputChannelOffset)
        {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            BIGINT.writeLong(blockBuilder, key);
        }
    }

    public static final class MultiGroupByKeyProducer
            extends GroupByKeyProducer
    {
        private final int[] channels;
        private final List<? extends Type> types;
        private final int[] offsets;

        public MultiGroupByKeyProducer(int[] channels, List<? extends Type> types)
        {
            this.channels = requireNonNull(channels, "channels is null");
            this.types = requireNonNull(types, "types is null");
            this.offsets = new int[channels.length + 1];

            int offset = 0;
            for (int i = 0; i < channels.length; i++) {
                Type type = types.get(i);
                if (TINYINT == type) {
                    offset += 1;
                }
                else if (SMALLINT == type) {
                    offset += 2;
                }
                else if (INTEGER == type) {
                    offset += 4;
                }
                else {
                    throw new IllegalArgumentException("Types of group by keys are too large to compress into a single long");
                }
                offsets[i + 1] = offset * 8;
            }

            if (offset > 8) {
                throw new IllegalArgumentException("Types of group by keys are too large to compress into a single long");
            }
        }

        public long getKey(Page page, int position)
        {
            long key = 0;
            for (int i = 0; i < channels.length; i++) {
                Block block = page.getBlock(channels[i]);

                // TODO Figure out how to handle nulls
                verify(!block.isNull(position));
                long value = types.get(i).getLong(block, position);
                int length = offsets[i + 1] - offsets[i];
                key |= (value & ((1L << length) - 1)) << offsets[i];
            }
            return key;
        }

        public boolean isNull(Page page, int position)
        {
            return false;
        }

        public List<? extends Type> getTypes()
        {
            return types;
        }

        public void appendNullTo(PageBuilder pageBuilder, int outputChannelOffset)
        {
            throw new UnsupportedOperationException();
        }

        public void appendValuesTo(long key, PageBuilder pageBuilder, int outputChannelOffset)
        {
            for (int i = 0; i < channels.length; i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + i);

                int length = offsets[i + 1] - offsets[i];
                long value = (key >> offsets[i]) & ((1L << length) - 1);
                Type type = types.get(i);
                if (INTEGER == type) {
                    blockBuilder.writeInt((int) value).closeEntry();
                }
                else if (SMALLINT == type) {
                    blockBuilder.writeShort((short) value).closeEntry();
                }
                else if (TINYINT == type) {
                    blockBuilder.writeByte((byte) value).closeEntry();
                }
                else {
                    verify(false);
                }
            }
        }
    }

    public BigintGroupByHash(GroupByKeyProducer groupByKeyProducer, Optional<Integer> inputHashChannel, int expectedSize, UpdateMemory updateMemory)
    {
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.groupByKeyProducer = requireNonNull(groupByKeyProducer, "groupByKeyProducer is null");
        this.outputRawHash = inputHashChannel.isPresent();
        this.inputHashChannel = inputHashChannel.orElse(-1);

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        values = new LongBigArray();
        values.ensureCapacity(hashCapacity);
        groupIds = new IntBigArray(-1);
        groupIds.ensureCapacity(hashCapacity);

        valuesByGroupId = new LongBigArray();
        valuesByGroupId.ensureCapacity(hashCapacity);

        if (outputRawHash) {
            rawHashesByGroupId = new LongBigArray();
            rawHashesByGroupId.ensureCapacity(hashCapacity);
        }
        else {
            rawHashesByGroupId = null;
        }

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                groupIds.sizeOf() +
                values.sizeOf() +
                valuesByGroupId.sizeOf() +
                (outputRawHash ? rawHashesByGroupId.sizeOf() : 0) +
                preallocatedMemoryInBytes;
    }

    @Override
    public long getHashCollisions()
    {
        return hashCollisions;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions + estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);
    }

    @Override
    public List<Type> getTypes()
    {
        return outputRawHash ? ImmutableList.copyOf(Iterables.concat(groupByKeyProducer.getTypes(), ImmutableList.of(BIGINT))) : ImmutableList.copyOf(groupByKeyProducer.getTypes());
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        checkArgument(groupId >= 0, "groupId is negative");
        if (groupId == nullGroupId) {
            groupByKeyProducer.appendNullTo(pageBuilder, outputChannelOffset);
        }
        else {
            groupByKeyProducer.appendValuesTo(valuesByGroupId.get(groupId), pageBuilder, outputChannelOffset);
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + groupByKeyProducer.getTypes().size());
            if (groupId == nullGroupId) {
                BIGINT.writeLong(hashBlockBuilder, NULL_HASH_CODE);
            }
            else {
                BIGINT.writeLong(hashBlockBuilder, rawHashesByGroupId.get(groupId));
            }
        }
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        return new AddPageWork(groupByKeyProducer, page);
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        return new GetGroupIdsWork(groupByKeyProducer, page);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        if (groupByKeyProducer.isNull(page, position)) {
            return nullGroupId >= 0;
        }

        long value = groupByKeyProducer.getKey(page, position);
        long hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds.get(hashPosition);
            if (groupId == -1) {
                return false;
            }
            else if (value == values.get(hashPosition)) {
                return true;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
    }

    @Override
    public long getRawHash(int groupId)
    {
        return rawHashesByGroupId.get(groupId);
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashCapacity;
    }

    private int putNullIfAbsent()
    {
        if (nullGroupId < 0) {
            // set null group id
            nullGroupId = nextGroupId++;
        }

        return nullGroupId;
    }

    private int putIfAbsent(long value, long rawHash)
    {
        long hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds.get(hashPosition);
            if (groupId == -1) {
                break;
            }

            if (value == values.get(hashPosition)) {
                return groupId;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
            hashCollisions++;
        }

        return addNewGroup(hashPosition, value, rawHash);
    }

    private int addNewGroup(long hashPosition, long value, long rawHash)
    {
        // record group id in hash
        int groupId = nextGroupId++;

        values.set(hashPosition, value);
        valuesByGroupId.set(groupId, value);
        if (outputRawHash) {
            rawHashesByGroupId.set(groupId, rawHash);
        }
        groupIds.set(hashPosition, groupId);

        // increase capacity, if necessary
        if (needRehash()) {
            tryRehash();
        }
        return groupId;
    }

    private boolean tryRehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for values, groupIds, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = (newCapacity - hashCapacity) * (long) (Long.BYTES + Integer.BYTES) + (calculateMaxFill(newCapacity) - maxFill) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);

        int newMask = newCapacity - 1;
        LongBigArray newValues = new LongBigArray();
        newValues.ensureCapacity(newCapacity);
        IntBigArray newGroupIds = new IntBigArray(-1);
        newGroupIds.ensureCapacity(newCapacity);

        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            if (groupId == nullGroupId) {
                continue;
            }
            long value = valuesByGroupId.get(groupId);

            // find an empty slot for the address
            long hashPosition = getHashPosition(value, newMask);
            while (newGroupIds.get(hashPosition) != -1) {
                hashPosition = (hashPosition + 1) & newMask;
                hashCollisions++;
            }

            // record the mapping
            newValues.set(hashPosition, value);
            newGroupIds.set(hashPosition, groupId);
        }

        mask = newMask;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        values = newValues;
        groupIds = newGroupIds;

        this.valuesByGroupId.ensureCapacity(maxFill);
        if (outputRawHash) {
            this.rawHashesByGroupId.ensureCapacity(maxFill);
        }
        return true;
    }

    private boolean needRehash()
    {
        return nextGroupId >= maxFill;
    }

    private static long getHashPosition(long rawHash, int mask)
    {
        return murmurHash3(rawHash) & mask;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private class AddPageWork
            implements Work<Void>
    {
        private final GroupByKeyProducer groupByKeyProducer;
        private final Page page;

        private int lastPosition;

        public AddPageWork(GroupByKeyProducer groupByKeyProducer, Page page)
        {
            this.groupByKeyProducer = requireNonNull(groupByKeyProducer, "groupByKeyProducer is null");
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // get the group for the current row
                if (groupByKeyProducer.isNull(page, lastPosition)) {
                    putNullIfAbsent();
                }
                else {
                    long rawHash = 0;
                    if (outputRawHash) {
                        rawHash = BIGINT.getLongUnchecked(page.getBlock(inputHashChannel), lastPosition);
                    }
                    putIfAbsent(groupByKeyProducer.getKey(page, lastPosition), rawHash);
                }
                lastPosition++;
            }
            return lastPosition == positionCount;
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
        private final BlockBuilder blockBuilder;
        private final GroupByKeyProducer groupByKeyProducer;
        private final Page page;

        private boolean finished;
        private int lastPosition;

        public GetGroupIdsWork(GroupByKeyProducer groupByKeyProducer, Page page)
        {
            this.groupByKeyProducer = requireNonNull(groupByKeyProducer, "groupByKeyProducer is null");
            this.page = requireNonNull(page, "page is null");
            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount());
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // output the group id for this row
                if (groupByKeyProducer.isNull(page, lastPosition)) {
                    BIGINT.writeLong(blockBuilder, putNullIfAbsent());
                }
                else {
                    long rawHash = 0;
                    if (outputRawHash) {
                        rawHash = BIGINT.getLongUnchecked(page.getBlock(inputHashChannel), lastPosition);
                    }
                    BIGINT.writeLong(blockBuilder, putIfAbsent(groupByKeyProducer.getKey(page, lastPosition), rawHash));
                }
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(nextGroupId, blockBuilder.build());
        }
    }
}
