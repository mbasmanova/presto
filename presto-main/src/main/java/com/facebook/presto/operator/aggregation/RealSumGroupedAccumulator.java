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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.array.BooleanBigArray;
import com.facebook.presto.array.DoubleBigArray;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader.ValueConsumer;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_BASE_OFFSET;

public class RealSumGroupedAccumulator
        implements GroupedAccumulator
{
    private static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(RealSumGroupedAccumulator.class).instanceSize();

    private final int inputChannel;
    private DoubleBigArray sums;
    private BooleanBigArray nulls;

    public RealSumGroupedAccumulator(int inputChannel)
    {
        checkArgument(inputChannel >= 0, "Input channel must not be negative");
        this.inputChannel = inputChannel;
        sums = new DoubleBigArray();
        nulls = new BooleanBigArray(true);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + sums.sizeOf() + nulls.sizeOf();
    }

    @Override
    public Type getFinalType()
    {
        return REAL;
    }

    @Override
    public Type getIntermediateType()
    {
        return DOUBLE;
    }

    @Override
    public void addInput(GroupByIdBlock groupIdsBlock, Page page)
    {
        int groupCount = toIntExact(groupIdsBlock.getGroupCount());
        sums.ensureCapacity(groupCount);
        nulls.ensureCapacity(groupCount);

        Block block = page.getBlock(inputChannel);
        if (block instanceof RunLengthEncodedBlock && block.isNull(0)) {
            return;
        }

        if (block instanceof LazyBlock) {
            LazyBlock lazyBlock = (LazyBlock) block;
            if (!lazyBlock.isLoaded()) {
                lazyBlock.load(new Consumer(groupIdsBlock), false);
                return;
            }
        }

        int positionCount = page.getPositionCount();

        if (block instanceof RunLengthEncodedBlock) {
            float value = intBitsToFloat(block.getInt(0));
            for (int i = 0; i < positionCount; i++) {
                int groupId = toIntExact(groupIdsBlock.getGroupIdUnchecked(i));
                sums.add(groupId, value);
                this.nulls.set(groupId, false);
            }
            return;
        }

        boolean[] nulls = ((IntArrayBlock) block).getValueIsNull();
        if (nulls == null) {
            for (int i = 0; i < positionCount; i++) {
                int groupId = toIntExact(groupIdsBlock.getGroupIdUnchecked(i));
                sums.add(groupId, intBitsToFloat(block.getIntUnchecked(i)));
                this.nulls.set(groupId, false);
            }
        }
        else {
            for (int offset = 0; offset + 8 < positionCount; offset += 8) {
                long flags = unsafe.getLong(nulls, (long) ARRAY_BOOLEAN_BASE_OFFSET + offset) ^ 0x0101010101010101L;
                while (flags != 0) {
                    int low = Long.numberOfTrailingZeros(flags) >> 3;
                    flags &= flags - 1;

                    int groupId = toIntExact(groupIdsBlock.getGroupIdUnchecked(offset + low));
                    sums.add(groupId, intBitsToFloat(block.getIntUnchecked(offset + low)));
                    this.nulls.set(groupId, false);
                }
            }
        }
    }

    private final class Consumer
            extends ValueConsumer
    {
        private final GroupByIdBlock groupByIdBlock;

        private Consumer(GroupByIdBlock groupByIdBlock)
        {
            this.groupByIdBlock = groupByIdBlock;
        }

        @Override
        public void acceptFloat(int position, float value)
        {
            long groupId = groupByIdBlock.getGroupIdUnchecked(position);
            sums.add(groupId, value);
            nulls.set(groupId, false);
        }
    }

    @Override
    public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void evaluateIntermediate(int groupId, BlockBuilder output)
    {
        if (nulls.get(groupId)) {
            output.appendNull();
        }
        else {
            DOUBLE.writeDouble(output, sums.get(groupId));
        }
    }

    @Override
    public void evaluateFinal(int groupId, BlockBuilder output)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void prepareFinal()
    {
        // nothing to do
    }
}
