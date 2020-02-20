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
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.base.Preconditions.checkArgument;

public class DoubleSumGroupedAccumulator
        implements GroupedAccumulator
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DoubleSumAccumulator.class).instanceSize();

    private final int inputChannel;
    private DoubleBigArray sums;
    private BooleanBigArray nulls;

    public DoubleSumGroupedAccumulator(int inputChannel)
    {
        checkArgument(inputChannel >= 0, "Input channel must not be negative");
        this.inputChannel = inputChannel;
        this.sums = new DoubleBigArray();
        this.nulls = new BooleanBigArray(true);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + sums.sizeOf() + nulls.sizeOf();
    }

    @Override
    public Type getFinalType()
    {
        return DOUBLE;
    }

    @Override
    public Type getIntermediateType()
    {
        return DOUBLE;
    }

    @Override
    public void addInput(GroupByIdBlock groupIdsBlock, Page page)
    {
        sums.ensureCapacity(groupIdsBlock.getGroupCount());
        nulls.ensureCapacity(groupIdsBlock.getGroupCount());

        Block block = page.getBlock(inputChannel);
        if (block instanceof RunLengthEncodedBlock && block.isNull(0)) {
            return;
        }

        int positionCount = page.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            if (!block.isNull(i)) {
                long groupId = groupIdsBlock.getGroupIdUnchecked(i);
                sums.add(groupId, DOUBLE.getDouble(block, i));
                nulls.set(groupId, false);
            }
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
    }
}
