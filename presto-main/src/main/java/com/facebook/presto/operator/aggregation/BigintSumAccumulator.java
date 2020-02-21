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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.function.WindowIndex;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;

public class BigintSumAccumulator
        implements Accumulator
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(BigintSumAccumulator.class).instanceSize();

    private final int inputChannel;
    private boolean empty = true;
    private long sum;

    public BigintSumAccumulator(int inputChannel)
    {
        checkArgument(inputChannel >= 0, "Input channel must not be negative");
        this.inputChannel = inputChannel;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public Type getFinalType()
    {
        return BIGINT;
    }

    @Override
    public Type getIntermediateType()
    {
        return BIGINT;
    }

    @Override
    public void addInput(Page page)
    {
        Block block = page.getBlock(inputChannel);

        if (block instanceof RunLengthEncodedBlock && block.isNull(0)) {
            return;
        }

        int positionCount = page.getPositionCount();
        if (empty) {
            for (int i = 0; i < positionCount; i++) {
                if (!block.isNull(i)) {
                    empty = false;
                    break;
                }
            }

            if (empty) {
                return;
            }
        }

        for (int i = 0; i < positionCount; i++) {
            if (!block.isNull(i)) {
                sum += BIGINT.getLong(block, i);
            }
        }
    }

    @Override
    public void addInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addIntermediate(Block block)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void evaluateIntermediate(BlockBuilder blockBuilder)
    {
        if (empty) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, sum);
        }
    }

    @Override
    public void evaluateFinal(BlockBuilder blockBuilder)
    {
    }
}
