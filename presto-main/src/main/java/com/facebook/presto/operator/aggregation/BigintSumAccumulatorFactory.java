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

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class BigintSumAccumulatorFactory
        implements AccumulatorFactory
{
    private final int inputChannel;

    public BigintSumAccumulatorFactory(int inputChannel)
    {
        checkArgument(inputChannel >= 0, "Input channel must not be negative");
        this.inputChannel = inputChannel;
    }

    @Override
    public List<Integer> getInputChannels()
    {
        return ImmutableList.of(inputChannel);
    }

    @Override
    public Accumulator createAccumulator()
    {
        return new BigintSumAccumulator(inputChannel);
    }

    @Override
    public Accumulator createIntermediateAccumulator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public GroupedAccumulator createGroupedAccumulator()
    {
        return new BigintSumGroupedAccumulator(inputChannel);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasOrderBy()
    {
        return false;
    }

    @Override
    public boolean hasDistinct()
    {
        return false;
    }
}
