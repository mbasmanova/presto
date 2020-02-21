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

import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.RealType.REAL;
import static java.lang.Float.floatToIntBits;
import static org.openjdk.jmh.annotations.Level.Invocation;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkRealSumGroupedAccumulator
{
    @Benchmark
    @OperationsPerInvocation(1_000)
    public void benchmark(BenchmarkData data)
    {
        data.getAccumulator().addInput(data.getGroupByIdBlock(), data.getPage());
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int POSITIONS_PER_PAGE = 1024;
        private static final int GROUP_COUNT = 1_000;

        private Page page;
        private GroupByIdBlock groupByIdBlock;
        private GroupedAccumulator accumulator;

        @Param({"3", "15", "50"})
        private int nullFraction;

        @Setup(Invocation)
        public void setup()
        {
            Random random = new Random(0);

            // Make a block of floats some percentage of rows being null
            BlockBuilder blockBuilder = REAL.createFixedSizeBlockBuilder(POSITIONS_PER_PAGE);
            for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
                if (random.nextInt(100) < nullFraction) {
                    REAL.writeLong(blockBuilder, floatToIntBits(random.nextFloat()));
                }
                else {
                    blockBuilder.appendNull();
                }
            }

            page = new Page(blockBuilder.build());

            // Make group-id block
            BlockBuilder groupIdBuilder = BIGINT.createFixedSizeBlockBuilder(POSITIONS_PER_PAGE);
            for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
                BIGINT.writeLong(groupIdBuilder, random.nextInt(GROUP_COUNT));
            }

            groupByIdBlock = new GroupByIdBlock(GROUP_COUNT, groupIdBuilder.build());

            accumulator = new RealSumGroupedAccumulator(0);
        }

        public GroupedAccumulator getAccumulator()
        {
            return accumulator;
        }

        public Page getPage()
        {
            return page;
        }

        public GroupByIdBlock getGroupByIdBlock()
        {
            return groupByIdBlock;
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.nullFraction = 5;
        data.setup();
        data.getAccumulator().addInput(data.getGroupByIdBlock(), data.getPage());
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.BULK)
                .include(".*" + BenchmarkRealSumGroupedAccumulator.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
