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

import com.facebook.presto.operator.BigintGroupByHash.MultiGroupByKeyProducer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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

import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static org.openjdk.jmh.annotations.Level.Invocation;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMultiGroupByKeyProducer
{
    @Benchmark
    @OperationsPerInvocation(1_000)
    public long[] benchmark(BenchmarkData data)
    {
        MultiGroupByKeyProducer producer = data.getProducer();
        Page page = data.getPage();
        int positionCount = page.getPositionCount();
        long[] keys = new long[positionCount];
        for (int i = 0; i < positionCount; i++) {
            keys[i] = producer.getKey(page, i);
        }
        return keys;
    }

    @Benchmark
    @OperationsPerInvocation(1_000)
    public long[] benchmark2(BenchmarkData data)
    {
        Page page = data.getPage();
        int positionCount = page.getPositionCount();
        long[] keys = new long[positionCount];
        for (int i = 0; i < positionCount; i++) {
            long key = 0;
            {
                Block block = page.getBlock(0);
                long value = SMALLINT.getLong(block, i);
                int length = 2;
                key |= (value & ((1L << length) - 1)) << 0;
            }
            {
                Block block = page.getBlock(1);
                long value = SMALLINT.getLong(block, i);
                int length = 2;
                key |= (value & ((1L << length) - 1)) << 2;
            }
            {
                Block block = page.getBlock(2);
                long value = TINYINT.getLong(block, i);
                int length = 1;
                key |= (value & ((1L << length) - 1)) << 4;
            }

            keys[i] = key;
        }
        return keys;
    }

//    @Benchmark
//    @OperationsPerInvocation(1_000)
    public long[] benchmark3(BenchmarkData data)
    {
        Page page = data.getPage();
        int positionCount = page.getPositionCount();
        long[] keys = new long[positionCount];

        Block block = page.getBlock(0);
        for (int i = 0; i < positionCount; i++) {
            long value = SMALLINT.getLong(block, i);
            int length = 2;
            keys[i] |= (value & ((1L << length) - 1)) << 0;
        }

        block = page.getBlock(1);
        for (int i = 0; i < positionCount; i++) {
            long value = SMALLINT.getLong(block, i);
            int length = 2;
            keys[i] |= (value & ((1L << length) - 1)) << 2;
        }

        block = page.getBlock(2);
        for (int i = 0; i < positionCount; i++) {
            long value = TINYINT.getLong(block, i);
            int length = 1;
            keys[i] |= (value & ((1L << length) - 1)) << 4;
        }

        return keys;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Page page;
        private MultiGroupByKeyProducer producer;

        @Setup(Invocation)
        public void setup()
        {
            Random random = new Random(0);
            Block block1;
            {
                BlockBuilder blockBuilder = SMALLINT.createFixedSizeBlockBuilder(1024);
                for (int i = 0; i < 1024; i++) {
                    blockBuilder.writeShort(random.nextInt(100));
                }
                block1 = blockBuilder.build();
            }

            Block block2;
            {
                BlockBuilder blockBuilder = SMALLINT.createFixedSizeBlockBuilder(1024);
                for (int i = 0; i < 1024; i++) {
                    blockBuilder.writeShort(random.nextInt(10_000));
                }
                block2 = blockBuilder.build();
            }

            Block block3;
            {
                BlockBuilder blockBuilder = TINYINT.createFixedSizeBlockBuilder(1024);
                for (int i = 0; i < 1024; i++) {
                    blockBuilder.writeByte(random.nextInt(10));
                }
                block3 = blockBuilder.build();
            }

            page = new Page(block1, block2, block3);

            producer = new MultiGroupByKeyProducer(new int[] {0, 1, 2}, ImmutableList.of(SMALLINT, SMALLINT, TINYINT));
        }

        public MultiGroupByKeyProducer getProducer()
        {
            return producer;
        }

        public Page getPage()
        {
            return page;
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        data.getProducer().getKey(data.getPage(), 0);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.BULK)
                .include(".*" + BenchmarkMultiGroupByKeyProducer.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
