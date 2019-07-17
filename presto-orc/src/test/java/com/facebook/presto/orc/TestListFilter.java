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
package com.facebook.presto.orc;

import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.PositionalFilter;
import com.facebook.presto.orc.reader.ListFilter;
import com.facebook.presto.spi.Subfield;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.INT;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.LIST;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static org.openjdk.jmh.util.Utils.sum;
import static org.testng.Assert.assertEquals;

public class TestListFilter
{
    private final Random random = new Random(0);

    @Test
    public void testNestedArray()
    {
        int[][][] data = generateData(100);

        assertPositionalFilter(ImmutableMap.of(
                Pair.of(1, 1), BigintRange.of(0, 50, false)), data);

        assertPositionalFilter(ImmutableMap.of(
                Pair.of(1, 2), BigintRange.of(0, 50, false),
                Pair.of(3, 2), BigintRange.of(25, 50, false)), data);

        assertPositionalFilter(ImmutableMap.of(
                Pair.of(2, 1), BigintRange.of(0, 50, false),
                Pair.of(2, 2), BigintRange.of(10, 40, false),
                Pair.of(3, 3), BigintRange.of(20, 30, false)), data);
    }

    private void assertPositionalFilter(Map<Pair<Integer, Integer>, TupleDomainFilter> filters, int[][][] data)
    {
        ListFilter listFilter = buildListFilter(filters, data);

        TestFilter testFilter = (i, j, value) -> Optional.ofNullable(filters.get(Pair.of(i + 1, j + 1)))
                .map(filter -> filter.testLong(value))
                .orElse(true);

        PositionalFilter positionalFilter = listFilter.getChild().getPositionalFilter();

        for (int i = 0; i < data.length; i++) {
            boolean expectedToFail = false;
            for (int j = 0; j < data[i].length; j++) {
                for (int k = 0; k < data[i][j].length; k++) {
                    int value = data[i][j][k];
                    boolean expectedToPass = !expectedToFail && testFilter.test(j, k, value);
                    assertEquals(positionalFilter.testLong(value), expectedToPass, format("i = %s, j = %s, k = %s, value = %s", i, j, k , value));
                    if (!expectedToPass) {
                        expectedToFail = true;
                    }
                }
            }
        }
    }

    private int[][][] generateData(int rowCount)
    {
        int[][][] data = new int[rowCount][][];
        for (int i = 0; i < data.length; i++) {
            data[i] = new int[3 + random.nextInt(10)][];
            for (int j = 0; j < data[i].length; j++) {
                data[i][j] = new int[3 + random.nextInt(10)];
                for (int k = 0; k < data[i][j].length; k++) {
                    data[i][j][k] = random.nextInt(100);
                }
            }
        }
        return data;
    }

    private static ListFilter buildListFilter(Map<Pair<Integer, Integer>, TupleDomainFilter> filters, int[][][] data)
    {
        Map<Subfield, TupleDomainFilter> subfieldFilters = filters.entrySet().stream()
                .collect(toImmutableMap(entry -> toSubfield(entry.getKey()), Map.Entry::getValue));

        DummyOrcDataSource orcDataSource = new DummyOrcDataSource();
        ListFilter filter = new ListFilter(makeStreamDescriptor(2), subfieldFilters);

        int[] lengths = Arrays.stream(data).mapToInt(v -> v.length).toArray();
        filter.populateElementFilters(data.length, null, lengths, sum(lengths));

        int[] nestedLenghts = Arrays.stream(data).flatMap(Arrays::stream).mapToInt(v -> v.length).toArray();
        ((ListFilter) filter.getChild()).populateElementFilters(sum(lengths), null, nestedLenghts, sum(nestedLenghts));

        return filter;
    }

    private static StreamDescriptor makeStreamDescriptor(int levels)
    {
        DummyOrcDataSource orcDataSource = new DummyOrcDataSource();

        StreamDescriptor streamDescriptor = new StreamDescriptor("a", 0, "a", INT, orcDataSource, ImmutableList.of());
        for (int i = 0; i < levels; i++) {
            streamDescriptor = new StreamDescriptor("a", 0, "a", LIST, orcDataSource, ImmutableList.of(streamDescriptor));
        }

        return streamDescriptor;
    }

    private static class DummyOrcDataSource
            implements OrcDataSource
    {

        @Override
        public OrcDataSourceId getId()
        {
            return null;
        }

        @Override
        public long getReadBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public long getSize()
        {
            return 0;
        }

        @Override
        public void readFully(long position, byte[] buffer)
                throws IOException
        {

        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {

        }

        @Override
        public <K> Map<K, OrcDataSourceInput> readFully(Map<K, DiskRange> diskRanges)
                throws IOException
        {
            return null;
        }
    }

    private static Subfield toSubfield(Pair<Integer, Integer> indices)
    {
        return new Subfield(format("c[%s][%s]", indices.getLeft(), indices.getRight()));
    }

    private interface TestFilter
    {
        boolean test(int i, int j, int value);
    }
}
