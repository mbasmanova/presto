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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static com.facebook.presto.orc.metadata.CompressionKind.LZ4;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.testng.Assert.assertEquals;

public class TestOrcLz4
{
    private static final DataSize SIZE = new DataSize(1, MEGABYTE);

    @Test
    public void test2()
            throws Exception
    {
        String path = "/Users/mbasmanova/test_data/deltoid/dwrf/metrics0/u1.dwrf";
        OrcReader orcReader = createOrcReader(path, DWRF);

        List<OrcType> orcTypes = orcReader.getTypes();
        OrcType rootOrcType = orcTypes.get(0);

        List<String> columnNames = orcReader.getColumnNames();
        List<ColumnStatistics> fileStats = orcReader.getFooter().getFileStats();

        for (int i = 0; i < rootOrcType.getFieldCount(); i++) {
            int field = rootOrcType.getFieldTypeIndex(i);
            System.out.println(String.format("%s: %s",
                    columnNames.get(i),
                    fileStats.get(field)));
        }
    }

    @Test
    public void test()
            throws Exception
    {
        String path = "/Users/mbasmanova/test_data/deltoid/metrics0/20211025_232846_51492_hftkx_aefe7649-5d3f-40ba-aa52-5c73daa409d9";
        String outputPath = "/Users/mbasmanova/test_data/deltoid/dwrf/metrics0/u4.dwrf";

        OrcReader orcReader = createOrcReader(path, ORC);

        // 284M rows

        List<OrcType> orcTypes = orcReader.getTypes();
        OrcType rootOrcType = orcTypes.get(0);

        List<Type> types = new ArrayList<>();
        for (int i = 0; i < rootOrcType.getFieldCount(); i++) {
            types.add(toType(orcTypes.get(rootOrcType.getFieldTypeIndex(i))));
        }

        List<String> names = orcReader.getColumnNames();

        File outputFile = new File(outputPath);
        OrcWriter orcWriter = createOrcWriter(outputFile, OrcEncoding.DWRF, orcReader.getCompressionKind(), Optional.empty(),
                types, names, OrcWriterOptions.builder().build(), new OrcWriterStats());

        Map<Integer, Type> columnTypes = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableMap(Functions.identity(), types::get));

        OrcBatchRecordReader recordReader = orcReader.createBatchRecordReader(columnTypes, OrcPredicate.TRUE, HIVE_STORAGE_TIME_ZONE, new TestingHiveOrcAggregatedMemoryContext(), 1024);

        int totalCount = 0;
        int nextCountToPrint = 0;
        int positionCount = recordReader.nextBatch();
        while (positionCount >= 0) {
            Block[] blocks = new Block[types.size()];
            for (int i = 0; i < rootOrcType.getFieldCount(); i++) {
                blocks[i] = recordReader.readBlock(i);
            }

            Page page = new Page(positionCount, blocks);
            orcWriter.write(page);

            totalCount += positionCount;
            if (totalCount > nextCountToPrint) {
                System.out.println("Total count: " + totalCount);
                nextCountToPrint = totalCount + 10_000_000;
            }

            positionCount = recordReader.nextBatch();
        }

        orcWriter.close();
    }

    OrcReader createOrcReader(String path, OrcEncoding orcEncoding)
            throws IOException {
        OrcDataSource orcDataSource = new FileOrcDataSource(new File(path), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);

        OrcReader orcReader = new OrcReader(
                orcDataSource,
                orcEncoding,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                new OrcReaderOptions(
                        SIZE,
                        SIZE,
                        SIZE,
                        false),
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats());
        return orcReader;
    }

    private static Type toType(OrcType orcType) {
        switch (orcType.getOrcTypeKind()) {
            case LONG:
                return BIGINT;
            case STRING:
                return VARCHAR;
            default:
                throw new UnsupportedOperationException("Unsupported ORC type: " + orcType);
        }
    }

    static OrcWriter createOrcWriter(File outputFile, OrcEncoding encoding, CompressionKind compression, Optional<DwrfWriterEncryption> dwrfWriterEncryption, List<Type> types, List<String> columnNames, OrcWriterOptions writerOptions, WriterStats stats)
            throws FileNotFoundException
    {
        ImmutableMap.Builder<String, String> metadata = ImmutableMap.builder();
        metadata.put("columns", String.join(", ", columnNames));
        metadata.put("columns.types", createSettableStructObjectInspector(types, columnNames).getTypeName());

        OrcWriter writer = new OrcWriter(
                new OutputStreamDataSink(new FileOutputStream(outputFile)),
                columnNames,
                types,
                encoding,
                compression,
                dwrfWriterEncryption,
                new DwrfEncryptionProvider(new UnsupportedEncryptionLibrary(), new TestingEncryptionLibrary()),
                writerOptions,
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                true,
                BOTH,
                stats);
        return writer;
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(List<Type> types, List<String> names)
    {
        List<ObjectInspector> columnTypes = types.stream()
                .map(OrcTester::getJavaObjectInspector)
                .collect(toList());

        return getStandardStructObjectInspector(names, columnTypes);
    }

    @Test
    public void testReadLz4()
            throws Exception
    {
        // this file was written with Apache ORC
        // TODO: use Apache ORC library in OrcTester
        byte[] data = toByteArray(getResource("apache-lz4.orc"));

        OrcReader orcReader = new OrcReader(
                new InMemoryOrcDataSource(data),
                ORC,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                new OrcReaderOptions(
                        SIZE,
                        SIZE,
                        SIZE,
                        false),
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats());

        assertEquals(orcReader.getCompressionKind(), LZ4);
        assertEquals(orcReader.getFooter().getNumberOfRows(), 10_000);

        Map<Integer, Type> includedColumns = ImmutableMap.<Integer, Type>builder()
                .put(0, BIGINT)
                .put(1, INTEGER)
                .put(2, BIGINT)
                .build();

        OrcBatchRecordReader reader = orcReader.createBatchRecordReader(
                includedColumns,
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                new TestingHiveOrcAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE);

        int rows = 0;
        while (true) {
            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            rows += batchSize;

            Block xBlock = reader.readBlock(0);
            Block yBlock = reader.readBlock(1);
            Block zBlock = reader.readBlock(2);

            for (int position = 0; position < batchSize; position++) {
                BIGINT.getLong(xBlock, position);
                INTEGER.getLong(yBlock, position);
                BIGINT.getLong(zBlock, position);
            }
        }

        assertEquals(rows, reader.getFileRowCount());
    }

    private static class InMemoryOrcDataSource
            extends AbstractOrcDataSource
    {
        private final byte[] data;

        public InMemoryOrcDataSource(byte[] data)
        {
            super(new OrcDataSourceId("memory"), data.length, SIZE, SIZE, SIZE, false);
            this.data = data;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            System.arraycopy(data, toIntExact(position), buffer, bufferOffset, bufferLength);
        }
    }
}
