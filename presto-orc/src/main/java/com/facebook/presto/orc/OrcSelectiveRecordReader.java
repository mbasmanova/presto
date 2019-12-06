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

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.TupleDomainFilter.BigintMultiRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintValues;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.PostScript;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.reader.SelectiveStreamReader;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.reader.SelectiveStreamReaders.createStreamReader;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class OrcSelectiveRecordReader
        extends AbstractOrcRecordReader<SelectiveStreamReader>
{
    // Marks a SQL null when occurring in constantValues.
    private static final byte[] NULL_MARKER = new byte[0];
    private static final Page EMPTY_PAGE = new Page(0);

    private final int[] hiveColumnIndices;                            // elements are hive column indices
    private final List<Integer> outputColumns;                        // elements are hive column indices
    private final Map<Integer, Type> columnTypes;                     // key: index into hiveColumnIndices array
    private final Object[] constantValues;                            // aligned with hiveColumnIndices array
    private final Function<Block, Block>[] coercers;                   // aligned with hiveColumnIndices array
    private final List<FilterFunction> filterFunctionsWithInputs;
    private final Optional<FilterFunction> filterFunctionWithoutInput;
    private final Map<Integer, Integer> filterFunctionInputMapping;   // channel-to-index-into-hiveColumnIndices-array mapping
    private final Set<Integer> filterFunctionInputs;                  // channels
    private final Map<Integer, Integer> columnsWithFilterScores;      // keys are indices into hiveColumnIndices array; values are filter scores

    // Optimal order of stream readers
    private int[] streamReaderOrder;                                  // elements are indices into hiveColumnIndices array

    // An immutable list of initial positions; includes all positions: 0,1,2,3,4,..
    // This array may grow, but cannot shrink. The values don't change.
    private int[] positions;

    // Used in applyFilterFunctions; mutable
    private int[] outputPositions;
    private RuntimeException[] errors;
    private boolean constantFilterIsFalse;

    private int readPositions;

    public OrcSelectiveRecordReader(
            Map<Integer, Type> includedColumns,                 // key: hiveColumnIndex
            List<Integer> outputColumns,                        // elements are hive column indices
            Map<Integer, Map<Subfield, TupleDomainFilter>> filters, // key: hiveColumnIndex
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputMapping,   // channel-to-hiveColumnIndex mapping for all filter function inputs
            Map<Integer, List<Subfield>> requiredSubfields,     // key: hiveColumnIndex
            Map<Integer, Object> constantValues,                // key: hiveColumnIndex
            Map<Integer, Function<Block, Block>> coercers,      // key: hiveColumnIndex
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            List<ColumnStatistics> fileStats,
            List<StripeStatistics> stripeStats,
            OrcDataSource orcDataSource,
            long offset,
            long length,
            List<OrcType> types,
            Optional<OrcDecompressor> decompressor,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            PostScript.HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            Map<String, Slice> userMetadata,
            AggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            StripeMetadataSource stripeMetadataSource)
    {
        super(includedColumns,
                createStreamReaders(
                        orcDataSource,
                        types,
                        hiveStorageTimeZone,
                        includedColumns,
                        outputColumns,
                        filters,
                        filterFunctions,
                        filterFunctionInputMapping,
                        requiredSubfields,
                        systemMemoryUsage.newAggregatedMemoryContext()),
                predicate,
                numberOfRows,
                fileStripes,
                fileStats,
                stripeStats,
                orcDataSource,
                offset,
                length,
                types,
                decompressor,
                rowsInRowGroup,
                hiveStorageTimeZone,
                hiveWriterVersion,
                metadataReader,
                maxMergeDistance,
                tinyStripeThreshold,
                maxBlockSize,
                userMetadata,
                systemMemoryUsage,
                writeValidation,
                initialBatchSize,
                stripeMetadataSource);

        // Hive column indices can't be used to index into arrays because they are negative
        // for partition and hidden columns. Hence, we create synthetic zero-based indices.

        List<Integer> hiveColumnIndices = ImmutableList.copyOf(includedColumns.keySet());
        Map<Integer, Integer> zeroBasedIndices = IntStream.range(0, hiveColumnIndices.size())
                .boxed()
                .collect(toImmutableMap(hiveColumnIndices::get, Function.identity()));

        this.hiveColumnIndices = hiveColumnIndices.stream().mapToInt(i -> i).toArray();
        this.outputColumns = outputColumns.stream().map(zeroBasedIndices::get).collect(toImmutableList());
        this.columnTypes = includedColumns.entrySet().stream().collect(toImmutableMap(entry -> zeroBasedIndices.get(entry.getKey()), Map.Entry::getValue));
        this.filterFunctionWithoutInput = getFilterFunctionWithoutInputs(filterFunctions);
        this.filterFunctionsWithInputs = filterFunctions.stream()
                .filter(function -> function.getInputChannels().length > 0)
                .collect(toImmutableList());
        this.filterFunctionInputMapping = Maps.transformValues(filterFunctionInputMapping, zeroBasedIndices::get);
        this.filterFunctionInputs = filterFunctions.stream()
                .flatMapToInt(function -> Arrays.stream(function.getInputChannels()))
                .boxed()
                .map(this.filterFunctionInputMapping::get)
                .collect(toImmutableSet());
        this.columnsWithFilterScores = filters
                .entrySet()
                .stream()
                .collect(toImmutableMap(entry -> zeroBasedIndices.get(entry.getKey()), entry -> scoreFilter(entry.getValue())));

        requireNonNull(coercers, "coercers is null");
        this.coercers = new Function[this.hiveColumnIndices.length];
        for (Map.Entry<Integer, Function<Block, Block>> entry : coercers.entrySet()) {
            this.coercers[zeroBasedIndices.get(entry.getKey())] = entry.getValue();
        }

        requireNonNull(constantValues, "constantValues is null");
        this.constantValues = new Object[this.hiveColumnIndices.length];
        for (int columnIndex : includedColumns.keySet()) {
            if (!isColumnPresent(columnIndex)) {
                // Any filter not true of null on a missing column
                // fails the whole split. Filters on prefilled columns
                // are already evaluated, hence we only check filters
                // for missing columns here.
                if (columnIndex >= 0 && containsNonNullFilter(filters.get(columnIndex))) {
                    constantFilterIsFalse = true;
                    // No further initialization needed.
                    return;
                }
                this.constantValues[zeroBasedIndices.get(columnIndex)] = NULL_MARKER;
            }
        }

        for (Map.Entry<Integer, Object> entry : constantValues.entrySet()) {
            // all included columns will be null, the constant columns should have a valid predicate or null marker so that there is no streamReader created below
            if (entry.getValue() != null) {
                this.constantValues[zeroBasedIndices.get(entry.getKey())] = entry.getValue();
            }
        }

        // Initial order of stream readers is:
        //  - readers with integer equality
        //  - readers with integer range / multivalues / inequality
        //  - readers with filters
        //  - followed by readers for columns that provide input to filter functions
        //  - followed by readers for columns that doesn't have any filtering
        streamReaderOrder = orderStreamReaders(columnTypes.keySet().stream().filter(index -> this.constantValues[index] == null).collect(toImmutableSet()), columnsWithFilterScores, filterFunctionInputs, columnTypes);
    }

    private static Optional<FilterFunction> getFilterFunctionWithoutInputs(List<FilterFunction> filterFunctions)
    {
        List<FilterFunction> functions = filterFunctions.stream()
                .filter(function -> function.getInputChannels().length == 0)
                .collect(toImmutableList());
        if (functions.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(Iterables.getOnlyElement(functions));
    }

    private static boolean containsNonNullFilter(Map<Subfield, TupleDomainFilter> columnFilters)
    {
        return columnFilters != null && !columnFilters.values().stream().allMatch(TupleDomainFilter::testNull);
    }

    private static int scoreFilter(Map<Subfield, TupleDomainFilter> filters)
    {
        checkArgument(!filters.isEmpty());

        if (filters.size() > 1) {
            // Complex type column. Complex types are expensive!
            return 1000;
        }

        Map.Entry<Subfield, TupleDomainFilter> filterEntry = Iterables.getOnlyElement(filters.entrySet());
        if (!filterEntry.getKey().getPath().isEmpty()) {
            // Complex type column. Complex types are expensive!
            return 1000;
        }

        TupleDomainFilter filter = filterEntry.getValue();
        if (filter instanceof BigintRange) {
            if (((BigintRange) filter).isSingleValue()) {
                // Integer equality. Generally cheap.
                return 10;
            }
            return 50;
        }

        if (filter instanceof BigintValues || filter instanceof BigintMultiRange) {
            return 50;
        }

        return 100;
    }

    private static int scoreType(Type type)
    {
        if (type == BOOLEAN) {
            return 10;
        }

        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == TIMESTAMP || type == DATE) {
            return 20;
        }

        if (type == REAL || type == DOUBLE) {
            return 30;
        }

        if (type instanceof DecimalType) {
            return 40;
        }

        if (isVarcharType(type) || type instanceof CharType) {
            return 50;
        }

        return 100;
    }

    private static int[] orderStreamReaders(
            Collection<Integer> columnIndices,
            Map<Integer, Integer> columnToScore,
            Set<Integer> filterFunctionInputs,
            Map<Integer, Type> columnTypes)
    {
        List<Integer> sortedColumnsByFilterScore = columnToScore.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        int[] order = new int[columnIndices.size()];
        int i = 0;
        for (int columnIndex : sortedColumnsByFilterScore) {
            if (columnIndices.contains(columnIndex)) {
                order[i++] = columnIndex;
            }
        }

        // read primitive types first
        List<Integer> sortedFilterFunctionInputs = filterFunctionInputs.stream()
                .collect(toImmutableMap(Function.identity(), columnIndex -> scoreType(columnTypes.get(columnIndex))))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        for (int columnIndex : sortedFilterFunctionInputs) {
            if (columnIndices.contains(columnIndex) && !sortedColumnsByFilterScore.contains(columnIndex)) {
                order[i++] = columnIndex;
            }
        }

        for (int columnIndex : columnIndices) {
            if (!sortedColumnsByFilterScore.contains(columnIndex) && !filterFunctionInputs.contains(columnIndex)) {
                order[i++] = columnIndex;
            }
        }

        return order;
    }

    private static SelectiveStreamReader[] createStreamReaders(
            OrcDataSource orcDataSource,
            List<OrcType> types,
            DateTimeZone hiveStorageTimeZone,
            Map<Integer, Type> includedColumns,
            List<Integer> outputColumns,
            Map<Integer, Map<Subfield, TupleDomainFilter>> filters,
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputMapping,
            Map<Integer, List<Subfield>> requiredSubfields,
            AggregatedMemoryContext systemMemoryContext)
    {
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();

        requireNonNull(filterFunctions, "filterFunctions is null");
        requireNonNull(filterFunctionInputMapping, "filterFunctionInputMapping is null");

        Set<Integer> filterFunctionInputColumns = filterFunctions.stream()
                .flatMapToInt(function -> Arrays.stream(function.getInputChannels()))
                .boxed()
                .map(filterFunctionInputMapping::get)
                .collect(toImmutableSet());

        OrcType rowType = types.get(0);
        SelectiveStreamReader[] streamReaders = new SelectiveStreamReader[rowType.getFieldCount()];
        for (int columnId = 0; columnId < rowType.getFieldCount(); columnId++) {
            if (includedColumns.containsKey(columnId)) {
                StreamDescriptor streamDescriptor = streamDescriptors.get(columnId);
                boolean outputRequired = outputColumns.contains(columnId) || filterFunctionInputColumns.contains(columnId);
                streamReaders[columnId] = createStreamReader(
                        streamDescriptor,
                        Optional.ofNullable(filters.get(columnId)).orElse(ImmutableMap.of()),
                        outputRequired ? Optional.of(includedColumns.get(columnId)) : Optional.empty(),
                        Optional.ofNullable(requiredSubfields.get(columnId)).orElse(ImmutableList.of()),
                        hiveStorageTimeZone,
                        systemMemoryContext);
            }
        }
        return streamReaders;
    }

    public int getReadPositions()
    {
        return readPositions;
    }

    public Page getNextPage()
            throws IOException
    {
        if (constantFilterIsFalse) {
            return null;
        }
        int batchSize = prepareNextBatch();
        if (batchSize < 0) {
            return null;
        }

        readPositions += batchSize;
        initializePositions(batchSize);

        int[] positionsToRead = this.positions;
        int positionCount = batchSize;

        if (filterFunctionWithoutInput.isPresent()) {
            positionCount = applyFilterFunctionWithNoInputs(positionCount);

            if (positionCount == 0) {
                batchRead(batchSize);
                return EMPTY_PAGE;
            }

            positionsToRead = outputPositions;
        }

        boolean filterFunctionsApplied = filterFunctionsWithInputs.isEmpty();
        int offset = getNextRowInGroup();

        for (int columnIndex : streamReaderOrder) {
            if (!filterFunctionsApplied && !hasAnyFilter(columnIndex)) {
                positionCount = applyFilterFunctions(positionsToRead, positionCount);
                if (positionCount == 0) {
                    break;
                }

                positionsToRead = outputPositions;
                filterFunctionsApplied = true;
            }

            if (!hasAnyFilter(columnIndex)) {
                break;
            }

            SelectiveStreamReader streamReader = getStreamReader(columnIndex);
            positionCount = streamReader.read(offset, positionsToRead, positionCount);
            if (positionCount == 0) {
                break;
            }

            positionsToRead = streamReader.getReadPositions();
            verify(positionCount == 1 || positionsToRead[positionCount - 1] - positionsToRead[0] >= positionCount - 1, "positions must monotonically increase");
        }

        if (positionCount > 0 && !filterFunctionsApplied) {
            positionCount = applyFilterFunctions(positionsToRead, positionCount);
            positionsToRead = outputPositions;
        }

        batchRead(batchSize);

        if (positionCount == 0) {
            return EMPTY_PAGE;
        }

        if (filterFunctionsWithInputs.isEmpty() && filterFunctionWithoutInput.isPresent()) {
            for (int i = 0; i < positionCount; i++) {
                if (errors[positionsToRead[i]] != null) {
                    throw errors[positionsToRead[i]];
                }
            }
        }

        for (SelectiveStreamReader reader : getStreamReaders()) {
            if (reader != null) {
                reader.throwAnyError(positionsToRead, positionCount);
            }
        }

        Block[] blocks = new Block[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            int columnIndex = outputColumns.get(i);
            if (constantValues[columnIndex] != null) {
                blocks[i] = RunLengthEncodedBlock.create(columnTypes.get(columnIndex), constantValues[columnIndex] == NULL_MARKER ? null : constantValues[columnIndex], positionCount);
            }
            else if (!hasAnyFilter(columnIndex)) {
                blocks[i] = new LazyBlock(positionCount, new OrcBlockLoader(getStreamReader(columnIndex), coercers[columnIndex], offset, positionsToRead, positionCount));
            }
            else {
                Block block = getStreamReader(columnIndex).getBlock(positionsToRead, positionCount);
                updateMaxCombinedBytesPerRow(columnIndex, block);

                if (coercers[columnIndex] != null) {
                    block = coercers[columnIndex].apply(block);
                }

                blocks[i] = block;
            }
        }

        Page page = new Page(positionCount, blocks);

        validateWritePageChecksum(page);

        return page;
    }

    private SelectiveStreamReader getStreamReader(int columnIndex)
    {
        return getStreamReaders()[hiveColumnIndices[columnIndex]];
    }

    private boolean hasAnyFilter(int columnIndex)
    {
        return columnsWithFilterScores.containsKey(columnIndex) || filterFunctionInputs.contains(columnIndex);
    }

    private void initializePositions(int batchSize)
    {
        if (positions == null || positions.length < batchSize) {
            positions = new int[batchSize];
            for (int i = 0; i < batchSize; i++) {
                positions[i] = i;
            }
        }

        if (errors == null || errors.length < batchSize) {
            errors = new RuntimeException[batchSize];
        }
        else {
            Arrays.fill(errors, null);
        }
    }

    private int applyFilterFunctionWithNoInputs(int positionCount)
    {
        initializeOutputPositions(positionCount);
        Page page = new Page(positionCount);
        return filterFunctionWithoutInput.get().filter(page, outputPositions, positionCount, errors);
    }

    private int applyFilterFunctions(int[] positions, int positionCount)
    {
        BlockLease[] blockLeases = new BlockLease[hiveColumnIndices.length];
        Block[] blocks = new Block[hiveColumnIndices.length];
        for (int columnIndex : filterFunctionInputs) {
            if (constantValues[columnIndex] != null) {
                blocks[columnIndex] = RunLengthEncodedBlock.create(columnTypes.get(columnIndex), constantValues[columnIndex] == NULL_MARKER ? null : constantValues[columnIndex], positionCount);
            }
            else {
                blockLeases[columnIndex] = getStreamReader(columnIndex).getBlockView(positions, positionCount);
                Block block = blockLeases[columnIndex].get();
                if (coercers[columnIndex] != null) {
                    block = coercers[columnIndex].apply(block);
                }
                blocks[columnIndex] = block;
            }
        }

        if (filterFunctionWithoutInput.isPresent()) {
            for (int i = 0; i < positionCount; i++) {
                errors[i] = errors[positions[i]];
            }
        }

        try {
            initializeOutputPositions(positionCount);

            for (FilterFunction function : filterFunctionsWithInputs) {
                int[] inputs = function.getInputChannels();
                Block[] inputBlocks = new Block[inputs.length];

                for (int i = 0; i < inputs.length; i++) {
                    inputBlocks[i] = blocks[filterFunctionInputMapping.get(inputs[i])];
                }

                Page page = new Page(positionCount, inputBlocks);
                positionCount = function.filter(page, outputPositions, positionCount, errors);
                if (positionCount == 0) {
                    break;
                }
            }

            for (int i = 0; i < positionCount; i++) {
                if (errors[i] != null) {
                    throw errors[i];
                }
            }

            // at this point outputPositions are relative to page, e.g. they are indices into positions array
            // translate outputPositions to positions relative to the start of the row group,
            // e.g. make outputPositions a subset of positions array
            for (int i = 0; i < positionCount; i++) {
                outputPositions[i] = positions[outputPositions[i]];
            }
            return positionCount;
        }
        finally {
            for (BlockLease blockLease : blockLeases) {
                if (blockLease != null) {
                    blockLease.close();
                }
            }
        }
    }

    private void initializeOutputPositions(int positionCount)
    {
        if (outputPositions == null || outputPositions.length < positionCount) {
            outputPositions = new int[positionCount];
        }

        for (int i = 0; i < positionCount; i++) {
            outputPositions[i] = i;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            for (SelectiveStreamReader streamReader : getStreamReaders()) {
                if (streamReader != null) {
                    closer.register(streamReader::close);
                }
            }
        }

        super.close();
    }

    private static final class OrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final SelectiveStreamReader reader;
        @Nullable
        private final Function<Block, Block> coercer;
        private final int offset;
        private final int[] positions;
        private final int positionCount;
        private boolean loaded;

        public OrcBlockLoader(SelectiveStreamReader reader, @Nullable Function<Block, Block> coercer, int offset, int[] positions, int positionCount)
        {
            this.reader = requireNonNull(reader, "reader is null");
            this.coercer = coercer; // can be null
            this.offset = offset;
            this.positions = requireNonNull(positions, "positions is null");
            this.positionCount = positionCount;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            try {
                reader.read(offset, positions, positionCount);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            Block block = reader.getBlock(positions, positionCount);
            if (coercer != null) {
                block = coercer.apply(block);
            }
            lazyBlock.setBlock(block);

            loaded = true;
        }
    }
}
