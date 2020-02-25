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
package com.facebook.presto.orc.reader;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.FloatInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.block.ClosingBlockLease;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LazyBlockLoader.ValueConsumer;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import org.openjdk.jol.info.ClassLayout;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_BASE_OFFSET;

public class FloatSelectiveStreamReader
        implements SelectiveStreamReader
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

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FloatSelectiveStreamReader.class).instanceSize();
    private static final Block NULL_BLOCK = REAL.createBlockBuilder(null, 1).appendNull().build();

    private final StreamDescriptor streamDescriptor;
    private final TupleDomainFilter filter;
    private final boolean nullsAllowed;
    private final boolean outputRequired;
    private final LocalMemoryContext systemMemoryContext;
    private final boolean nonDeterministicFilter;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private InputStreamSource<FloatInputStream> dataStreamSource = missingStreamSource(FloatInputStream.class);
    private BooleanInputStream presentStream;
    private FloatInputStream dataStream;

    private boolean rowGroupOpen;
    private int readOffset;
    private int[] values;
    private boolean[] nulls;
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean allNulls;
    private boolean valuesInUse;

    public FloatSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            boolean outputRequired,
            LocalMemoryContext systemMemoryContext)
    {
        requireNonNull(filter, "filter is null");
        checkArgument(filter.isPresent() || outputRequired, "filter must be present if outputRequired is false");
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = filter.orElse(null);
        this.outputRequired = outputRequired;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        this.nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(FloatInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, FloatInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions);
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public void read(int offset, int[] positions, int positionCount, ValueConsumer consumer, boolean includeNulls)
            throws IOException
    {
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");
        checkState(filter == null, "Filter is not supported when reading with a callback");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int streamPosition = 0;
        if (dataStream == null && presentStream != null) {
            presentStream.skip(positions[positionCount - 1]);
            outputPositionCount = positionCount;
            allNulls = true;
            streamPosition = positions[positionCount - 1] + 1;
        }
        else {
            if (positions[positionCount - 1] == positionCount - 1) {
                // no skipping
                if (presentStream != null) {
                    // some nulls
                    nulls = ensureCapacity(nulls, positionCount);
                    int nullCount = presentStream.getUnsetBits(positionCount, nulls);
                    if (nullCount == positionCount) {
                        // all nulls
                        if (includeNulls) {
                            for (int i = 0; i < positionCount; i++) {
                                consumer.acceptNull(i);
                            }
                        }
                        allNulls = true;
                    }
                    else {
                        if (includeNulls) {
                            for (int i = 0; i < positionCount; i++) {
                                if (nulls[i]) {
                                    consumer.acceptNull(i);
                                }
                                else {
                                    consumer.acceptFloat(i, dataStream.next());
                                }
                            }
                        }
                        else {
                            for (int i = 0; i + 8 < positionCount; i += 8) {
                                long flags = unsafe.getLong(nulls, (long) ARRAY_BOOLEAN_BASE_OFFSET + i) ^ 0x0101010101010101L;
                                while (flags != 0) {
                                    int low = Long.numberOfTrailingZeros(flags) >> 3;
                                    flags &= flags - 1;

                                    consumer.acceptFloat(i + low, dataStream.next());
                                }
                            }
                            // TODO Process the remaining <8 values
                        }
                    }
                }
                else {
                    // no nulls
                    for (int i = 0; i < positionCount; i++) {
                        consumer.acceptFloat(i, dataStream.next());
                    }
                }
                streamPosition = positions[positionCount - 1] + 1;
            }
            else {
                for (int i = 0; i < positionCount; i++) {
                    int position = positions[i];
                    if (position > streamPosition) {
                        skip(position - streamPosition);
                        streamPosition = position;
                    }

                    if (presentStream != null && !presentStream.nextBit()) {
                        if (includeNulls) {
                            consumer.acceptNull(position);
                        }
                    }
                    else {
                        consumer.acceptFloat(position, dataStream.next());
                    }
                    streamPosition++;
                }
            }
            outputPositionCount = positionCount;
        }

        readOffset = offset + streamPosition;
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        if (outputRequired) {
            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
        }

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int streamPosition = 0;
        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else if (filter == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else {
            streamPosition = readWithFilter(positions, positionCount);
        }

        readOffset = offset + streamPosition;
        return outputPositionCount;
    }

    private int readWithFilter(int[] positions, int positionCount)
            throws IOException
    {
        int streamPosition = 0;
        outputPositionCount = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if ((nonDeterministicFilter && filter.testNull()) || nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                float value = dataStream.next();
                if (filter.testFloat(value)) {
                    if (outputRequired) {
                        values[outputPositionCount] = floatToRawIntBits(value);
                        if (nullsAllowed && presentStream != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            streamPosition++;

            if (filter != null) {
                outputPositionCount -= filter.getPrecedingPositionsToFail();
                int succeedingPositionsToFail = filter.getSucceedingPositionsToFail();
                if (succeedingPositionsToFail > 0) {
                    int positionsToSkip = 0;
                    for (int j = 0; j < succeedingPositionsToFail; j++) {
                        i++;
                        int nextPosition = positions[i];
                        positionsToSkip += 1 + nextPosition - streamPosition;
                        streamPosition = nextPosition + 1;
                    }
                    skip(positionsToSkip);
                }
            }
        }

        return streamPosition;
    }

    private int readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);

        if (nonDeterministicFilter) {
            outputPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                if (filter.testNull()) {
                    outputPositionCount++;
                }
                else {
                    outputPositionCount -= filter.getPrecedingPositionsToFail();
                    i += filter.getSucceedingPositionsToFail();
                }
            }
        }
        else if (nullsAllowed) {
            outputPositionCount = positionCount;
        }
        else {
            outputPositionCount = 0;
        }

        allNulls = true;
        return positions[positionCount - 1] + 1;
    }

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        if (positions[positionCount - 1] == positionCount - 1) {
            // no skipping
            if (presentStream != null) {
                // some nulls
                int nullCount = presentStream.getUnsetBits(positionCount, nulls);
                if (nullCount == positionCount) {
                    allNulls = true;
                }
                else {
                    for (int i = 0; i < positionCount; i++) {
                        if (!nulls[i]) {
                            values[i] = floatToRawIntBits(dataStream.next());
                        }
                    }
                }
            }
            else {
                // no nulls
                for (int i = 0; i < positionCount; i++) {
                    values[i] = floatToRawIntBits(dataStream.next());
                }
            }
            outputPositionCount = positionCount;
            return positions[positionCount - 1] + 1;
        }

        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                nulls[i] = true;
            }
            else {
                values[i] = floatToRawIntBits(dataStream.next());
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    private void skip(int items)
            throws IOException
    {
        if (dataStream == null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            dataStream.skip(dataToSkip);
        }
        else {
            dataStream.skip(items);
        }
    }

    private void ensureValuesCapacity(int capacity, boolean recordNulls)
    {
        values = ensureCapacity(values, capacity);

        if (recordNulls) {
            nulls = ensureCapacity(nulls, capacity);
        }
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return new RunLengthEncodedBlock(NULL_BLOCK, positionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount == outputPositionCount) {
            Block block = new IntArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values);
            nulls = null;
            values = null;
            return block;
        }

        int[] valuesCopy = new int[positionCount];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            valuesCopy[positionIndex] = this.values[i];
            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new IntArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(NULL_BLOCK, positionCount));
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount != outputPositionCount) {
            compactValues(positions, positionCount, includeNulls);
        }

        return newLease(new IntArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values));
    }

    private BlockLease newLease(Block block)
    {
        valuesInUse = true;
        return ClosingBlockLease.newLease(block, () -> valuesInUse = false);
    }

    private void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            values[positionIndex] = values[i];
            if (compactNulls) {
                nulls[positionIndex] = nulls[i];
            }
            outputPositions[positionIndex] = nextPosition;

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
    }

    @Override
    public void throwAnyError(int[] positions, int positionCount)
    {
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
