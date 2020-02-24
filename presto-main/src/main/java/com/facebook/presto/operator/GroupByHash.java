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

import com.facebook.presto.Session;
import com.facebook.presto.operator.BigintGroupByHash.MultiGroupByKeyProducer;
import com.facebook.presto.operator.BigintGroupByHash.SingleGroupByKeyProducer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isDictionaryAggregationEnabled;
import static com.facebook.presto.operator.UpdateMemory.NOOP;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;

public interface GroupByHash
{
    static GroupByHash createGroupByHash(
            Session session,
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            JoinCompiler joinCompiler)
    {
        return createGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, isDictionaryAggregationEnabled(session), joinCompiler, NOOP, false);
    }

    static GroupByHash createGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory,
            boolean preferArrayAggregation)
    {
        if (hashTypes.size() == 1 && hashTypes.get(0).equals(BIGINT) && hashChannels.length == 1) {
            if (preferArrayAggregation) {
                return new ArrayBasedGroupByHash(hashChannels[0]);
            }

            return new BigintGroupByHash(new SingleGroupByKeyProducer(hashChannels[0]), inputHashChannel, expectedSize, updateMemory);
        }

        int totalBytes = 0;
        for (int i = 0; i < hashChannels.length; i++) {
            Type type = hashTypes.get(i);
            if (TINYINT == type) {
                totalBytes += 1;
            }
            else if (SMALLINT == type) {
                totalBytes += 2;
            }
            else if (INTEGER == type) {
                totalBytes += 4;
            }
            else {
                totalBytes = Integer.MAX_VALUE;
                break;
            }
        }

        if (totalBytes <= 8) {
            return new BigintGroupByHash(new MultiGroupByKeyProducer(hashChannels, hashTypes), inputHashChannel, expectedSize, updateMemory);
        }

        return new MultiChannelGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler, updateMemory);
    }

    long getEstimatedSize();

    long getHashCollisions();

    double getExpectedHashCollisions();

    List<Type> getTypes();

    int getGroupCount();

    default IntIterator getGroupIds()
    {
        return IntIterators.fromTo(0, getGroupCount());
    }

    void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset);

    Work<?> addPage(Page page);

    Work<GroupByIdBlock> getGroupIds(Page page);

    boolean contains(int position, Page page, int[] hashChannels);

    long getRawHash(int groupId);

    @VisibleForTesting
    int getCapacity();
}
