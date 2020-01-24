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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.orc.FileCache;
import com.facebook.presto.orc.FileCache.FileCacheStats;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

public class CacheStatistics
{
    private static final FileCacheStats stats = new FileCacheStats();

    @Inject
    CacheStatistics(HiveClientConfig hiveClientConfig)
    {
        FileCache.incrementTargetSize(hiveClientConfig.getBlockCacheSize().toBytes());
        FileCache.registerStats(stats);
    }

    @Managed
    @Flatten
    public FileCache.FileCacheStats getStats()
    {
        return stats;
    }
}
