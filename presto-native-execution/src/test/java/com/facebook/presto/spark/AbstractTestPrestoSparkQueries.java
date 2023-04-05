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
package com.facebook.presto.spark;

import com.facebook.presto.hive.HiveExternalWorkerQueryRunner;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle;

import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AbstractTestPrestoSparkQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        return PrestoSparkNativeQueryRunner.createQueryRunner();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        String dataDirectory = System.getProperty("DATA_DIR");
        return HiveExternalWorkerQueryRunner.createJavaQueryRunner(Optional.ofNullable(dataDirectory).map(Paths::get), "legacy");
    }

    protected void assertShuffleMetadata()
    {
        assertNotNull(SparkEnv.get());
        assertTrue(SparkEnv.get().shuffleManager() instanceof PrestoSparkNativeExecutionShuffleManager);
        PrestoSparkNativeExecutionShuffleManager shuffleManager = (PrestoSparkNativeExecutionShuffleManager) SparkEnv.get().shuffleManager();
        Set<Integer> partitions = shuffleManager.getAllPartitions();

        for (Integer partition : partitions) {
            Optional<ShuffleHandle> shuffleHandle = shuffleManager.getShuffleHandle(partition);
            assertTrue(shuffleHandle.isPresent());
            assertTrue(shuffleHandle.get() instanceof BypassMergeSortShuffleHandle);
        }
    }

    protected void assertQuery(String sql)
    {
        super.assertQuery(sql);
        assertShuffleMetadata();
    }
}
