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
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerHiveProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerSystemProperties;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.METASTORE_CONTEXT;
import static java.util.Objects.requireNonNull;

public class PrestoSparkNativeQueryRunner
{
    private static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
    private static final String FALLBACK_SPARK_SHUFFLE_MANAGER = "spark.fallback.shuffle.manager";
    private static final int AVAILABLE_CPU_COUNT = 4;

    private PrestoSparkNativeQueryRunner() {}

    public static QueryRunner createQueryRunner()
    {
        ImmutableMap.Builder<String, String> configBuilder = new ImmutableMap.Builder<String, String>()
                .putAll(getNativeWorkerSystemProperties())
                // Do not use default Prestissimo config files. Presto-Spark will generate the configs on-the-fly.
                .put("catalog.config-dir", "/")
                .put("native-execution-enabled", "true")
                .put("spark.initial-partition-count", "1")
                .put("spark.partition-count-auto-tune-enabled", "false");

        if (System.getProperty("NATIVE_PORT") == null) {
            String path = requireNonNull(System.getProperty("PRESTO_SERVER"),
                    "Native worker binary path is missing. " +
                            "Add -DPRESTO_SERVER=/path/to/native/process/bin to your JVM arguments.");
            configBuilder.put("native-execution-executable-path", path);
        }

        String dataDirectory = System.getProperty("DATA_DIR");

        PrestoSparkQueryRunner queryRunner = new PrestoSparkQueryRunner(
                "hive",
                configBuilder.build(),
                getNativeWorkerHiveProperties(),
                getNativeExecutionShuffleConfigs(),
                Optional.ofNullable(dataDirectory).map(Paths::get),
                AVAILABLE_CPU_COUNT);

        ExtendedHiveMetastore metastore = queryRunner.getMetastore();
        if (!metastore.getDatabase(METASTORE_CONTEXT, "tpch").isPresent()) {
            metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject("tpch"));
        }
        return queryRunner;
    }

    private static Map<String, String> getNativeExecutionShuffleConfigs()
    {
        ImmutableMap.Builder<String, String> sparkConfigs = ImmutableMap.builder();
        sparkConfigs.put(SPARK_SHUFFLE_MANAGER, "com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager");
        sparkConfigs.put(FALLBACK_SPARK_SHUFFLE_MANAGER, "org.apache.spark.shuffle.sort.SortShuffleManager");
        return sparkConfigs.build();
    }

    private static Database createDatabaseMetastoreObject(String name)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }

    public static QueryRunner createJavaQueryRunner() throws Exception {
        String dataDirectory = System.getProperty("DATA_DIR");
        return HiveExternalWorkerQueryRunner.createJavaQueryRunner(Optional.ofNullable(dataDirectory).map(Paths::get), "legacy");
    }
}
