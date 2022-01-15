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
package io.trino.plugin.hive;

import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.plugin.hive.security.AccessControlMetadataFactory;
import io.trino.plugin.hive.statistics.HiveStatisticsProvider;
import io.trino.plugin.hive.statistics.MetastoreHiveStatisticsProvider;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static java.util.Objects.requireNonNull;

public class HiveMetadataFactory
        implements TransactionalMetadataFactory
{
    private final CatalogName catalogName;
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final boolean translateHiveViews;
    private final boolean hideDeltaLakeTables;
    private final long perTransactionCacheMaximumSize;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final BoundedExecutor renameExecution;
    private final BoundedExecutor dropExecutor;
    private final Executor updateExecutor;
    private final String trinoVersion;
    private final HiveRedirectionsProvider hiveRedirectionsProvider;
    private final Set<SystemTableProvider> systemTableProviders;
    private final HiveMaterializedViewMetadataFactory hiveMaterializedViewMetadataFactory;
    private final AccessControlMetadataFactory accessControlMetadataFactory;
    private final Optional<Duration> hiveTransactionHeartbeatInterval;
    private final ScheduledExecutorService heartbeatService;

    @Inject
    public HiveMetadataFactory(
            CatalogName catalogName,
            HiveConfig hiveConfig,
            MetastoreConfig metastoreConfig,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            ExecutorService executorService,
            @ForHiveTransactionHeartbeats ScheduledExecutorService heartbeatService,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            NodeVersion nodeVersion,
            HiveRedirectionsProvider hiveRedirectionsProvider,
            Set<SystemTableProvider> systemTableProviders,
            HiveMaterializedViewMetadataFactory hiveMaterializedViewMetadataFactory,
            AccessControlMetadataFactory accessControlMetadataFactory)
    {
        this(
                catalogName,
                metastore,
                hdfsEnvironment,
                partitionManager,
                hiveConfig.getMaxConcurrentFileRenames(),
                hiveConfig.getMaxConcurrentMetastoreDrops(),
                hiveConfig.getMaxConcurrentMetastoreUpdates(),
                hiveConfig.isSkipDeletionForAlter(),
                hiveConfig.isSkipTargetCleanupOnRollback(),
                hiveConfig.getWritesToNonManagedTablesEnabled(),
                hiveConfig.getCreatesOfNonManagedTablesEnabled(),
                hiveConfig.isTranslateHiveViews(),
                hiveConfig.getPerTransactionMetastoreCacheMaximumSize(),
                hiveConfig.getHiveTransactionHeartbeatInterval(),
                metastoreConfig.isHideDeltaLakeTables(),
                typeManager,
                locationService,
                partitionUpdateCodec,
                executorService,
                heartbeatService,
                nodeVersion.toString(),
                hiveRedirectionsProvider,
                systemTableProviders,
                hiveMaterializedViewMetadataFactory,
                accessControlMetadataFactory);
    }

    public HiveMetadataFactory(
            CatalogName catalogName,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            int maxConcurrentFileRenames,
            int maxConcurrentMetastoreDrops,
            int maxConcurrentMetastoreUpdates,
            boolean skipDeletionForAlter,
            boolean skipTargetCleanupOnRollback,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean translateHiveViews,
            long perTransactionCacheMaximumSize,
            Optional<Duration> hiveTransactionHeartbeatInterval,
            boolean hideDeltaLakeTables,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ExecutorService executorService,
            ScheduledExecutorService heartbeatService,
            String trinoVersion,
            HiveRedirectionsProvider hiveRedirectionsProvider,
            Set<SystemTableProvider> systemTableProviders,
            HiveMaterializedViewMetadataFactory hiveMaterializedViewMetadataFactory,
            AccessControlMetadataFactory accessControlMetadataFactory)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.translateHiveViews = translateHiveViews;
        this.hideDeltaLakeTables = hideDeltaLakeTables;
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.hiveRedirectionsProvider = requireNonNull(hiveRedirectionsProvider, "hiveRedirectionsProvider is null");
        this.systemTableProviders = requireNonNull(systemTableProviders, "systemTableProviders is null");
        this.hiveMaterializedViewMetadataFactory = requireNonNull(hiveMaterializedViewMetadataFactory, "hiveMaterializedViewMetadataFactory is null");
        this.accessControlMetadataFactory = requireNonNull(accessControlMetadataFactory, "accessControlMetadataFactory is null");
        this.hiveTransactionHeartbeatInterval = requireNonNull(hiveTransactionHeartbeatInterval, "hiveTransactionHeartbeatInterval is null");

        renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
        dropExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreDrops);
        if (maxConcurrentMetastoreUpdates == 1) {
            // this will serve as a kill switch in case we observe that parallel updates causes conflicts in metastore's DB side
            updateExecutor = directExecutor();
        }
        else {
            updateExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreUpdates);
        }
        this.heartbeatService = requireNonNull(heartbeatService, "heartbeatService is null");
    }

    @Override
    public TransactionalMetadata create()
    {
        HiveMetastoreClosure hiveMetastoreClosure = new HiveMetastoreClosure(
                memoizeMetastore(this.metastore, perTransactionCacheMaximumSize)); // per-transaction cache
        SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                hiveMetastoreClosure,
                renameExecution,
                dropExecutor,
                updateExecutor,
                skipDeletionForAlter,
                skipTargetCleanupOnRollback,
                hiveTransactionHeartbeatInterval,
                heartbeatService);

        return create(
                catalogName,
                metastore,
                hdfsEnvironment,
                partitionManager,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                translateHiveViews,
                hideDeltaLakeTables,
                typeManager,
                locationService,
                partitionUpdateCodec,
                trinoVersion,
                new MetastoreHiveStatisticsProvider(metastore),
                hiveRedirectionsProvider,
                systemTableProviders,
                hiveMaterializedViewMetadataFactory.create(hiveMetastoreClosure),
                accessControlMetadataFactory.create(metastore));
    }

    protected TransactionalMetadata create(
            CatalogName catalogName,
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean translateHiveViews,
            boolean hideDeltaLakeTables,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            String trinoVersion,
            HiveStatisticsProvider hiveStatisticsProvider,
            HiveRedirectionsProvider hiveRedirectionsProvider,
            Set<SystemTableProvider> systemTableProviders,
            HiveMaterializedViewMetadata hiveMaterializedViewMetadata,
            AccessControlMetadata accessControlMetadata)
    {
        return new HiveMetadata(
                catalogName,
                metastore,
                hdfsEnvironment,
                partitionManager,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                translateHiveViews,
                hideDeltaLakeTables,
                typeManager,
                locationService,
                partitionUpdateCodec,
                trinoVersion,
                hiveStatisticsProvider,
                hiveRedirectionsProvider,
                systemTableProviders,
                hiveMaterializedViewMetadata,
                accessControlMetadata);
    }
}
