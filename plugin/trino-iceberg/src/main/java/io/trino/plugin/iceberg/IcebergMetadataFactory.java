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
package io.trino.plugin.iceberg;

import io.airlift.json.JsonCodec;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
{
    private final CatalogName catalogName;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final HiveTableOperationsProvider tableOperationsProvider;
    private final String trinoVersion;
    private final boolean useUniqueTableLocation;

    @Inject
    public IcebergMetadataFactory(
            CatalogName catalogName,
            IcebergConfig config,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskDataJsonCodec,
            HiveTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion)
    {
        this(catalogName, metastore, hdfsEnvironment, typeManager, commitTaskDataJsonCodec, tableOperationsProvider, nodeVersion, config.isUniqueTableLocation());
    }

    public IcebergMetadataFactory(
            CatalogName catalogName,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            HiveTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion,
            boolean useUniqueTableLocation)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.useUniqueTableLocation = useUniqueTableLocation;
    }

    public IcebergMetadata create()
    {
        return new IcebergMetadata(catalogName, metastore, hdfsEnvironment, typeManager, commitTaskCodec, tableOperationsProvider, trinoVersion, useUniqueTableLocation);
    }
}
