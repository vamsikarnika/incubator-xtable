/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.catalog.glue;

import static io.onetable.catalog.glue.GlueCatalogSyncOperations.GLUE_EXTERNAL_TABLE_TYPE;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getInputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getOutputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getSerDeClassName;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import com.google.common.annotations.VisibleForTesting;

import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.exception.CatalogSyncException;
import io.onetable.hudi.HudiSparkDataSourceTableUtils;
import io.onetable.hudi.HudiTableManager;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.TableFormat;
import io.onetable.reflection.ReflectionUtils;

@Log4j2
public class HudiGlueCatalogSyncRequestProvider extends GlueCatalogSyncRequestProvider {

  private final Configuration configuration;
  private final GlueSchemaExtractor schemaExtractor;
  private final GlueClient glueClient;
  private final HudiTableManager hudiTableManager;
  private final PartitionValueExtractor partitionValueExtractor;
  private HoodieTableMetaClient metaClient;

  public HudiGlueCatalogSyncRequestProvider(
      GlueCatalogConfig glueCatalogConfig,
      GlueClient glueClient,
      GlueSchemaExtractor schemaExtractor,
      Configuration configuration) {
    super(glueCatalogConfig);
    this.glueClient = glueClient;
    this.configuration = configuration;
    this.schemaExtractor = schemaExtractor;
    this.hudiTableManager = HudiTableManager.of(configuration);
    this.partitionValueExtractor =
        ReflectionUtils.createInstanceOfClass(glueCatalogConfig.getPartitionExtractorClass());
  }

  @VisibleForTesting
  HudiGlueCatalogSyncRequestProvider(
      GlueCatalogConfig glueCatalogConfig,
      GlueClient glueClient,
      GlueSchemaExtractor schemaExtractor,
      HudiTableManager hudiTableManager,
      Configuration configuration,
      HoodieTableMetaClient metaClient,
      PartitionValueExtractor partitionValueExtractor) {
    super(glueCatalogConfig);
    this.hudiTableManager = hudiTableManager;
    this.glueClient = glueClient;
    this.schemaExtractor = schemaExtractor;
    this.configuration = configuration;
    this.metaClient = metaClient;
    this.partitionValueExtractor = partitionValueExtractor;
  }

  HoodieTableMetaClient getMetaClient(String basePath) {
    if (metaClient == null) {
      Optional<HoodieTableMetaClient> metaClientOpt =
          hudiTableManager.loadTableMetaClientIfExists(basePath);

      if (!metaClientOpt.isPresent()) {
        throw new CatalogSyncException(
            "failed to get meta client since table is not present in the base path " + basePath);
      }

      metaClient = metaClientOpt.get();
    }
    return metaClient;
  }

  @Override
  TableInput getCreateTableInput(
      OneTable table, ExternalCatalogConfig.TableIdentifier tableIdentifier) {
    final Instant now = Instant.now();
    List<String> partitionFields =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(getTableParameters(partitionFields, table.getReadSchema()))
        .partitionKeys(getSchemaPartitionKeys(table.getPartitioningFields()))
        .storageDescriptor(getStorageDescriptor(table))
        .lastAccessTime(now)
        .lastAnalyzedTime(now)
        .build();
  }

  @Override
  TableInput getUpdateTableInput(
      OneTable table, Table glueTable, ExternalCatalogConfig.TableIdentifier tableIdentifier) {
    List<String> partitionFields =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    Map<String, String> tableParameters = new HashMap<>(glueTable.parameters());
    tableParameters.putAll(getTableParameters(partitionFields, table.getReadSchema()));
    List<Column> newColumns = getSchemaWithoutPartitionKeys(table);
    StorageDescriptor sd = glueTable.storageDescriptor();
    StorageDescriptor partitionSD = sd.copy(copySd -> copySd.columns(newColumns));

    final Instant now = Instant.now();
    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(glueTable.tableType())
        .parameters(tableParameters)
        .partitionKeys(getSchemaPartitionKeys(table.getPartitioningFields()))
        .storageDescriptor(partitionSD)
        .lastAccessTime(now)
        .lastAnalyzedTime(now)
        .build();
  }

  @VisibleForTesting
  Map<String, String> getTableParameters(List<String> partitionFields, OneSchema schema) {
    Map<String, String> sparkTableProperties =
        HudiSparkDataSourceTableUtils.getSparkTableProperties(
            partitionFields, "", glueCatalogConfig.getSchemaLengthThreshold(), schema);
    return new HashMap<>(sparkTableProperties);
  }

  @VisibleForTesting
  StorageDescriptor getStorageDescriptor(OneTable table) {
    HoodieFileFormat baseFileFormat =
        getMetaClient(table.getBasePath()).getTableConfig().getBaseFileFormat();
    SerDeInfo serDeInfo =
        SerDeInfo.builder()
            .serializationLibrary(getSerDeClassName(baseFileFormat))
            .parameters(getSerdeProperties(false, table.getBasePath()))
            .build();
    return StorageDescriptor.builder()
        .serdeInfo(serDeInfo)
        .location(table.getBasePath())
        .inputFormat(getInputFormatClassName(baseFileFormat, false))
        .outputFormat(getOutputFormatClassName(baseFileFormat))
        .columns(getSchemaWithoutPartitionKeys(table))
        .build();
  }

  List<Column> getSchemaWithoutPartitionKeys(OneTable table) {
    List<String> partitionKeys =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    return schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema()).stream()
        .filter(c -> !partitionKeys.contains(c.name()))
        .collect(Collectors.toList());
  }

  List<Column> getSchemaPartitionKeys(List<OnePartitionField> partitioningFields) {
    return partitioningFields.stream()
        .map(
            field -> {
              String fieldName = field.getSourceField().getName();
              String fieldType =
                  schemaExtractor.toTypeString(
                      field.getSourceField().getSchema(), TableFormat.HUDI);
              return Column.builder().name(fieldName).type(fieldType).build();
            })
        .collect(Collectors.toList());
  }

  Map<String, String> getSerdeProperties(boolean readAsOptimized, String basePath) {
    Map<String, String> serdeProperties = new HashMap<>();
    serdeProperties.put(ConfigUtils.TABLE_SERDE_PATH, basePath);
    serdeProperties.put(ConfigUtils.IS_QUERY_AS_RO_TABLE, String.valueOf(readAsOptimized));
    return serdeProperties;
  }
}
