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
import static io.onetable.hudi.HudiPartitionSyncTool.LAST_COMMIT_COMPLETION_TIME_SYNC;
import static io.onetable.hudi.HudiPartitionSyncTool.LAST_COMMIT_TIME_SYNC;
import static org.apache.hudi.common.util.CollectionUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.MapUtils.containsAll;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getInputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getOutputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getSerDeClassName;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequestEntry;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

import com.google.common.annotations.VisibleForTesting;

import io.onetable.catalog.CatalogPartitionSyncOperations;
import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
import io.onetable.catalog.Partition;
import io.onetable.exception.CatalogSyncException;
import io.onetable.hudi.HudiPartitionSyncTool;
import io.onetable.hudi.HudiSparkDataSourceTableUtils;
import io.onetable.hudi.HudiTableManager;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.TableFormat;
import io.onetable.reflection.ReflectionUtils;

@Log4j2
public class HudiGlueCatalogSyncRequestProvider extends GlueCatalogSyncRequestProvider
    implements CatalogPartitionSyncOperations {

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

  @Override
  public void syncPartitions(OneTable oneTable, TableIdentifier tableIdentifier) {
    try {
      Table table = getTable(tableIdentifier);
      Option<String> lastCommitTimeSynced =
          Option.ofNullable(table.parameters().get(LAST_COMMIT_TIME_SYNC));
      Option<String> lastCommitCompletionTimeSynced =
          Option.ofNullable(table.parameters().get(LAST_COMMIT_COMPLETION_TIME_SYNC));
      HudiPartitionSyncTool hudiPartitionSyncTool =
          new HudiPartitionSyncTool(
              getMetaClient(oneTable.getBasePath()), this, partitionValueExtractor);
      boolean updatePartitions =
          hudiPartitionSyncTool.syncPartitions(
              oneTable,
              tableIdentifier,
              configuration,
              lastCommitTimeSynced,
              lastCommitCompletionTimeSynced);
      if (updatePartitions) {
        updateLastCommitTimeSynced(tableIdentifier);
      }
      log.info("synced all partitions for table: {}", tableIdentifier.getId());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to create hms table: " + tableIdentifier.getId(), e);
    }
  }

  private void updateLastCommitTimeSynced(TableIdentifier tableIdentifier) {
    if (!metaClient.getActiveTimeline().lastInstant().isPresent()) {
      log.warn("No commit in active timeline.");
      return;
    }
    final String lastCommitTimestamp =
        metaClient.getActiveTimeline().lastInstant().get().getTimestamp();
    try {
      updateTableParameters(
          tableIdentifier, Collections.singletonMap(LAST_COMMIT_TIME_SYNC, lastCommitTimestamp));
    } catch (Exception e) {
      throw new CatalogSyncException(
          "Fail to update last sync commit time for " + tableIdentifier, e);
    }
  }

  private boolean updateTableParameters(
      TableIdentifier tableIdentifier, Map<String, String> updatingParams) {
    if (isNullOrEmpty(updatingParams)) {
      return false;
    }
    try {
      Table table = getTable(tableIdentifier);
      Map<String, String> remoteParams = table.parameters();
      if (containsAll(remoteParams, updatingParams)) {
        return false;
      }

      final Map<String, String> newParams = new HashMap<>();
      newParams.putAll(table.parameters());
      newParams.putAll(updatingParams);

      final Instant now = Instant.now();
      TableInput updatedTableInput =
          TableInput.builder()
              .name(tableIdentifier.getTableName())
              .tableType(table.tableType())
              .parameters(newParams)
              .partitionKeys(table.partitionKeys())
              .storageDescriptor(table.storageDescriptor())
              .lastAccessTime(now)
              .lastAnalyzedTime(now)
              .build();

      UpdateTableRequest request =
          UpdateTableRequest.builder()
              .databaseName(tableIdentifier.getDatabaseName())
              .tableInput(updatedTableInput)
              .skipArchive(true)
              .build();
      glueClient.updateTable(request);
      return true;
    } catch (Exception e) {
      throw new CatalogSyncException(
          "Fail to update params for table " + tableIdentifier + ": " + updatingParams, e);
    }
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

  public Table getTable(TableIdentifier tableIdentifier) {
    try {
      GetTableResponse response =
          glueClient.getTable(
              GetTableRequest.builder()
                  .catalogId(glueCatalogConfig.getCatalogId())
                  .databaseName(tableIdentifier.getDatabaseName())
                  .name(tableIdentifier.getTableName())
                  .build());
      return response.table();
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  @Override
  public List<Partition> getAllPartitions(TableIdentifier tableIdentifier) {
    try {
      List<Partition> partitions = new ArrayList<>();
      String nextToken = null;
      do {
        GetPartitionsResponse result =
            glueClient.getPartitions(
                GetPartitionsRequest.builder()
                    .databaseName(tableIdentifier.getDatabaseName())
                    .tableName(tableIdentifier.getDatabaseName())
                    .nextToken(nextToken)
                    .build());
        partitions.addAll(
            result.partitions().stream()
                .map(p -> new Partition(p.values(), p.storageDescriptor().location()))
                .collect(Collectors.toList()));
        nextToken = result.nextToken();
      } while (nextToken != null);
      return partitions;
    } catch (Exception e) {
      throw new CatalogSyncException(
          "Failed to get all partitions for table " + tableIdentifier, e);
    }
  }

  @Override
  public void addPartitionsToTable(TableIdentifier tableIdentifier, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      log.info("No partitions to add for " + tableIdentifier);
      return;
    }
    log.info("Adding " + partitionsToAdd.size() + " partition(s) in table " + tableIdentifier);
    try {
      Table table = getTable(tableIdentifier);
      StorageDescriptor sd = table.storageDescriptor();
      List<PartitionInput> partitionInputs =
          partitionsToAdd.stream()
              .map(
                  partition -> {
                    String fullPartitionPath =
                        FSUtils.getPartitionPath(metaClient.getBasePathV2(), partition).toString();
                    List<String> partitionValues =
                        partitionValueExtractor.extractPartitionValuesInPath(partition);
                    StorageDescriptor partitionSD =
                        sd.copy(copySd -> copySd.location(fullPartitionPath));
                    return PartitionInput.builder()
                        .values(partitionValues)
                        .storageDescriptor(partitionSD)
                        .build();
                  })
              .collect(Collectors.toList());

      List<BatchCreatePartitionResponse> responses = new ArrayList<>();

      // ToDo - create a config in glueCatalogConfig for MAX_PARTITIONS_PER_REQUEST, for now taking
      // default value 1000
      for (List<PartitionInput> batch : CollectionUtils.batches(partitionInputs, 1000)) {
        BatchCreatePartitionRequest request =
            BatchCreatePartitionRequest.builder()
                .databaseName(tableIdentifier.getDatabaseName())
                .tableName(tableIdentifier.getTableName())
                .partitionInputList(batch)
                .build();
        responses.add(glueClient.batchCreatePartition(request));
      }

      for (BatchCreatePartitionResponse response : responses) {
        if (CollectionUtils.nonEmpty(response.errors())) {
          if (response.errors().stream()
              .allMatch(
                  (error) -> "AlreadyExistsException".equals(error.errorDetail().errorCode()))) {
            log.warn("Partitions already exist in glue: " + response.errors());
          } else {
            throw new CatalogSyncException(
                "Fail to add partitions to "
                    + tableIdentifier
                    + " with error(s): "
                    + response.errors());
          }
        }
      }
    } catch (Exception e) {
      throw new CatalogSyncException("Fail to add partitions to " + tableIdentifier, e);
    }
  }

  @Override
  public void updatePartitionsToTable(
      TableIdentifier tableIdentifier, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      log.info("No partitions to change for " + tableIdentifier.getTableName());
      return;
    }
    log.info("Updating " + changedPartitions.size() + "partition(s) in table " + tableIdentifier);
    try {
      Table table = getTable(tableIdentifier);
      StorageDescriptor sd = table.storageDescriptor();
      List<BatchUpdatePartitionRequestEntry> updatePartitionEntries =
          changedPartitions.stream()
              .map(
                  partition -> {
                    String fullPartitionPath =
                        FSUtils.getPartitionPath(metaClient.getBasePathV2(), partition).toString();
                    List<String> partitionValues =
                        partitionValueExtractor.extractPartitionValuesInPath(partition);
                    StorageDescriptor partitionSD =
                        sd.copy(copySd -> copySd.location(fullPartitionPath));
                    PartitionInput partitionInput =
                        PartitionInput.builder()
                            .values(partitionValues)
                            .storageDescriptor(partitionSD)
                            .build();
                    return BatchUpdatePartitionRequestEntry.builder()
                        .partitionInput(partitionInput)
                        .partitionValueList(partitionValues)
                        .build();
                  })
              .collect(Collectors.toList());

      List<BatchUpdatePartitionResponse> responses = new ArrayList<>();
      // ToDo - create a config in glueCatalogConfig for MAX_PARTITIONS_PER_REQUEST, for now taking
      // default value 1000
      for (List<BatchUpdatePartitionRequestEntry> batch :
          CollectionUtils.batches(updatePartitionEntries, 1000)) {
        BatchUpdatePartitionRequest request =
            BatchUpdatePartitionRequest.builder()
                .databaseName(tableIdentifier.getDatabaseName())
                .tableName(tableIdentifier.getTableName())
                .entries(batch)
                .build();
        responses.add(glueClient.batchUpdatePartition(request));
      }

      for (BatchUpdatePartitionResponse response : responses) {
        if (CollectionUtils.nonEmpty(response.errors())) {
          throw new CatalogSyncException(
              "Fail to update partitions to "
                  + tableIdentifier
                  + " with error(s): "
                  + response.errors());
        }
      }
    } catch (Exception e) {
      throw new CatalogSyncException("Fail to update partitions to " + tableIdentifier, e);
    }
  }

  @Override
  public void dropPartitions(TableIdentifier tableIdentifier, List<String> partitionsToDrop) {
    if (isNullOrEmpty(partitionsToDrop)) {
      log.info("No partitions to drop for " + tableIdentifier);
      return;
    }
    log.info("Drop " + partitionsToDrop.size() + "partition(s) in table " + tableIdentifier);
    try {
      List<BatchDeletePartitionResponse> responses = new ArrayList<>();
      // ToDo - create a config in glueCatalogConfig for MAX_PARTITIONS_PER_REQUEST, for now taking
      // default value 1000
      for (List<String> batch : CollectionUtils.batches(partitionsToDrop, 1000)) {
        List<PartitionValueList> partitionValueLists =
            batch.stream()
                .map(
                    partition ->
                        PartitionValueList.builder()
                            .values(partitionValueExtractor.extractPartitionValuesInPath(partition))
                            .build())
                .collect(Collectors.toList());

        BatchDeletePartitionRequest batchDeletePartitionRequest =
            BatchDeletePartitionRequest.builder()
                .databaseName(tableIdentifier.getDatabaseName())
                .tableName(tableIdentifier.getTableName())
                .partitionsToDelete(partitionValueLists)
                .build();
        responses.add(glueClient.batchDeletePartition(batchDeletePartitionRequest));
      }

      for (BatchDeletePartitionResponse response : responses) {
        if (CollectionUtils.nonEmpty(response.errors())) {
          throw new CatalogSyncException(
              "Fail to drop partitions to "
                  + tableIdentifier
                  + " with error(s): "
                  + response.errors());
        }
      }
    } catch (Exception e) {
      throw new CatalogSyncException("Fail to drop partitions to " + tableIdentifier, e);
    }
  }
}
