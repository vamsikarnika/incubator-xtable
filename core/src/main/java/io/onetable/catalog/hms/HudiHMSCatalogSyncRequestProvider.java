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
 
package io.onetable.catalog.hms;

import static io.onetable.hudi.HudiPartitionSyncTool.LAST_COMMIT_COMPLETION_TIME_SYNC;
import static io.onetable.hudi.HudiPartitionSyncTool.LAST_COMMIT_TIME_SYNC;
import static io.onetable.hudi.HudiPartitionSyncTool.getSerdeProperties;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getInputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getOutputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getSerDeClassName;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import com.google.common.annotations.VisibleForTesting;

import io.onetable.catalog.CatalogPartitionSyncOperations;
import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
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
public class HudiHMSCatalogSyncRequestProvider extends HMSCatalogSyncRequestProvider
    implements CatalogPartitionSyncOperations {

  private final HudiTableManager hudiTableManager;
  private final IMetaStoreClient metaStoreClient;
  private final HMSSchemaExtractor schemaExtractor;
  private final PartitionValueExtractor partitionValueExtractor;
  private final Configuration configuration;

  private HoodieTableMetaClient metaClient;

  public HudiHMSCatalogSyncRequestProvider(
      HMSCatalogConfig hmsCatalogConfig,
      IMetaStoreClient metaStoreClient,
      HMSSchemaExtractor schemaExtractor,
      Configuration configuration) {
    super(hmsCatalogConfig);
    this.hudiTableManager = HudiTableManager.of(configuration);
    this.metaStoreClient = metaStoreClient;
    this.schemaExtractor = schemaExtractor;
    this.configuration = configuration;
    // TODO - fetch this class name from hms catalog configs
    this.partitionValueExtractor =
        ReflectionUtils.createInstanceOfClass(MultiPartKeysValueExtractor.class.getName());
  }

  @VisibleForTesting
  HudiHMSCatalogSyncRequestProvider(
      HMSCatalogConfig hmsCatalogConfig,
      IMetaStoreClient metaStoreClient,
      HMSSchemaExtractor schemaExtractor,
      HudiTableManager hudiTableManager,
      Configuration configuration,
      HoodieTableMetaClient metaClient,
      PartitionValueExtractor partitionValueExtractor) {
    super(hmsCatalogConfig);
    this.hudiTableManager = hudiTableManager;
    this.metaStoreClient = metaStoreClient;
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
  Table getCreateTableInput(OneTable table, TableIdentifier tableIdentifier) {
    Table newTb = new Table();
    newTb.setDbName(tableIdentifier.getDatabaseName());
    newTb.setTableName(tableIdentifier.getTableName());
    try {
      newTb.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
    } catch (IOException e) {
      throw new CatalogSyncException(
          "Failed to set owner for hms table: " + tableIdentifier.getId(), e);
    }

    newTb.setCreateTime((int) ZonedDateTime.now().toEpochSecond());
    List<String> partitionFields =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    Map<String, String> tableProperties =
        getTableProperties(partitionFields, table.getReadSchema());
    newTb.setParameters(tableProperties);
    newTb.setSd(getStorageDescriptor(table));
    newTb.setPartitionKeys(getSchemaPartitionKeys(table));
    return newTb;
  }

  @Override
  Table getUpdateTableInput(OneTable table, Table hmsTable, TableIdentifier tableIdentifier) {
    Map<String, String> parameters = hmsTable.getParameters();
    List<String> partitionFields =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    Map<String, String> tableParameters = hmsTable.getParameters();
    tableParameters.putAll(getTableProperties(partitionFields, table.getReadSchema()));
    hmsTable.setParameters(tableParameters);
    hmsTable.setSd(getStorageDescriptor(table));

    hmsTable.setParameters(parameters);
    hmsTable.getSd().setCols(getSchemaWithoutPartitionKeys(table));
    return hmsTable;
  }

  @Override
  protected void syncPartitions(OneTable oneTable, TableIdentifier tableIdentifier) {
    Table table = getTable(tableIdentifier);
    Option<String> lastCommitTimeSynced =
        Option.ofNullable(table.getParameters().get(LAST_COMMIT_TIME_SYNC));
    Option<String> lastCommitCompletionTimeSynced =
        Option.ofNullable(table.getParameters().get(LAST_COMMIT_COMPLETION_TIME_SYNC));
    HoodieTableMetaClient metaClient = getMetaClient(oneTable.getBasePath());
    HudiPartitionSyncTool hudiPartitionSyncTool =
        new HudiPartitionSyncTool(metaClient, this, partitionValueExtractor);
    boolean updatedPartitions =
        hudiPartitionSyncTool.syncPartitions(
            oneTable,
            tableIdentifier,
            configuration,
            lastCommitTimeSynced,
            lastCommitCompletionTimeSynced);
    if (updatedPartitions) {
      updateLastCommitTimeSynced(tableIdentifier);
    }
  }

  public Table getTable(TableIdentifier tableIdentifier) {
    try {
      return metaStoreClient.getTable(
          tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException e) {
      throw new CatalogSyncException("Failed to get table: " + tableIdentifier.getId(), e);
    }
  }

  private Map<String, String> getTableProperties(List<String> partitionFields, OneSchema schema) {
    Map<String, String> sparkTableProperties =
        HudiSparkDataSourceTableUtils.getSparkTableProperties(
            partitionFields, "", hmsCatalogConfig.getSchemaLengthThreshold(), schema);
    return new HashMap<>(sparkTableProperties);
  }

  @VisibleForTesting
  StorageDescriptor getStorageDescriptor(OneTable table) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(getSchemaWithoutPartitionKeys(table));
    storageDescriptor.setLocation(table.getBasePath());
    HoodieFileFormat fileFormat =
        getMetaClient(table.getBasePath()).getTableConfig().getBaseFileFormat();
    String inputFormatClassName = getInputFormatClassName(fileFormat, false);
    String outputFormatClassName = getOutputFormatClassName(fileFormat);
    String serdeClassName = getSerDeClassName(fileFormat);
    storageDescriptor.setInputFormat(inputFormatClassName);
    storageDescriptor.setOutputFormat(outputFormatClassName);
    Map<String, String> serdeProperties = getSerdeProperties(false, table.getBasePath());
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib(serdeClassName);
    serDeInfo.setParameters(serdeProperties);
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  List<FieldSchema> getSchemaWithoutPartitionKeys(OneTable table) {
    List<String> partitionKeys =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    return schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema()).stream()
        .filter(c -> !partitionKeys.contains(c.getName()))
        .collect(Collectors.toList());
  }

  List<FieldSchema> getSchemaPartitionKeys(OneTable table) {
    List<FieldSchema> allFields =
        schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema());
    List<OnePartitionField> onePartitioningFields = table.getPartitioningFields();
    Map<String, FieldSchema> fieldSchemaMap =
        allFields.stream().collect(Collectors.toMap(FieldSchema::getName, field -> field));

    return onePartitioningFields.stream()
        .map(
            onePartitionField -> {
              if (fieldSchemaMap.containsKey(onePartitionField.getSourceField().getName())) {
                return fieldSchemaMap.get(onePartitionField.getSourceField().getName());
              } else {
                return new FieldSchema(onePartitionField.getSourceField().getName(), "string", "");
              }
            })
        .collect(Collectors.toList());
  }

  @Override
  public List<io.onetable.catalog.Partition> getAllPartitions(
      ExternalCatalogConfig.TableIdentifier tableIdentifier) {
    try {
      return metaStoreClient
          .listPartitions(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(), (short) -1)
          .stream()
          .map(p -> new io.onetable.catalog.Partition(p.getValues(), p.getSd().getLocation()))
          .collect(Collectors.toList());
    } catch (TException e) {
      throw new CatalogSyncException(
          "Failed to get all partitions for table " + tableIdentifier, e);
    }
  }

  @VisibleForTesting
  void updateLastCommitTimeSynced(TableIdentifier tableIdentifier) {
    HoodieTimeline activeTimeline = metaClient.getActiveTimeline();
    Option<String> lastCommitSynced = activeTimeline.lastInstant().map(HoodieInstant::getTimestamp);
    Option<String> lastCommitCompletionSynced =
        activeTimeline
            .getInstantsOrderedByStateTransitionTime()
            .skip(activeTimeline.countInstants() - 1)
            .findFirst()
            .map(i -> Option.of(i.getStateTransitionTime()))
            .orElse(Option.empty());

    if (lastCommitSynced.isPresent()) {
      try {
        Table table =
            metaStoreClient.getTable(
                tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
        String basePath = metaClient.getBasePathV2().toString();
        StorageDescriptor sd = table.getSd();
        sd.setLocation(basePath);
        SerDeInfo serdeInfo = sd.getSerdeInfo();
        serdeInfo.putToParameters(ConfigUtils.TABLE_SERDE_PATH, basePath);
        Map<String, String> tableParameters = table.getParameters();
        tableParameters.put(LAST_COMMIT_TIME_SYNC, lastCommitSynced.get());
        tableParameters.put(LAST_COMMIT_COMPLETION_TIME_SYNC, lastCommitCompletionSynced.get());
        metaStoreClient.alter_table(
            tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(), table);
      } catch (TException e) {
        throw new CatalogSyncException(
            "failed to update last commit time synced for table " + tableIdentifier, e);
      }
    }
  }

  @Override
  public void addPartitionsToTable(
      ExternalCatalogConfig.TableIdentifier tableIdentifier, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      log.info("No partitions to add for " + tableIdentifier);
      return;
    }
    log.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableIdentifier);
    try {
      StorageDescriptor sd =
          metaStoreClient
              .getTable(tableIdentifier.getDatabaseName(), tableIdentifier.getTableName())
              .getSd();
      // int batchSyncPartitionNum = syncConfig.getIntOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM);
      int batchSyncPartitionNum = 1000;
      for (List<String> batch : CollectionUtils.batches(partitionsToAdd, batchSyncPartitionNum)) {
        List<Partition> partitionList = new ArrayList<>();
        batch.forEach(
            x -> {
              StorageDescriptor partitionSd = new StorageDescriptor();
              partitionSd.setCols(sd.getCols());
              partitionSd.setInputFormat(sd.getInputFormat());
              partitionSd.setOutputFormat(sd.getOutputFormat());
              partitionSd.setSerdeInfo(sd.getSerdeInfo());
              String fullPartitionPath =
                  FSUtils.getPartitionPath(metaClient.getBasePathV2(), x).toString();
              List<String> partitionValues =
                  partitionValueExtractor.extractPartitionValuesInPath(x);
              partitionSd.setLocation(fullPartitionPath);
              partitionList.add(
                  new Partition(
                      partitionValues,
                      tableIdentifier.getDatabaseName(),
                      tableIdentifier.getTableName(),
                      0,
                      0,
                      partitionSd,
                      null));
            });
        metaStoreClient.add_partitions(partitionList, true, false);
        log.info("HMSDDLExecutor add a batch partitions done: " + partitionList.size());
      }
    } catch (TException e) {
      log.error("{} add partition failed", tableIdentifier, e);
      throw new CatalogSyncException(tableIdentifier + " add partition failed", e);
    }
  }

  @Override
  public void updatePartitionsToTable(
      ExternalCatalogConfig.TableIdentifier tableIdentifier, List<String> changedPartitions) {
    try {
      Table table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
      StorageDescriptor tableSd = table.getSd();

      List<Partition> updatedPartitions = new ArrayList<>();

      changedPartitions.forEach(
          partitionPath -> {
            StorageDescriptor partitionSd = new StorageDescriptor(tableSd);
            partitionSd.setLocation(partitionPath);

            Partition partition = new Partition();
            partition.setDbName(tableIdentifier.getDatabaseName());
            List<String> partitionValues =
                partitionValueExtractor.extractPartitionValuesInPath(partitionPath);
            partition.setTableName(tableIdentifier.getTableName());
            partition.setValues(partitionValues);
            partition.setSd(partitionSd);
            updatedPartitions.add(partition);
          });

      // Update partitions (drop existing and add new ones with updated locations)
      for (Partition partition : updatedPartitions) {
        metaStoreClient.dropPartition(
            tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(),
            partition.getValues(),
            false);
        metaStoreClient.add_partition(partition);
      }
    } catch (TException e) {
      throw new CatalogSyncException(
          "Failed to update partitions for the table " + tableIdentifier, e);
    }
  }

  @Override
  public void dropPartitions(
      ExternalCatalogConfig.TableIdentifier tableIdentifier, List<String> partitionsToDrop) {
    try {
      for (String partitionPath : partitionsToDrop) {
        List<String> partitionValues =
            partitionValueExtractor.extractPartitionValuesInPath(partitionPath);
        metaStoreClient.dropPartition(
            tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(),
            partitionValues,
            false);
      }
    } catch (TException e) {
      throw new CatalogSyncException("Failed to drop partitions for table " + tableIdentifier, e);
    }
  }
}
