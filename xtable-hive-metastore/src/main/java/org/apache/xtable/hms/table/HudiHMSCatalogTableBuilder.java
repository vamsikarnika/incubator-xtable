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
 
package org.apache.xtable.hms.table;

import static org.apache.xtable.catalog.CatalogUtils.toHierarchicalTableIdentifier;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConfigUtils;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.hms.HMSCatalogConfig;
import org.apache.xtable.hms.HMSSchemaExtractor;
import org.apache.xtable.hudi.HudiInputFormatUtils;
import org.apache.xtable.hudi.HudiSparkDataSourceTableUtils;
import org.apache.xtable.hudi.HudiTableManager;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.TableFormat;

@Log4j2
public class HudiHMSCatalogTableBuilder implements CatalogTableBuilder<Table, Table> {

  private final HudiTableManager hudiTableManager;
  private final HMSSchemaExtractor schemaExtractor;
  private final HMSCatalogConfig hmsCatalogConfig;

  private HoodieTableMetaClient metaClient;

  protected static final String HUDI_METADATA_CONFIG = "hudi.metadata-listing-enabled";

  public HudiHMSCatalogTableBuilder(
      HMSCatalogConfig hmsCatalogConfig, Configuration configuration) {
    this.hudiTableManager = HudiTableManager.of(configuration);
    this.schemaExtractor = HMSSchemaExtractor.getInstance();
    this.hmsCatalogConfig = hmsCatalogConfig;
  }

  @VisibleForTesting
  HudiHMSCatalogTableBuilder(
      HMSCatalogConfig hmsCatalogConfig,
      HMSSchemaExtractor schemaExtractor,
      HudiTableManager hudiTableManager,
      HoodieTableMetaClient metaClient) {
    this.hudiTableManager = hudiTableManager;
    this.schemaExtractor = schemaExtractor;
    this.metaClient = metaClient;
    this.hmsCatalogConfig = hmsCatalogConfig;
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
  public Table getCreateTableRequest(
      InternalTable table, CatalogTableIdentifier catalogTableIdentifier) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    Table newTb = new Table();
    newTb.setDbName(tableIdentifier.getDatabaseName());
    newTb.setTableName(tableIdentifier.getTableName());
    try {
      newTb.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
    } catch (IOException e) {
      throw new CatalogSyncException(
          "Failed to set owner for hms table: " + tableIdentifier.getTableName(), e);
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
  public Table getUpdateTableRequest(
      InternalTable table, Table hmsTable, CatalogTableIdentifier tableIdentifier) {
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
    hmsTable.getSd().setCols(schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema()));
    return hmsTable;
  }

  private Map<String, String> getTableProperties(
      List<String> partitionFields, InternalSchema schema) {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(HUDI_METADATA_CONFIG, "true");
    Map<String, String> sparkTableProperties =
        HudiSparkDataSourceTableUtils.getSparkTableProperties(
            partitionFields, "", hmsCatalogConfig.getSchemaLengthThreshold(), schema);
    tableProperties.putAll(sparkTableProperties);
    return tableProperties;
  }

  @VisibleForTesting
  StorageDescriptor getStorageDescriptor(InternalTable table) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema()));
    storageDescriptor.setLocation(table.getBasePath());
    HoodieFileFormat fileFormat =
        getMetaClient(table.getBasePath()).getTableConfig().getBaseFileFormat();
    String inputFormatClassName = HudiInputFormatUtils.getInputFormatClassName(fileFormat, false);
    String outputFormatClassName = HudiInputFormatUtils.getOutputFormatClassName(fileFormat);
    String serdeClassName = HudiInputFormatUtils.getSerDeClassName(fileFormat);
    storageDescriptor.setInputFormat(inputFormatClassName);
    storageDescriptor.setOutputFormat(outputFormatClassName);
    Map<String, String> serdeProperties = getSerdeProperties(false, table.getBasePath());
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib(serdeClassName);
    serDeInfo.setParameters(serdeProperties);
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private static Map<String, String> getSerdeProperties(boolean readAsOptimized, String basePath) {
    Map<String, String> serdeProperties = new HashMap<>();
    serdeProperties.put(ConfigUtils.TABLE_SERDE_PATH, basePath);
    serdeProperties.put(ConfigUtils.IS_QUERY_AS_RO_TABLE, String.valueOf(readAsOptimized));
    return serdeProperties;
  }

  List<FieldSchema> getSchemaPartitionKeys(InternalTable table) {

    List<InternalPartitionField> partitioningFields = table.getPartitioningFields();
    Map<String, FieldSchema> fieldSchemaMap =
        schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema()).stream()
            .collect(Collectors.toMap(FieldSchema::getName, field -> field));

    return partitioningFields.stream()
        .map(
            partitionField -> {
              if (fieldSchemaMap.containsKey(partitionField.getSourceField().getName())) {
                return fieldSchemaMap.get(partitionField.getSourceField().getName());
              } else {
                return new FieldSchema(partitionField.getSourceField().getName(), "string", "");
              }
            })
        .collect(Collectors.toList());
  }
}
