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

import static io.onetable.hudi.HudiPartitionSyncTool.getSerdeProperties;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getInputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getOutputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getSerDeClassName;

import java.io.IOException;
import java.time.ZonedDateTime;
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
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import com.google.common.annotations.VisibleForTesting;

import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
import io.onetable.exception.CatalogSyncException;
import io.onetable.hudi.HudiSparkDataSourceTableUtils;
import io.onetable.hudi.HudiTableManager;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.TableFormat;
import io.onetable.reflection.ReflectionUtils;

@Log4j2
public class HudiHMSCatalogSyncRequestProvider extends HMSCatalogSyncRequestProvider {

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
    this.partitionValueExtractor =
        ReflectionUtils.createInstanceOfClass(hmsCatalogConfig.getPartitionExtractorClass());
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
    hmsTable.getSd().setCols(schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema()));
    return hmsTable;
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
    storageDescriptor.setCols(schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema()));
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
}
