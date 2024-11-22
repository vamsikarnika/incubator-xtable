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

import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.hive.HiveIcebergInputFormat;
import org.apache.iceberg.mr.hive.HiveIcebergOutputFormat;
import org.apache.iceberg.mr.hive.HiveIcebergSerDe;
import org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;

import com.google.common.annotations.VisibleForTesting;

import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
import io.onetable.exception.CatalogSyncException;
import io.onetable.model.OneTable;
import io.onetable.model.storage.TableFormat;

public class IcebergHMSCatalogSyncOperations extends HMSCatalogSyncOperations {

  private final HadoopTables hadoopTables;
  private static final String ICEBERG_CATALOG_NAME_PROP = "iceberg.catalog";
  private static final String ICEBERG_HADOOP_TABLE_NAME = "location_based_table";

  public IcebergHMSCatalogSyncOperations(
      ExternalCatalogConfig externalCatalogConfig, Configuration configuration) {
    super(externalCatalogConfig, configuration);
    this.hadoopTables = new HadoopTables(configuration);
  }

  @VisibleForTesting
  public IcebergHMSCatalogSyncOperations(
      TableIdentifier tableIdentifier,
      HMSCatalogConfig catalogConfig,
      Configuration configuration,
      IMetaStoreClient metaStoreClient,
      HadoopTables hadoopTables,
      HMSSchemaExtractor schemaExtractor) {
    super(tableIdentifier, catalogConfig, configuration, metaStoreClient, schemaExtractor);
    this.hadoopTables = hadoopTables;
  }

  @Override
  public void createTable(OneTable table, TableIdentifier tableIdentifier) {
    try {
      metaStoreClient.createTable(newHmsTable(table, tableIdentifier));
    } catch (TException e) {
      throw new CatalogSyncException("Failed to create hms table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void refreshTable(OneTable table, Table hmsTable, TableIdentifier tableIdentifier) {
    try {
      BaseTable icebergTable = loadTableFromFs(table.getBasePath());
      Map<String, String> parameters = hmsTable.getParameters();
      String currentMetadataLocation = parameters.get(METADATA_LOCATION_PROP);
      parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation);
      parameters.put(METADATA_LOCATION_PROP, getMetadataFileLocation(icebergTable));
      hmsTable.setParameters(parameters);
      hmsTable.getSd().setCols(schemaExtractor.toColumns(getTableFormat(), table.getReadSchema()));
      metaStoreClient.alter_table(
          tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(), hmsTable);
    } catch (TException e) {
      throw new CatalogSyncException("Failed to refresh hms table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public String getTableFormat() {
    return TableFormat.ICEBERG;
  }

  @VisibleForTesting
  StorageDescriptor getStorageDescriptor(OneTable table) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(schemaExtractor.toColumns(getTableFormat(), table.getReadSchema()));
    storageDescriptor.setLocation(table.getBasePath());
    storageDescriptor.setInputFormat(HiveIcebergInputFormat.class.getCanonicalName());
    storageDescriptor.setOutputFormat(HiveIcebergOutputFormat.class.getCanonicalName());
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib(HiveIcebergSerDe.class.getCanonicalName());
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  @VisibleForTesting
  Table newHmsTable(OneTable table, TableIdentifier tableIdentifier) {
    try {
      Table newTb = new Table();
      newTb.setDbName(tableIdentifier.getDatabaseName());
      newTb.setTableName(tableIdentifier.getTableName());
      newTb.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      newTb.setCreateTime((int) ZonedDateTime.now().toEpochSecond());
      newTb.setSd(getStorageDescriptor(table));
      newTb.setTableType(TableType.EXTERNAL_TABLE.toString());
      newTb.setParameters(getHmsTableParameters(loadTableFromFs(table.getBasePath())));
      return newTb;
    } catch (IOException e) {
      throw new CatalogSyncException(
          "Failed to set owner for hms table: " + tableIdentifier.getId(), e);
    }
  }

  @VisibleForTesting
  Map<String, String> getHmsTableParameters(BaseTable icebergTable) {
    Map<String, String> parameters = new HashMap<>(icebergTable.properties());
    parameters.put("EXTERNAL", "TRUE");
    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(METADATA_LOCATION_PROP, getMetadataFileLocation(icebergTable));
    parameters.put(
        hive_metastoreConstants.META_TABLE_STORAGE,
        HiveIcebergStorageHandler.class.getCanonicalName());
    // Iceberg tries to commit the table after it has been created in HMS
    // as part of HiveIcebergMetaHook and results in "Table Already exists" exception.
    // Adding below param will prevent the same
    parameters.put(ICEBERG_CATALOG_NAME_PROP, ICEBERG_HADOOP_TABLE_NAME);
    return parameters;
  }

  private BaseTable loadTableFromFs(String tableBasePath) {
    return (BaseTable) hadoopTables.load(tableBasePath);
  }

  private String getMetadataFileLocation(BaseTable table) {
    return table.operations().current().metadataFileLocation();
  }
}
