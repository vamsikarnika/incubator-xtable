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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import java.lang.reflect.InvocationTargetException;
import java.time.ZonedDateTime;
import java.util.Collections;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import org.apache.hudi.common.util.VisibleForTesting;

import io.onetable.catalog.CatalogConfigFactory;
import io.onetable.catalog.CatalogSyncOperations;
import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
import io.onetable.exception.CatalogSyncException;
import io.onetable.model.OneTable;

@Log4j2
public abstract class HMSCatalogSyncOperations implements CatalogSyncOperations<Database, Table> {

  private static final String TEMP_SUFFIX = "_temp";
  protected final TableIdentifier tableIdentifier;
  protected final HMSCatalogConfig hmsCatalogConfig;
  protected final Configuration configuration;
  protected final IMetaStoreClient metaStoreClient;
  protected final HMSSchemaExtractor schemaExtractor;

  public HMSCatalogSyncOperations(
      ExternalCatalogConfig externalCatalogConfig, Configuration configuration) {
    this.tableIdentifier = externalCatalogConfig.getTableFormatsToSync().get(getTableFormat());
    this.hmsCatalogConfig =
        CatalogConfigFactory.getHMSCatalogConfig(externalCatalogConfig.getCatalogProperties());
    this.configuration = new Configuration(configuration);
    this.schemaExtractor = HMSSchemaExtractor.getInstance();
    try {
      this.metaStoreClient = getMSC();
    } catch (MetaException | HiveException e) {
      throw new CatalogSyncException("HiveMetastoreClient could not be created", e);
    }
  }

  public HMSCatalogSyncOperations(
      TableIdentifier tableIdentifier,
      HMSCatalogConfig catalogConfig,
      Configuration configuration,
      IMetaStoreClient metaStoreClient,
      HMSSchemaExtractor schemaExtractor) {
    this.tableIdentifier = tableIdentifier;
    this.hmsCatalogConfig = catalogConfig;
    this.configuration = new Configuration(configuration);
    this.metaStoreClient = metaStoreClient;
    this.schemaExtractor = schemaExtractor;
  }

  @Override
  public Database getDatabase(String databaseName) {
    try {
      return metaStoreClient.getDatabase(databaseName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException e) {
      throw new CatalogSyncException("Failed to get database: " + databaseName, e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    try {
      Database database =
          new Database(
              databaseName, "created by HMSCatalogSyncClient", null, Collections.emptyMap());
      metaStoreClient.createDatabase(database);
    } catch (TException e) {
      throw new CatalogSyncException("Failed to create database: " + databaseName, e);
    }
  }

  @Override
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

  @Override
  public void dropTable(OneTable table, TableIdentifier tableIdentifier) {
    try {
      metaStoreClient.dropTable(tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
    } catch (TException e) {
      throw new CatalogSyncException("Failed to drop table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void createOrReplaceTable(OneTable table, TableIdentifier tableIdentifier) {
    // validate before dropping the table
    validateTempTableCreation(table, tableIdentifier);
    dropTable(table, tableIdentifier);
    createTable(table, tableIdentifier);
  }

  @Override
  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  @Override
  public String getStorageDescriptorLocation(Table hmsTable) {
    if (hmsTable == null || hmsTable.getSd() == null) {
      return null;
    }
    return hmsTable.getSd().getLocation();
  }

  /**
   * creates a temp table with new metadata and properties to ensure table creation succeeds before
   * dropping the table and recreating it. This ensures that actual table is not dropped in case
   * there are any issues
   */
  private void validateTempTableCreation(OneTable table, TableIdentifier tableIdentifier) {
    String tempTableName =
        tableIdentifier.getTableName() + TEMP_SUFFIX + ZonedDateTime.now().toEpochSecond();
    TableIdentifier tempTableIdentifier =
        TableIdentifier.builder()
            .tableName(tempTableName)
            .databaseName(tableIdentifier.getDatabaseName())
            .build();
    createTable(table, tempTableIdentifier);
    dropTable(table, tempTableIdentifier);
  }

  @VisibleForTesting
  IMetaStoreClient getMSC() throws MetaException, HiveException {
    HiveConf hiveConf = new HiveConf(configuration, HiveConf.class);
    hiveConf.set(METASTOREURIS.varname, hmsCatalogConfig.getServerUrl());
    IMetaStoreClient metaStoreClient;
    try {
      metaStoreClient =
          ((Hive)
                  Hive.class
                      .getMethod("getWithoutRegisterFns", HiveConf.class)
                      .invoke(null, hiveConf))
              .getMSC();
    } catch (NoSuchMethodException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException ex) {
      metaStoreClient = Hive.get(hiveConf).getMSC();
    }
    log.debug("Connected to metastore with uri: {}", hmsCatalogConfig.getServerUrl());
    return metaStoreClient;
  }

  @Override
  public void close() {
    if (this.metaStoreClient != null) {
      this.metaStoreClient.close();
    }
  }
}
