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

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;

import io.onetable.catalog.CatalogConfigFactory;
import io.onetable.catalog.CatalogSyncOperations;
import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
import io.onetable.model.OneTable;

@Log4j2
public abstract class GlueCatalogSyncOperations implements CatalogSyncOperations<Database, Table> {
  protected static final String GLUE_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
  // Error encountered during table refresh that require table re-creation
  private static final List<String> RECREATE_TABLE_ERROR_MESSAGES = Collections.emptyList();
  private static final String TEMP_SUFFIX = "_temp";

  protected final TableIdentifier tableIdentifier;
  protected final GlueClient glueClient;
  protected final GlueCatalogConfig glueCatalogConfig;
  protected final Configuration configuration;
  protected final GlueSchemaExtractor schemaExtractor;

  public GlueCatalogSyncOperations(
      ExternalCatalogConfig externalCatalogConfig, Configuration configuration) {
    this.glueCatalogConfig =
        CatalogConfigFactory.getGlueCatalogConfig(externalCatalogConfig.getCatalogProperties());
    this.glueClient = new DefaultGlueClientFactory(glueCatalogConfig).getGlueClient();
    this.tableIdentifier = externalCatalogConfig.getTableFormatsToSync().get(getTableFormat());
    this.configuration = new Configuration(configuration);
    this.schemaExtractor = GlueSchemaExtractor.getInstance();
  }

  GlueCatalogSyncOperations(
      TableIdentifier tableIdentifier,
      GlueClient glueClient,
      GlueCatalogConfig glueCatalogConfig,
      Configuration configuration,
      GlueSchemaExtractor schemaExtractor) {
    this.tableIdentifier = tableIdentifier;
    this.glueClient = glueClient;
    this.glueCatalogConfig = glueCatalogConfig;
    this.configuration = new Configuration(configuration);
    this.schemaExtractor = schemaExtractor;
  }

  @Override
  public Database getDatabase(String databaseName) {
    try {
      return glueClient
          .getDatabase(
              GetDatabaseRequest.builder()
                  .catalogId(glueCatalogConfig.getCatalogId())
                  .name(databaseName)
                  .build())
          .database();
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    glueClient.createDatabase(
        CreateDatabaseRequest.builder()
            .catalogId(glueCatalogConfig.getCatalogId())
            .databaseInput(
                DatabaseInput.builder()
                    .name(databaseName)
                    .description("Automatically created by " + this.getClass().getName())
                    .build())
            .build());
  }

  @Override
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
  public void createOrReplaceTable(OneTable table, TableIdentifier tableIdentifier) {
    // validate before dropping the table
    validateTempTableCreation(table, tableIdentifier);
    dropTable(table, tableIdentifier);
    createTable(table, tableIdentifier);
  }

  @Override
  public void dropTable(OneTable table, TableIdentifier tableIdentifier) {
    glueClient.deleteTable(
        DeleteTableRequest.builder()
            .catalogId(glueCatalogConfig.getCatalogId())
            .databaseName(tableIdentifier.getDatabaseName())
            .name(tableIdentifier.getTableName())
            .build());
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

  @Override
  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  @Override
  public String getStorageDescriptorLocation(Table table) {
    if (table == null || table.storageDescriptor() == null) {
      return null;
    }
    return table.storageDescriptor().location();
  }

  @Override
  public void close() {
    if (this.glueClient != null) {
      this.glueClient.close();
    }
  }
}
