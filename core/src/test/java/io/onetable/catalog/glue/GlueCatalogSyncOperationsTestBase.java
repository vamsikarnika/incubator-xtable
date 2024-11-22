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

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneSchema;

public class GlueCatalogSyncOperationsTestBase {

  @Mock protected GlueClient mockGlueClient;
  @Mock protected GlueCatalogConfig mockGlueCatalogConfig;
  @Mock protected GlueSchemaExtractor mockGlueSchemaExtractor;
  protected Configuration mockConfiguration = new Configuration();

  protected static final String GLUE_DATABASE = "glue_db";
  protected static final String GLUE_TABLE = "glue_table";
  protected static final String GLUE_CATALOG_ID = "glue-catalog-id";
  protected static final String ONETABLE_BASE_PATH = "onetable-base-path";
  protected static final OneTable TEST_ONETABLE =
      OneTable.builder()
          .basePath(ONETABLE_BASE_PATH)
          .readSchema(OneSchema.builder().fields(Collections.emptyList()).build())
          .build();
  protected static final ExternalCatalogConfig.TableIdentifier TEST_TABLE_IDENTIFIER =
      ExternalCatalogConfig.TableIdentifier.builder()
          .databaseName(GLUE_DATABASE)
          .tableName(GLUE_TABLE)
          .build();

  protected GetDatabaseRequest getDbRequest(String dbName) {
    return GetDatabaseRequest.builder().catalogId(GLUE_CATALOG_ID).name(dbName).build();
  }

  protected GetTableRequest getTableRequest(String dbName, String tableName) {
    return GetTableRequest.builder()
        .catalogId(GLUE_CATALOG_ID)
        .databaseName(dbName)
        .name(tableName)
        .build();
  }

  protected CreateDatabaseRequest createDbRequest(String dbName) {
    return CreateDatabaseRequest.builder()
        .catalogId(GLUE_CATALOG_ID)
        .databaseInput(
            DatabaseInput.builder()
                .name(dbName)
                .description(
                    "Automatically created by io.onetable.catalog.glue.IcebergGlueCatalogSyncOperations")
                .build())
        .build();
  }

  protected CreateTableRequest createTableRequest(
      String dbName, String tableName, Map<String, String> parameters) {
    return CreateTableRequest.builder()
        .catalogId(GLUE_CATALOG_ID)
        .databaseName(dbName)
        .tableInput(
            TableInput.builder()
                .name(tableName)
                .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                .parameters(parameters)
                .storageDescriptor(
                    StorageDescriptor.builder()
                        .location(TEST_ONETABLE.getBasePath())
                        .columns(Collections.emptyList())
                        .build())
                .build())
        .build();
  }

  protected UpdateTableRequest updateTableRequest(
      String dbName, String tableName, Map<String, String> parameters) {
    return UpdateTableRequest.builder()
        .catalogId(GLUE_CATALOG_ID)
        .databaseName(dbName)
        .skipArchive(true)
        .tableInput(
            TableInput.builder()
                .name(tableName)
                .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                .parameters(parameters)
                .storageDescriptor(
                    StorageDescriptor.builder()
                        .location(TEST_ONETABLE.getBasePath())
                        .columns(Collections.emptyList())
                        .build())
                .build())
        .build();
  }

  protected DeleteTableRequest deleteTableRequest(String dbName, String tableName) {
    return DeleteTableRequest.builder()
        .catalogId(GLUE_CATALOG_ID)
        .databaseName(dbName)
        .name(tableName)
        .build();
  }

  protected Table getGlueTable(String dbName, String tableName, String location) {
    return Table.builder()
        .databaseName(dbName)
        .name(tableName)
        .storageDescriptor(StorageDescriptor.builder().location(location).build())
        .build();
  }
}
