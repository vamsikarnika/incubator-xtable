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

import static io.onetable.catalog.glue.IcebergGlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE;
import static io.onetable.catalog.glue.IcebergGlueCatalogSyncClient.GLUE_ICEBERG_METADATA_LOCATION_PROP;
import static io.onetable.catalog.glue.IcebergGlueCatalogSyncClient.GLUE_ICEBERG_PREV_METADATA_LOCATION_PROP;
import static io.onetable.catalog.glue.IcebergGlueCatalogSyncClient.GLUE_TABLE_TYPE_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.exception.CatalogSyncException;
import io.onetable.model.OneTable;
import io.onetable.model.storage.TableFormat;

@ExtendWith(MockitoExtension.class)
public class TestIcebergGlueCatalogSyncClient {

  @Mock private GlueClient mockGlueClient;
  @Mock private Configuration mockConfiguration;
  @Mock private HadoopTables mockHadoopTables;
  @Mock private GlueCatalogConfig mockGlueCatalogConfig;
  @Mock private BaseTable mockBaseTable;
  @Mock private TableOperations mockTableOperations;
  @Mock private TableMetadata mockTableMetadata;
  private MockedStatic<GlueSchemaExtractor> mockGlueSchemaExtractor;

  private static final String ICEBERG_GLUE_DATABASE = "iceberg_glue_db";
  private static final String ICEBERG_GLUE_TABLE = "iceberg_glue_table";
  private static final String GLUE_CATALOG_ID = "aws-account-id";
  private static final String ONETABLE_BASE_PATH = "onetable-base-path";
  private static final String ICEBERG_METADATA_FILE_LOCATION = "onetable-base-path/metadata";
  private static final OneTable TEST_ONETABLE =
      OneTable.builder().basePath(ONETABLE_BASE_PATH).build();
  private static final ExternalCatalogConfig.TableIdentifier TEST_TABLE_IDENTIFIER =
      ExternalCatalogConfig.TableIdentifier.builder()
          .databaseName(ICEBERG_GLUE_DATABASE)
          .tableName(ICEBERG_GLUE_TABLE)
          .build();
  private IcebergGlueCatalogSyncClient mockSyncClient;

  @AfterEach
  void teardown() {
    if (mockGlueSchemaExtractor != null) {
      mockGlueSchemaExtractor.close();
    }
  }

  void mockSyncClient() {
    mockSyncClient =
        new IcebergGlueCatalogSyncClient(
            TEST_TABLE_IDENTIFIER,
            mockGlueClient,
            mockGlueCatalogConfig,
            mockConfiguration,
            mockHadoopTables);
  }

  void setupCommonMocks() {
    when(mockGlueCatalogConfig.getCatalogId()).thenReturn(GLUE_CATALOG_ID);
    mockSyncClient();
  }

  void mockHadoopTables(boolean shouldLoadTable) {
    if (shouldLoadTable) {
      when(mockHadoopTables.load(ONETABLE_BASE_PATH)).thenReturn(mockBaseTable);
    }
    when(mockBaseTable.operations()).thenReturn(mockTableOperations);
    when(mockTableOperations.current()).thenReturn(mockTableMetadata);
    when(mockTableMetadata.metadataFileLocation()).thenReturn(ICEBERG_METADATA_FILE_LOCATION);
  }

  void mockHadoopTables() {
    mockHadoopTables(true);
  }

  void mockStaticGlueSchemaExtractor() {
    mockGlueSchemaExtractor = mockStatic(GlueSchemaExtractor.class);
    mockGlueSchemaExtractor
        .when(() -> GlueSchemaExtractor.toColumns(any(), any()))
        .thenReturn(Collections.emptyList());
    mockGlueSchemaExtractor
        .when(() -> GlueSchemaExtractor.toColumns(any(), any(), any()))
        .thenReturn(Collections.emptyList());
  }

  private GetDatabaseRequest getDbRequest(String dbName) {
    return GetDatabaseRequest.builder().catalogId(GLUE_CATALOG_ID).name(dbName).build();
  }

  private GetTableRequest getTableRequest(String dbName, String tableName) {
    return GetTableRequest.builder()
        .catalogId(GLUE_CATALOG_ID)
        .databaseName(dbName)
        .name(tableName)
        .build();
  }

  private CreateDatabaseRequest createDbRequest(String dbName) {
    return CreateDatabaseRequest.builder()
        .catalogId(GLUE_CATALOG_ID)
        .databaseInput(
            DatabaseInput.builder()
                .name(dbName)
                .description(
                    "Automatically created by io.onetable.catalog.glue.IcebergGlueCatalogSyncClient")
                .build())
        .build();
  }

  private CreateTableRequest createTableRequest(
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

  private UpdateTableRequest updateTableRequest(
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

  private DeleteTableRequest deleteTableRequest(String dbName, String tableName) {
    return DeleteTableRequest.builder()
        .catalogId(GLUE_CATALOG_ID)
        .databaseName(dbName)
        .name(tableName)
        .build();
  }

  private Table getGlueTable(String dbName, String tableName, String location) {
    return Table.builder()
        .databaseName(dbName)
        .name(tableName)
        .storageDescriptor(StorageDescriptor.builder().location(location).build())
        .build();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetDatabase(boolean isDbPresent) {
    setupCommonMocks();
    GetDatabaseRequest dbRequest = getDbRequest(TEST_TABLE_IDENTIFIER.getDatabaseName());
    GetDatabaseResponse dbResponse =
        GetDatabaseResponse.builder()
            .database(Database.builder().name(TEST_TABLE_IDENTIFIER.getDatabaseName()).build())
            .build();
    if (isDbPresent) {
      when(mockGlueClient.getDatabase(dbRequest)).thenReturn(dbResponse);
    } else {
      when(mockGlueClient.getDatabase(dbRequest))
          .thenThrow(EntityNotFoundException.builder().message("db not found").build());
    }
    Database db = mockSyncClient.getDatabase(TEST_TABLE_IDENTIFIER.getDatabaseName());
    if (isDbPresent) {
      assertNotNull(db);
      assertEquals(TEST_TABLE_IDENTIFIER.getDatabaseName(), db.name());
    } else {
      assertNull(db);
    }
    verify(mockGlueClient, times(1)).getDatabase(dbRequest);
  }

  @Test
  void testGetDatabaseFailure() {
    setupCommonMocks();
    GetDatabaseRequest dbRequest = getDbRequest(TEST_TABLE_IDENTIFIER.getDatabaseName());
    when(mockGlueClient.getDatabase(dbRequest))
        .thenThrow(new RuntimeException("something went wrong"));
    assertThrows(
        RuntimeException.class,
        () -> mockSyncClient.getDatabase(TEST_TABLE_IDENTIFIER.getDatabaseName()));
    verify(mockGlueClient, times(1)).getDatabase(dbRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetTable(boolean isTablePresent) {
    setupCommonMocks();
    GetTableRequest tableRequest =
        getTableRequest(
            TEST_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_IDENTIFIER.getTableName());
    GetTableResponse tableResponse =
        GetTableResponse.builder()
            .table(
                Table.builder()
                    .databaseName(TEST_TABLE_IDENTIFIER.getDatabaseName())
                    .name(TEST_TABLE_IDENTIFIER.getTableName())
                    .build())
            .build();
    if (isTablePresent) {
      when(mockGlueClient.getTable(tableRequest)).thenReturn(tableResponse);
    } else {
      when(mockGlueClient.getTable(tableRequest))
          .thenThrow(EntityNotFoundException.builder().message("table not found").build());
    }
    Table table = mockSyncClient.getTable(TEST_TABLE_IDENTIFIER);
    if (isTablePresent) {
      assertNotNull(table);
      assertEquals(TEST_TABLE_IDENTIFIER.getDatabaseName(), table.databaseName());
      assertEquals(TEST_TABLE_IDENTIFIER.getTableName(), table.name());
    } else {
      assertNull(table);
    }
    verify(mockGlueClient, times(1)).getTable(tableRequest);
  }

  @Test
  void testGetTableFailure() {
    setupCommonMocks();
    GetTableRequest tableRequest =
        getTableRequest(
            TEST_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_IDENTIFIER.getTableName());
    when(mockGlueClient.getTable(tableRequest))
        .thenThrow(new RuntimeException("something went wrong"));
    assertThrows(RuntimeException.class, () -> mockSyncClient.getTable(TEST_TABLE_IDENTIFIER));
    verify(mockGlueClient, times(1)).getTable(tableRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateDatabase(boolean shouldFail) {
    setupCommonMocks();
    CreateDatabaseRequest dbRequest = createDbRequest(TEST_TABLE_IDENTIFIER.getDatabaseName());
    if (shouldFail) {
      when(mockGlueClient.createDatabase(dbRequest))
          .thenThrow(new RuntimeException("something went wrong"));
      assertThrows(
          RuntimeException.class,
          () -> mockSyncClient.createDatabase(TEST_TABLE_IDENTIFIER.getDatabaseName()));
    } else {
      when(mockGlueClient.createDatabase(dbRequest))
          .thenReturn(CreateDatabaseResponse.builder().build());
      mockSyncClient.createDatabase(TEST_TABLE_IDENTIFIER.getDatabaseName());
    }
    verify(mockGlueClient, times(1)).createDatabase(dbRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateTable(boolean shouldFail) {
    setupCommonMocks();
    mockHadoopTables();
    mockStaticGlueSchemaExtractor();
    CreateTableRequest createTableRequest =
        createTableRequest(
            TEST_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_TABLE_IDENTIFIER.getTableName(),
            mockSyncClient.getTableParameters(mockBaseTable));
    if (shouldFail) {
      when(mockGlueClient.createTable(createTableRequest))
          .thenThrow(new RuntimeException("something went wrong"));
      assertThrows(
          RuntimeException.class,
          () -> mockSyncClient.createTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER));
    } else {
      when(mockGlueClient.createTable(createTableRequest))
          .thenReturn(CreateTableResponse.builder().build());
      mockSyncClient.createTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).createTable(createTableRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRefreshTable(boolean shouldFail) {
    setupCommonMocks();
    mockHadoopTables();
    mockStaticGlueSchemaExtractor();
    Map<String, String> glueTableParams = new HashMap<>();
    glueTableParams.put(GLUE_ICEBERG_METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    Table glueTable = Table.builder().parameters(glueTableParams).build();
    Map<String, String> parameters = new HashMap<>();
    parameters.put(
        GLUE_ICEBERG_PREV_METADATA_LOCATION_PROP,
        glueTableParams.get(GLUE_ICEBERG_METADATA_LOCATION_PROP));
    parameters.putAll(mockSyncClient.getTableParameters(mockBaseTable));
    UpdateTableRequest tableReq =
        updateTableRequest(
            TEST_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_TABLE_IDENTIFIER.getTableName(),
            parameters);
    if (shouldFail) {
      when(mockGlueClient.updateTable(tableReq))
          .thenThrow(new RuntimeException("something went wrong"));
      assertThrows(
          RuntimeException.class,
          () -> mockSyncClient.refreshTable(TEST_ONETABLE, glueTable, TEST_TABLE_IDENTIFIER));
    } else {
      when(mockGlueClient.updateTable(tableReq)).thenReturn(UpdateTableResponse.builder().build());
      mockSyncClient.refreshTable(TEST_ONETABLE, glueTable, TEST_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).updateTable(tableReq);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDropTable(boolean shouldFail) {
    setupCommonMocks();
    DeleteTableRequest deleteRequest =
        deleteTableRequest(
            TEST_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_IDENTIFIER.getTableName());
    if (shouldFail) {
      when(mockGlueClient.deleteTable(deleteRequest))
          .thenThrow(new RuntimeException("something went wrong"));
      assertThrows(
          RuntimeException.class,
          () -> mockSyncClient.dropTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER));
    } else {
      when(mockGlueClient.deleteTable(deleteRequest))
          .thenReturn(DeleteTableResponse.builder().build());
      mockSyncClient.dropTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).deleteTable(deleteRequest);
  }

  @Test
  void testCreateOrReplaceTable() {
    setupCommonMocks();
    mockHadoopTables();
    mockStaticGlueSchemaExtractor();
    ZonedDateTime fixedDateTime = ZonedDateTime.parse("2024-10-25T10:15:30.00Z");
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(fixedDateTime);
      CreateTableRequest mainCreateTableRequest =
          createTableRequest(
              TEST_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_TABLE_IDENTIFIER.getTableName(),
              mockSyncClient.getTableParameters(mockBaseTable));
      DeleteTableRequest mainTableDeleteRequest =
          deleteTableRequest(
              TEST_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_IDENTIFIER.getTableName());
      CreateTableRequest tempCreateTableRequest =
          createTableRequest(
              TEST_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_TABLE_IDENTIFIER.getTableName() + "_temp" + ZonedDateTime.now().toEpochSecond(),
              mockSyncClient.getTableParameters(mockBaseTable));
      DeleteTableRequest tempTableDeleteRequest =
          deleteTableRequest(
              TEST_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_TABLE_IDENTIFIER.getTableName() + "_temp" + ZonedDateTime.now().toEpochSecond());

      when(mockGlueClient.createTable(mainCreateTableRequest))
          .thenReturn(CreateTableResponse.builder().build());
      when(mockGlueClient.createTable(tempCreateTableRequest))
          .thenReturn(CreateTableResponse.builder().build());
      when(mockGlueClient.deleteTable(mainTableDeleteRequest))
          .thenReturn(DeleteTableResponse.builder().build());
      when(mockGlueClient.deleteTable(tempTableDeleteRequest))
          .thenReturn(DeleteTableResponse.builder().build());

      mockSyncClient.createOrReplaceTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);

      verify(mockGlueClient, times(1)).createTable(tempCreateTableRequest);
      verify(mockGlueClient, times(1)).deleteTable(tempTableDeleteRequest);
      verify(mockGlueClient, times(1)).createTable(mainCreateTableRequest);
      verify(mockGlueClient, times(1)).deleteTable(mainTableDeleteRequest);
    }
  }

  @Test
  void testGetTableParameters() {
    Map<String, String> expected = new HashMap<>();
    expected.put(GLUE_TABLE_TYPE_PROP, TableFormat.ICEBERG);
    expected.put(GLUE_ICEBERG_METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    mockHadoopTables(false);
    mockSyncClient =
        new IcebergGlueCatalogSyncClient(
            TEST_TABLE_IDENTIFIER,
            mockGlueClient,
            mockGlueCatalogConfig,
            mockConfiguration,
            mockHadoopTables);
    Map<String, String> tableParameters = mockSyncClient.getTableParameters(mockBaseTable);
    assertEquals(expected, tableParameters);
  }

  @Test
  void testSyncTable_DatabaseDoesNotExists() {
    setupCommonMocks();
    mockHadoopTables();
    mockStaticGlueSchemaExtractor();

    // mock database does not exists
    when(mockGlueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().build());
    // create database succeed
    when(mockGlueClient.createDatabase(any(CreateDatabaseRequest.class))).thenReturn(null);
    // mock table does not exist
    when(mockGlueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().build());
    // create table succeed
    when(mockGlueClient.createTable(any(CreateTableRequest.class))).thenReturn(null);

    mockSyncClient.syncTable(TEST_ONETABLE);

    verifyGlueDbApiCalls(1, 1);
    verifyGlueTableApiCalls(1, 1, 0, 0);
  }

  @Test
  void testSyncTable_DatabaseExistsButTableDoesNotExists() {
    setupCommonMocks();
    mockHadoopTables();
    mockStaticGlueSchemaExtractor();

    String dbname = TEST_TABLE_IDENTIFIER.getDatabaseName();
    Database db = Database.builder().name(dbname).build();

    // mock databases exists
    when(mockGlueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().database(db).build());
    // mock table does not exist
    when(mockGlueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().build());
    // create table succeed
    when(mockGlueClient.createTable(any(CreateTableRequest.class))).thenReturn(null);

    mockSyncClient.syncTable(TEST_ONETABLE);

    verifyGlueDbApiCalls(1, 0);
    verifyGlueTableApiCalls(1, 1, 0, 0);
  }

  @Test
  void testSyncTable_CreateOrReplaceTableDueToLocationMismatch() {
    setupCommonMocks();
    mockHadoopTables();
    mockStaticGlueSchemaExtractor();

    String dbName = TEST_TABLE_IDENTIFIER.getDatabaseName();
    String tableName = TEST_TABLE_IDENTIFIER.getTableName();
    Database db = Database.builder().name(dbName).build();
    Table table = getGlueTable(dbName, tableName, "");

    // mock databases exists
    when(mockGlueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().database(db).build());
    // mock table exists
    when(mockGlueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(table).build());
    when(mockGlueClient.createTable(any(CreateTableRequest.class))).thenReturn(null);
    when(mockGlueClient.deleteTable(any(DeleteTableRequest.class))).thenReturn(null);

    mockSyncClient.syncTable(TEST_ONETABLE);

    verifyGlueDbApiCalls(1, 0);
    verifyGlueTableApiCalls(1, 2, 0, 2);
  }

  @Test
  void testSyncTable_RefreshTable() {
    setupCommonMocks();
    mockHadoopTables();
    mockStaticGlueSchemaExtractor();

    String dbName = TEST_TABLE_IDENTIFIER.getDatabaseName();
    String tableName = TEST_TABLE_IDENTIFIER.getTableName();
    Database db = Database.builder().name(dbName).build();
    Table table = getGlueTable(dbName, tableName, TEST_ONETABLE.getBasePath());

    // mock databases exists
    when(mockGlueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().database(db).build());
    // mock table exists
    when(mockGlueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(table).build());
    when(mockGlueClient.updateTable(any(UpdateTableRequest.class))).thenReturn(null);

    mockSyncClient.syncTable(TEST_ONETABLE);

    verifyGlueDbApiCalls(1, 0);
    verifyGlueTableApiCalls(1, 0, 1, 0);
  }

  @Test
  void testSyncTable_FailureWhenUpdatingTable() {
    setupCommonMocks();
    mockHadoopTables();
    mockStaticGlueSchemaExtractor();

    String dbName = TEST_TABLE_IDENTIFIER.getDatabaseName();
    String tableName = TEST_TABLE_IDENTIFIER.getTableName();
    Database db = Database.builder().name(dbName).build();
    Table table = getGlueTable(dbName, tableName, TEST_ONETABLE.getBasePath());

    // mock databases exists
    when(mockGlueClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().database(db).build());
    // mock table exists
    when(mockGlueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(table).build());
    when(mockGlueClient.updateTable(any(UpdateTableRequest.class)))
        .thenThrow(new RuntimeException("error updating table"));

    assertThrows(CatalogSyncException.class, () -> mockSyncClient.syncTable(TEST_ONETABLE));

    verifyGlueDbApiCalls(1, 0);
    verifyGlueTableApiCalls(1, 0, 1, 0);
  }

  private void verifyGlueDbApiCalls(int getDbCount, int createDbCount) {
    verify(mockGlueClient, times(getDbCount)).getDatabase(any(GetDatabaseRequest.class));
    verify(mockGlueClient, times(createDbCount)).createDatabase(any(CreateDatabaseRequest.class));
  }

  private void verifyGlueTableApiCalls(
      int getTableCount, int createTableCount, int updateTableCount, int deleteTableCount) {
    verify(mockGlueClient, times(getTableCount)).getTable(any(GetTableRequest.class));
    verify(mockGlueClient, times(createTableCount)).createTable(any(CreateTableRequest.class));
    verify(mockGlueClient, times(updateTableCount)).updateTable(any(UpdateTableRequest.class));
    verify(mockGlueClient, times(deleteTableCount)).deleteTable(any(DeleteTableRequest.class));
  }
}
