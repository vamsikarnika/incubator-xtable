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

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

import io.onetable.model.storage.TableFormat;

@ExtendWith(MockitoExtension.class)
public class TestIcebergGlueCatalogSyncOperations extends GlueCatalogSyncOperationsTestBase {

  @Mock private HadoopTables mockHadoopTables;
  @Mock private BaseTable mockBaseTable;
  @Mock private TableOperations mockTableOperations;
  @Mock private TableMetadata mockTableMetadata;
  private IcebergGlueCatalogSyncOperations mockIcebergGlueCatalogOperations;

  private static final String ICEBERG_METADATA_FILE_LOCATION = "onetable-base-path/metadata";

  private IcebergGlueCatalogSyncOperations createIcebergGlueCatalogSyncOperations() {
    return new IcebergGlueCatalogSyncOperations(
        TEST_TABLE_IDENTIFIER,
        mockGlueClient,
        mockGlueCatalogConfig,
        mockConfiguration,
        mockHadoopTables,
        mockGlueSchemaExtractor);
  }

  void setupCommonMocks() {
    mockIcebergGlueCatalogOperations = createIcebergGlueCatalogSyncOperations();
    when(mockGlueCatalogConfig.getCatalogId()).thenReturn(GLUE_CATALOG_ID);
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
    Database db =
        mockIcebergGlueCatalogOperations.getDatabase(TEST_TABLE_IDENTIFIER.getDatabaseName());
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
        () ->
            mockIcebergGlueCatalogOperations.getDatabase(TEST_TABLE_IDENTIFIER.getDatabaseName()));
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
    Table table = mockIcebergGlueCatalogOperations.getTable(TEST_TABLE_IDENTIFIER);
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
    assertThrows(
        RuntimeException.class,
        () -> mockIcebergGlueCatalogOperations.getTable(TEST_TABLE_IDENTIFIER));
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
          () ->
              mockIcebergGlueCatalogOperations.createDatabase(
                  TEST_TABLE_IDENTIFIER.getDatabaseName()));
    } else {
      when(mockGlueClient.createDatabase(dbRequest))
          .thenReturn(CreateDatabaseResponse.builder().build());
      mockIcebergGlueCatalogOperations.createDatabase(TEST_TABLE_IDENTIFIER.getDatabaseName());
    }
    verify(mockGlueClient, times(1)).createDatabase(dbRequest);
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
          () -> mockIcebergGlueCatalogOperations.dropTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER));
    } else {
      when(mockGlueClient.deleteTable(deleteRequest))
          .thenReturn(DeleteTableResponse.builder().build());
      mockIcebergGlueCatalogOperations.dropTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).deleteTable(deleteRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateTable(boolean shouldFail) {
    setupCommonMocks();
    mockHadoopTables();
    when(mockGlueSchemaExtractor.toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());
    CreateTableRequest createTableRequest =
        createTableRequest(
            TEST_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_TABLE_IDENTIFIER.getTableName(),
            mockIcebergGlueCatalogOperations.getTableParameters(mockBaseTable));
    if (shouldFail) {
      when(mockGlueClient.createTable(createTableRequest))
          .thenThrow(new RuntimeException("something went wrong"));
      assertThrows(
          RuntimeException.class,
          () -> mockIcebergGlueCatalogOperations.createTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER));
    } else {
      when(mockGlueClient.createTable(createTableRequest))
          .thenReturn(CreateTableResponse.builder().build());
      mockIcebergGlueCatalogOperations.createTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).createTable(createTableRequest);
    verify(mockGlueSchemaExtractor, times(1))
        .toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRefreshTable(boolean shouldFail) {
    setupCommonMocks();
    mockHadoopTables();
    Map<String, String> glueTableParams = new HashMap<>();
    glueTableParams.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    Table glueTable = Table.builder().parameters(glueTableParams).build();
    Map<String, String> parameters = new HashMap<>();
    parameters.put(PREVIOUS_METADATA_LOCATION_PROP, glueTableParams.get(METADATA_LOCATION_PROP));
    parameters.putAll(mockIcebergGlueCatalogOperations.getTableParameters(mockBaseTable));
    when(mockGlueSchemaExtractor.toColumns(
            TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema(), glueTable))
        .thenReturn(Collections.emptyList());
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
          () ->
              mockIcebergGlueCatalogOperations.refreshTable(
                  TEST_ONETABLE, glueTable, TEST_TABLE_IDENTIFIER));
    } else {
      when(mockGlueClient.updateTable(tableReq)).thenReturn(UpdateTableResponse.builder().build());
      mockIcebergGlueCatalogOperations.refreshTable(
          TEST_ONETABLE, glueTable, TEST_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).updateTable(tableReq);
    verify(mockGlueSchemaExtractor, times(1))
        .toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema(), glueTable);
  }

  @Test
  void testCreateOrReplaceTable() {
    setupCommonMocks();
    mockHadoopTables();
    when(mockGlueSchemaExtractor.toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());
    ZonedDateTime fixedDateTime = ZonedDateTime.parse("2024-10-25T10:15:30.00Z");
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(fixedDateTime);
      CreateTableRequest mainCreateTableRequest =
          createTableRequest(
              TEST_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_TABLE_IDENTIFIER.getTableName(),
              mockIcebergGlueCatalogOperations.getTableParameters(mockBaseTable));
      DeleteTableRequest mainTableDeleteRequest =
          deleteTableRequest(
              TEST_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_IDENTIFIER.getTableName());
      CreateTableRequest tempCreateTableRequest =
          createTableRequest(
              TEST_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_TABLE_IDENTIFIER.getTableName() + "_temp" + ZonedDateTime.now().toEpochSecond(),
              mockIcebergGlueCatalogOperations.getTableParameters(mockBaseTable));
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

      mockIcebergGlueCatalogOperations.createOrReplaceTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);

      verify(mockGlueClient, times(1)).createTable(tempCreateTableRequest);
      verify(mockGlueClient, times(1)).deleteTable(tempTableDeleteRequest);
      verify(mockGlueClient, times(1)).createTable(mainCreateTableRequest);
      verify(mockGlueClient, times(1)).deleteTable(mainTableDeleteRequest);
      verify(mockGlueSchemaExtractor, times(2))
          .toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema());
    }
  }

  @Test
  void testGetTableParameters() {
    Map<String, String> expected = new HashMap<>();
    expected.put(TABLE_TYPE_PROP, TableFormat.ICEBERG);
    expected.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    mockHadoopTables(false);
    mockIcebergGlueCatalogOperations = createIcebergGlueCatalogSyncOperations();
    Map<String, String> tableParameters =
        mockIcebergGlueCatalogOperations.getTableParameters(mockBaseTable);
    assertEquals(expected, tableParameters);
  }
}
