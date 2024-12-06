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

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
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

import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.model.storage.TableFormat;

@ExtendWith(MockitoExtension.class)
public class TestIcebergHMSCatalogSyncOperations extends HMSCatalogSyncOperationsTestBase {

  @Mock private HadoopTables mockHadoopTables;
  @Mock private BaseTable mockBaseTable;
  @Mock private TableOperations mockTableOperations;
  @Mock private TableMetadata mockTableMetadata;
  private IcebergHMSCatalogSyncOperations mockIcebergHmsCatalogSyncOperations;

  private static final String ICEBERG_METADATA_FILE_LOCATION = "onetable-base-path/metadata";
  private static final String ICEBERG_METADATA_FILE_LOCATION_V2 = "onetable-base-path/v2-metadata";
  private static final String ONETABLE_LAST_INSTANT_SYNCED_PROP = "ONETABLE_LAST_INSTANT_SYNCED";
  private static final String SCHEMA_NAME_MAPPING_PROP = "schema.name-mapping.default";

  private IcebergHMSCatalogSyncOperations createIcebergHMSCatalogSyncOperations() {
    return new IcebergHMSCatalogSyncOperations(
        TEST_TABLE_IDENTIFIER,
        mockCatalogConfig,
        mockConfiguration,
        mockMetaStoreClient,
        mockHadoopTables,
        mockHmsSchemaExtractor);
  }

  void setupCommonMocks() {
    mockIcebergHmsCatalogSyncOperations = createIcebergHMSCatalogSyncOperations();
  }

  void mockHadoopTables() {
    when(mockHadoopTables.load(ONETABLE_BASE_PATH)).thenReturn(mockBaseTable);
    mockMetadataFileLocation();
  }

  void mockMetadataFileLocation() {
    when(mockBaseTable.operations()).thenReturn(mockTableOperations);
    when(mockTableOperations.current()).thenReturn(mockTableMetadata);
    when(mockTableMetadata.metadataFileLocation()).thenReturn(ICEBERG_METADATA_FILE_LOCATION);
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetDatabase(boolean isDbPresent) {
    setupCommonMocks();
    Database db = new Database(HMS_DATABASE, "hms db", null, Collections.emptyMap());
    if (isDbPresent) {
      when(mockMetaStoreClient.getDatabase(HMS_DATABASE)).thenReturn(db);
    } else {
      when(mockMetaStoreClient.getDatabase(HMS_DATABASE))
          .thenThrow(new NoSuchObjectException("db not found"));
    }
    Database hmsDb = mockIcebergHmsCatalogSyncOperations.getDatabase(HMS_DATABASE);
    if (isDbPresent) {
      assertEquals(db, hmsDb);
    } else {
      assertNull(hmsDb);
    }
  }

  @SneakyThrows
  @Test
  void testGetDatabaseFailure() {
    setupCommonMocks();
    when(mockMetaStoreClient.getDatabase(HMS_DATABASE))
        .thenThrow(new RuntimeException("something went wrong"));
    assertThrows(
        RuntimeException.class,
        () -> mockIcebergHmsCatalogSyncOperations.getDatabase(HMS_DATABASE));
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetTable(boolean isTablePresent) {
    setupCommonMocks();
    Table table = newHmsTable(HMS_DATABASE, HMS_TABLE);
    if (isTablePresent) {
      when(mockMetaStoreClient.getTable(HMS_DATABASE, HMS_TABLE)).thenReturn(table);
    } else {
      when(mockMetaStoreClient.getTable(HMS_DATABASE, HMS_TABLE))
          .thenThrow(new NoSuchObjectException("db not found"));
    }
    Table hmsTable = mockIcebergHmsCatalogSyncOperations.getTable(TEST_TABLE_IDENTIFIER);
    if (isTablePresent) {
      assertEquals(table, hmsTable);
    } else {
      assertNull(hmsTable);
    }
  }

  @SneakyThrows
  @Test
  void testGetTableFailure() {
    setupCommonMocks();
    when(mockMetaStoreClient.getTable(HMS_DATABASE, HMS_TABLE))
        .thenThrow(new RuntimeException("something went wrong"));
    assertThrows(
        RuntimeException.class,
        () -> mockIcebergHmsCatalogSyncOperations.getTable(TEST_TABLE_IDENTIFIER));
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateDatabase(boolean shouldFail) {
    setupCommonMocks();
    Database database = newDatabase(HMS_DATABASE);
    if (shouldFail) {
      doThrow(new RuntimeException("something went wrong"))
          .when(mockMetaStoreClient)
          .createDatabase(database);
      assertThrows(
          RuntimeException.class,
          () -> mockIcebergHmsCatalogSyncOperations.createDatabase(HMS_DATABASE));
    } else {
      mockIcebergHmsCatalogSyncOperations.createDatabase(HMS_DATABASE);
      verify(mockMetaStoreClient, times(1)).createDatabase(database);
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDropTable(boolean shouldFail) {
    setupCommonMocks();
    if (shouldFail) {
      doThrow(new RuntimeException("something went wrong"))
          .when(mockMetaStoreClient)
          .dropTable(HMS_DATABASE, HMS_TABLE);
      assertThrows(
          RuntimeException.class,
          () ->
              mockIcebergHmsCatalogSyncOperations.dropTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER));
    } else {
      mockIcebergHmsCatalogSyncOperations.dropTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
      verify(mockMetaStoreClient, times(1)).dropTable(HMS_DATABASE, HMS_TABLE);
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateTable(boolean shouldFail) {
    setupCommonMocks();
    mockHadoopTables();
    when(mockHmsSchemaExtractor.toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());

    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);
      Table hmsTable =
          mockIcebergHmsCatalogSyncOperations.newHmsTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
      if (shouldFail) {
        doThrow(new RuntimeException("something went wrong"))
            .when(mockMetaStoreClient)
            .createTable(hmsTable);
        assertThrows(
            RuntimeException.class,
            () ->
                mockIcebergHmsCatalogSyncOperations.createTable(
                    TEST_ONETABLE, TEST_TABLE_IDENTIFIER));
      } else {
        mockIcebergHmsCatalogSyncOperations.createTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
        verify(mockMetaStoreClient, times(1)).createTable(hmsTable);
      }
      verify(mockHmsSchemaExtractor, times(2))
          .toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema());
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRefreshTable(boolean shouldFail) {
    setupCommonMocks();
    mockHadoopTables();
    when(mockHmsSchemaExtractor.toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());

    Map<String, String> tableParams = new HashMap<>();
    tableParams.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    tableParams.put(ONETABLE_LAST_INSTANT_SYNCED_PROP, "ts1");
    tableParams.put(SCHEMA_NAME_MAPPING_PROP, "v1-schema");
    Table hmsTable = newHmsTable(HMS_DATABASE, HMS_TABLE, tableParams);
    hmsTable.setSd(getTestStorageDescriptor());

    Map<String, String> newTableParams = new HashMap<>();
    newTableParams.put(ONETABLE_LAST_INSTANT_SYNCED_PROP, "ts2");
    newTableParams.put(SCHEMA_NAME_MAPPING_PROP, "v2-schema");

    when(mockTableMetadata.metadataFileLocation()).thenReturn(ICEBERG_METADATA_FILE_LOCATION_V2);
    when(mockBaseTable.properties()).thenReturn(newTableParams);

    if (shouldFail) {
      doThrow(new RuntimeException("something went wrong"))
          .when(mockMetaStoreClient)
          .alter_table(HMS_DATABASE, HMS_TABLE, hmsTable);
      assertThrows(
          RuntimeException.class,
          () ->
              mockIcebergHmsCatalogSyncOperations.refreshTable(
                  TEST_ONETABLE, hmsTable, TEST_TABLE_IDENTIFIER));
    } else {
      mockIcebergHmsCatalogSyncOperations.refreshTable(
          TEST_ONETABLE, hmsTable, TEST_TABLE_IDENTIFIER);
      verify(mockMetaStoreClient, times(1)).alter_table(HMS_DATABASE, HMS_TABLE, hmsTable);
      // verify table contains latest iceberg metadata params during alter_table operation
      assertEquals(hmsTable.getParameters().get(ONETABLE_LAST_INSTANT_SYNCED_PROP), "ts2");
      assertEquals(hmsTable.getParameters().get(SCHEMA_NAME_MAPPING_PROP), "v2-schema");
      assertEquals(
          hmsTable.getParameters().get(METADATA_LOCATION_PROP), ICEBERG_METADATA_FILE_LOCATION_V2);
    }
    verify(mockHmsSchemaExtractor, times(1))
        .toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema());
  }

  @SneakyThrows
  @Test
  void testCreateOrReplaceTable() {
    setupCommonMocks();
    mockHadoopTables();
    when(mockHmsSchemaExtractor.toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());

    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);

      final ExternalCatalogConfig.TableIdentifier tempTableIdentifier =
          ExternalCatalogConfig.TableIdentifier.builder()
              .databaseName(HMS_DATABASE)
              .tableName(HMS_TABLE + "_temp" + ZonedDateTime.now().toEpochSecond())
              .build();

      String dbName = HMS_DATABASE;
      Table hmsTable =
          mockIcebergHmsCatalogSyncOperations.newHmsTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
      Table tmpHmsTable =
          mockIcebergHmsCatalogSyncOperations.newHmsTable(TEST_ONETABLE, tempTableIdentifier);

      mockIcebergHmsCatalogSyncOperations.createOrReplaceTable(
          TEST_ONETABLE, TEST_TABLE_IDENTIFIER);

      verify(mockMetaStoreClient, times(1)).createTable(hmsTable);
      verify(mockMetaStoreClient, times(1)).dropTable(dbName, TEST_TABLE_IDENTIFIER.getTableName());
      verify(mockMetaStoreClient, times(1)).createTable(tmpHmsTable);
      verify(mockMetaStoreClient, times(1)).dropTable(dbName, tempTableIdentifier.getTableName());
      verify(mockHmsSchemaExtractor, times(4))
          .toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema());
    }
  }

  @SneakyThrows
  @Test
  void testNewHmsTable() {
    setupCommonMocks();
    mockHadoopTables();
    when(mockHmsSchemaExtractor.toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);
      Table expected = new Table();
      expected.setDbName(HMS_DATABASE);
      expected.setTableName(HMS_TABLE);
      expected.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      expected.setCreateTime((int) zonedDateTime.toEpochSecond());
      expected.setSd(getTestStorageDescriptor());
      expected.setTableType("EXTERNAL_TABLE");
      expected.setParameters(getTestParameters());

      assertEquals(
          expected,
          mockIcebergHmsCatalogSyncOperations.newHmsTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER));
      verify(mockHmsSchemaExtractor, times(1))
          .toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema());
      verify(mockBaseTable, times(1)).properties();
      verify(mockHadoopTables, times(1)).load(ONETABLE_BASE_PATH);
    }
  }

  @Test
  void testGetStorageDescriptor() {
    setupCommonMocks();
    when(mockHmsSchemaExtractor.toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());
    StorageDescriptor expected = getTestStorageDescriptor();
    assertEquals(expected, mockIcebergHmsCatalogSyncOperations.getStorageDescriptor(TEST_ONETABLE));
    verify(mockHmsSchemaExtractor, times(1))
        .toColumns(TableFormat.ICEBERG, TEST_ONETABLE.getReadSchema());
  }

  @Test
  void testGetHmsTableParameters() {
    setupCommonMocks();
    mockMetadataFileLocation();
    when(mockBaseTable.properties()).thenReturn(Collections.emptyMap());
    Map<String, String> expected = getTestParameters();
    assertEquals(
        expected, mockIcebergHmsCatalogSyncOperations.getHmsTableParameters(mockBaseTable));
    verify(mockBaseTable, times(1)).properties();
    verify(mockHadoopTables, never()).load(any());
  }

  private StorageDescriptor getTestStorageDescriptor() {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    SerDeInfo serDeInfo = new SerDeInfo();
    storageDescriptor.setCols(Collections.emptyList());
    storageDescriptor.setLocation(ONETABLE_BASE_PATH);
    storageDescriptor.setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
    storageDescriptor.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    serDeInfo.setSerializationLib("org.apache.iceberg.mr.hive.HiveIcebergSerDe");
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private Map<String, String> getTestParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("EXTERNAL", "TRUE");
    parameters.put("table_type", "ICEBERG");
    parameters.put("metadata_location", ICEBERG_METADATA_FILE_LOCATION);
    parameters.put("storage_handler", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
    parameters.put("iceberg.catalog", "location_based_table");
    return parameters;
  }
}
