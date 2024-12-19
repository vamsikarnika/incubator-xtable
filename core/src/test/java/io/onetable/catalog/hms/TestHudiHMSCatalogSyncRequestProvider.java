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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import lombok.SneakyThrows;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import io.onetable.hudi.HudiTableManager;

@ExtendWith(MockitoExtension.class)
public class TestHudiHMSCatalogSyncRequestProvider extends HMSCatalogSyncRequestProviderTestBase {

  @Mock private HoodieTableMetaClient mockMetaClient;
  @Mock private HudiTableManager mockHudiTableManager;
  @Mock private PartitionValueExtractor mockPartitionValueExtractor;
  @Mock private HoodieTableConfig mockTableConfig;

  private HudiHMSCatalogSyncRequestProvider mockHudiHMSCatalogSyncRequestProvider;

  private HudiHMSCatalogSyncRequestProvider createMockHudiHMSCatalogSyncRequestProvider() {
    return new HudiHMSCatalogSyncRequestProvider(
        mockCatalogConfig,
        mockMetaStoreClient,
        HMSSchemaExtractor.getInstance(),
        mockHudiTableManager,
        mockConfiguration,
        mockMetaClient,
        mockPartitionValueExtractor);
  }

  void setupCommonMocks() {
    mockHudiHMSCatalogSyncRequestProvider = createMockHudiHMSCatalogSyncRequestProvider();
    when(mockCatalogConfig.getSchemaLengthThreshold()).thenReturn(1000);
  }

  void setupMetaClientMocks() {
    when(mockTableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    // when(mockMetaClient.getBasePathV2()).thenReturn(new Path(ONETABLE_BASE_PATH));
  }

  @Test
  void testGetCreateTableInput() {
    setupCommonMocks();
    setupMetaClientMocks();

    Table table =
        mockHudiHMSCatalogSyncRequestProvider.getCreateTableInput(
            TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
    assertEquals(TEST_TABLE_IDENTIFIER.getTableName(), table.getTableName());
    assertEquals(TEST_TABLE_IDENTIFIER.getDatabaseName(), table.getDbName());
    assertEquals(2, table.getSd().getCols().size());
    assertEquals(1, table.getPartitionKeys().size());
    assertNotNull(table.getParameters());
    assertFalse(table.getParameters().isEmpty());
  }

  @Test
  void testGetUpdateTableInput() {
    setupCommonMocks();
    setupMetaClientMocks();

    Table table =
        mockHudiHMSCatalogSyncRequestProvider.getCreateTableInput(
            TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
    Table updatedTable =
        mockHudiHMSCatalogSyncRequestProvider.getUpdateTableInput(
            TEST_ONETABLE_WITH_EVOLVED_SCHEMA, table, TEST_TABLE_IDENTIFIER);
    assertEquals(TEST_TABLE_IDENTIFIER.getTableName(), updatedTable.getTableName());
    assertEquals(TEST_TABLE_IDENTIFIER.getDatabaseName(), updatedTable.getDbName());
    assertEquals(3, updatedTable.getSd().getCols().size());
    assertEquals(1, updatedTable.getPartitionKeys().size());
    assertNotNull(updatedTable.getParameters());
    assertFalse(table.getParameters().isEmpty());
  }

// TODO - move these tests to TestHudiHMSCatalogPartitionSyncOperations
//
//  @SneakyThrows
//  @Test
//  void testSyncAllPartitions() {
//    setupCommonMocks();
//    setupMetaClientMocks();
//
//    String partitionKey1 = "key1";
//    ZonedDateTime zonedDateTime =
//        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
//    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class);
//        MockedStatic<FSUtils> mockFSUtils = mockStatic(FSUtils.class)) {
//      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);
//      List<String> mockedPartitions = Collections.singletonList(partitionKey1);
//      mockFSUtils
//          .when(
//              () ->
//                  FSUtils.getAllPartitionPaths(any(), eq(ONETABLE_BASE_PATH), eq(true), eq(false)))
//          .thenReturn(mockedPartitions);
//      mockFSUtils
//          .when(() -> FSUtils.getPartitionPath(new Path(ONETABLE_BASE_PATH), partitionKey1))
//          .thenReturn(new Path(ONETABLE_BASE_PATH + "/" + partitionKey1));
//      when(mockMetaClient.getBasePathV2()).thenReturn(new Path(ONETABLE_BASE_PATH));
//      when(mockPartitionValueExtractor.extractPartitionValuesInPath(partitionKey1))
//          .thenReturn(Collections.singletonList(partitionKey1));
//
//      HoodieActiveTimeline mockTimeline = mock(HoodieActiveTimeline.class);
//      HoodieInstant instant1 =
//          new HoodieInstant(HoodieInstant.State.COMPLETED, "replacecommit", "100", "1000");
//      HoodieInstant instant2 =
//          new HoodieInstant(HoodieInstant.State.COMPLETED, "replacecommit", "101", "1100");
//      when(mockTimeline.countInstants()).thenReturn(2);
//      when(mockTimeline.lastInstant()).thenReturn(Option.of(instant2));
//      when(mockTimeline.getInstantsOrderedByStateTransitionTime())
//          .thenReturn(Stream.of(instant1, instant2));
//      when(mockMetaClient.getActiveTimeline()).thenReturn(mockTimeline);
//
//      when(mockMetaStoreClient.listPartitions(
//              TEST_TABLE_IDENTIFIER.getDatabaseName(),
//              TEST_TABLE_IDENTIFIER.getTableName(),
//              (short) -1))
//          .thenReturn(Collections.emptyList());
//      Table hmsTable =
//          mockHudiHMSCatalogSyncRequestProvider.getCreateTableInput(
//              TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
//      when(mockMetaStoreClient.getTable(
//              TEST_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_IDENTIFIER.getTableName()))
//          .thenReturn(hmsTable.deepCopy());
//      mockHudiHMSCatalogSyncRequestProvider.syncPartitions(
//          TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
//
//      ArgumentCaptor<List<Partition>> addPartitionsCaptor = ArgumentCaptor.forClass(List.class);
//      verify(mockMetaStoreClient, times(1))
//          .add_partitions(addPartitionsCaptor.capture(), anyBoolean(), anyBoolean());
//      List<List<Partition>> addedPartitions = addPartitionsCaptor.getAllValues();
//      assertEquals(addedPartitions.size(), 1);
//      assertEquals(addedPartitions.get(0).size(), 1);
//      assertEquals(
//          addedPartitions.get(0).get(0).getValues(), Collections.singletonList(partitionKey1));
//      verify(mockMetaStoreClient, times(0))
//          .dropPartition(
//              eq(TEST_TABLE_IDENTIFIER.getDatabaseName()),
//              eq(TEST_TABLE_IDENTIFIER.getTableName()),
//              any(List.class),
//              anyBoolean());
//
//      ArgumentCaptor<String> databaseNameCaptor = ArgumentCaptor.forClass(String.class);
//      ArgumentCaptor<String> tableNameCaptor = ArgumentCaptor.forClass(String.class);
//      ArgumentCaptor<Table> alterTableCaptor = ArgumentCaptor.forClass(Table.class);
//      verify(mockMetaStoreClient, times(1))
//          .alter_table(
//              databaseNameCaptor.capture(), tableNameCaptor.capture(), alterTableCaptor.capture());
//      assertEquals(TEST_TABLE_IDENTIFIER.getDatabaseName(), databaseNameCaptor.getValue());
//      assertEquals(TEST_TABLE_IDENTIFIER.getTableName(), tableNameCaptor.getValue());
//      Table alterTable = alterTableCaptor.getValue();
//      assertEquals("101", alterTable.getParameters().get(LAST_COMMIT_TIME_SYNC));
//      assertEquals("1100", alterTable.getParameters().get(LAST_COMMIT_COMPLETION_TIME_SYNC));
//    }
//  }
//
//  @SneakyThrows
//  @Test
//  void testSyncPartitionsDiff() {
//    setupCommonMocks();
//    setupMetaClientMocks();
//
//    String partitionKey1 = "key1";
//    String partitionKey2 = "key2";
//    ZonedDateTime zonedDateTime =
//        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
//    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class);
//        MockedStatic<FSUtils> mockFSUtils = mockStatic(FSUtils.class)) {
//      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);
//      List<String> mockedPartitions = Arrays.asList(partitionKey1, partitionKey2);
//      mockFSUtils
//          .when(
//              () ->
//                  FSUtils.getAllPartitionPaths(any(), eq(ONETABLE_BASE_PATH), eq(true), eq(false)))
//          .thenReturn(mockedPartitions);
//      mockFSUtils
//          .when(() -> FSUtils.getPartitionPath(new Path(ONETABLE_BASE_PATH), partitionKey1))
//          .thenReturn(new Path(ONETABLE_BASE_PATH + "/" + partitionKey1));
//      mockFSUtils
//          .when(() -> FSUtils.getPartitionPath(new Path(ONETABLE_BASE_PATH), partitionKey2))
//          .thenReturn(new Path(ONETABLE_BASE_PATH + "/" + partitionKey2));
//      when(mockMetaClient.getBasePathV2()).thenReturn(new Path(ONETABLE_BASE_PATH));
//      when(mockPartitionValueExtractor.extractPartitionValuesInPath(partitionKey1))
//          .thenReturn(Collections.singletonList(partitionKey1));
//      when(mockPartitionValueExtractor.extractPartitionValuesInPath(partitionKey2))
//          .thenReturn(Collections.singletonList(partitionKey2));
//
//      HoodieActiveTimeline mockTimeline = mock(HoodieActiveTimeline.class);
//      HoodieInstant instant1 =
//          new HoodieInstant(HoodieInstant.State.COMPLETED, "replacecommit", "100", "1000");
//      HoodieInstant instant2 =
//          new HoodieInstant(HoodieInstant.State.COMPLETED, "replacecommit", "101", "1100");
//      when(mockTimeline.countInstants()).thenReturn(2);
//      when(mockTimeline.lastInstant()).thenReturn(Option.of(instant2));
//      when(mockTimeline.getInstantsOrderedByStateTransitionTime())
//          .thenReturn(Stream.of(instant1, instant2));
//      when(mockMetaClient.getActiveTimeline()).thenReturn(mockTimeline);
//
//      Partition partition = new Partition();
//      partition.setValues(Collections.singletonList(partitionKey1));
//      StorageDescriptor sd = new StorageDescriptor();
//      sd.setLocation(ONETABLE_BASE_PATH + "/" + partitionKey1);
//      partition.setSd(sd);
//      when(mockMetaStoreClient.listPartitions(
//              TEST_TABLE_IDENTIFIER.getDatabaseName(),
//              TEST_TABLE_IDENTIFIER.getTableName(),
//              (short) -1))
//          .thenReturn(Collections.singletonList(partition));
//      Table hmsTable =
//          mockHudiHMSCatalogSyncRequestProvider.getCreateTableInput(
//              TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
//      when(mockMetaStoreClient.getTable(
//              TEST_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_IDENTIFIER.getTableName()))
//          .thenReturn(hmsTable.deepCopy());
//      mockHudiHMSCatalogSyncRequestProvider.syncPartitions(
//          TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
//
//      ArgumentCaptor<List<Partition>> addPartitionsCaptor = ArgumentCaptor.forClass(List.class);
//      verify(mockMetaStoreClient, times(1))
//          .add_partitions(addPartitionsCaptor.capture(), anyBoolean(), anyBoolean());
//      List<List<Partition>> addedPartitions = addPartitionsCaptor.getAllValues();
//      assertEquals(addedPartitions.size(), 1);
//      assertEquals(addedPartitions.get(0).size(), 1);
//      assertEquals(
//          addedPartitions.get(0).get(0).getValues(), Collections.singletonList(partitionKey2));
//      verify(mockMetaStoreClient, times(0))
//          .dropPartition(
//              eq(TEST_TABLE_IDENTIFIER.getDatabaseName()),
//              eq(TEST_TABLE_IDENTIFIER.getTableName()),
//              any(List.class),
//              anyBoolean());
//
//      ArgumentCaptor<String> databaseNameCaptor = ArgumentCaptor.forClass(String.class);
//      ArgumentCaptor<String> tableNameCaptor = ArgumentCaptor.forClass(String.class);
//      ArgumentCaptor<Table> alterTableCaptor = ArgumentCaptor.forClass(Table.class);
//      verify(mockMetaStoreClient, times(1))
//          .alter_table(
//              databaseNameCaptor.capture(), tableNameCaptor.capture(), alterTableCaptor.capture());
//      assertEquals(TEST_TABLE_IDENTIFIER.getDatabaseName(), databaseNameCaptor.getValue());
//      assertEquals(TEST_TABLE_IDENTIFIER.getTableName(), tableNameCaptor.getValue());
//      Table alterTable = alterTableCaptor.getValue();
//      assertEquals("101", alterTable.getParameters().get(LAST_COMMIT_TIME_SYNC));
//      assertEquals("1100", alterTable.getParameters().get(LAST_COMMIT_COMPLETION_TIME_SYNC));
//    }
//  }
}
