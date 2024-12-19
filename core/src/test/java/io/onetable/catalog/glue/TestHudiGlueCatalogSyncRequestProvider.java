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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import io.onetable.hudi.HudiTableManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHudiGlueCatalogSyncRequestProvider extends GlueCatalogSyncRequestProviderTestBase {

  @Mock private HoodieTableMetaClient mockMetaClient;
  @Mock private HudiTableManager mockHudiTableManager;
  @Mock private PartitionValueExtractor mockPartitionValueExtractor;
  @Mock private HoodieTableConfig mockTableConfig;

  private HudiGlueCatalogSyncRequestProvider mockHudiGlueCatalogSyncRequestProvider;

  private HudiGlueCatalogSyncRequestProvider createMockHudiGlueCatalogSyncRequestProvider() {
    return new HudiGlueCatalogSyncRequestProvider(
        mockCatalogConfig,
        mockGlueClient,
        GlueSchemaExtractor.getInstance(),
        mockHudiTableManager,
        mockConfiguration,
        mockMetaClient,
        mockPartitionValueExtractor);
  }

  void setupCommonMocks() {
    mockHudiGlueCatalogSyncRequestProvider = createMockHudiGlueCatalogSyncRequestProvider();
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

    TableInput table =
        mockHudiGlueCatalogSyncRequestProvider.getCreateTableInput(
            TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
    assertEquals(TEST_TABLE_IDENTIFIER.getTableName(), table.name());
    assertEquals(2, table.storageDescriptor().columns().size());
    assertEquals(1, table.partitionKeys().size());
    assertNotNull(table.parameters());
    assertFalse(table.parameters().isEmpty());
  }

  @Test
  void testGetUpdateTableInput() {
    setupCommonMocks();
    setupMetaClientMocks();

    TableInput tableInput =
        mockHudiGlueCatalogSyncRequestProvider.getCreateTableInput(
            TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
    Table table =
        Table.builder()
            .name(tableInput.name())
            .parameters(tableInput.parameters())
            .storageDescriptor(tableInput.storageDescriptor())
            .partitionKeys(tableInput.partitionKeys())
            .build();
    TableInput updatedTable =
        mockHudiGlueCatalogSyncRequestProvider.getUpdateTableInput(
            TEST_ONETABLE_WITH_EVOLVED_SCHEMA, table, TEST_TABLE_IDENTIFIER);
    assertEquals(TEST_TABLE_IDENTIFIER.getTableName(), updatedTable.name());
    assertEquals(3, updatedTable.storageDescriptor().columns().size());
    assertEquals(1, updatedTable.partitionKeys().size());
    assertNotNull(updatedTable.parameters());
    assertFalse(table.parameters().isEmpty());
  }

//  TODO - Move these tests to TestHudiGlueCatalogPartitionSyncOperations
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
//      HoodieInstant instant2 =
//          new HoodieInstant(HoodieInstant.State.COMPLETED, "replacecommit", "101", "1100");
//      when(mockTimeline.lastInstant()).thenReturn(Option.of(instant2));
//      when(mockMetaClient.getActiveTimeline()).thenReturn(mockTimeline);
//
//      GetPartitionsResponse response =
//          GetPartitionsResponse.builder().partitions(Collections.emptyList()).build();
//
//      when(mockGlueClient.getPartitions(any(GetPartitionsRequest.class))).thenReturn(response);
//      TableInput glueTableInput =
//          mockHudiGlueCatalogSyncRequestProvider.getCreateTableInput(
//              TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
//      Table glueTable =
//          Table.builder()
//              .name(glueTableInput.name())
//              .parameters(glueTableInput.parameters())
//              .storageDescriptor(glueTableInput.storageDescriptor())
//              .partitionKeys(glueTableInput.partitionKeys())
//              .build();
//      GetTableResponse tableResponse = GetTableResponse.builder().table(glueTable).build();
//      when(mockGlueClient.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);
//      when(mockGlueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
//          .thenReturn(BatchCreatePartitionResponse.builder().build());
//      mockHudiGlueCatalogSyncRequestProvider.syncPartitions(
//          TEST_ONETABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER);
//
//      verify(mockGlueClient, times(1)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
//
//      verify(mockGlueClient, times(0)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
//
//      ArgumentCaptor<UpdateTableRequest> updateTableRequestArgumentCaptor =
//          ArgumentCaptor.forClass(UpdateTableRequest.class);
//      verify(mockGlueClient, times(1)).updateTable(updateTableRequestArgumentCaptor.capture());
//      assertNotNull(updateTableRequestArgumentCaptor.getValue());
//      assertEquals(
//          TEST_TABLE_IDENTIFIER.getDatabaseName(),
//          updateTableRequestArgumentCaptor.getValue().databaseName());
//      assertEquals(
//          TEST_TABLE_IDENTIFIER.getTableName(),
//          updateTableRequestArgumentCaptor.getValue().tableInput().name());
//      Map<String, String> tableParameters =
//          updateTableRequestArgumentCaptor.getValue().tableInput().parameters();
//      assertEquals("101", tableParameters.get(LAST_COMMIT_TIME_SYNC));
//    }
//  }
}
