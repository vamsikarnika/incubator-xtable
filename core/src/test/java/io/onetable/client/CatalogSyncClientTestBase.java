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
 
package io.onetable.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;

import io.onetable.catalog.CatalogClientFactory;
import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.model.catalog.CatalogType;
import io.onetable.model.storage.TableFormat;
import io.onetable.spi.sync.CatalogSyncClient;

public class CatalogSyncClientTestBase {

  protected final Configuration mockConf = mock(Configuration.class);
  protected final CatalogClientFactory mockCatalogClientFactory = mock(CatalogClientFactory.class);
  protected final CatalogSyncClient mockIcebergGlueCatalogSyncClient1 =
      mock(CatalogSyncClient.class);
  protected final CatalogSyncClient mockIcebergGlueCatalogSyncClient2 =
      mock(CatalogSyncClient.class);
  protected final CatalogSyncClient mockIcebergHmsCatalogSyncClient1 =
      mock(CatalogSyncClient.class);
  protected final CatalogSyncClient mockIcebergHmsCatalogSyncClient2 =
      mock(CatalogSyncClient.class);
  protected final ExternalCatalogConfig mockGlueCatalogConfig1 =
      getExternalTableConfig("glue-catalog-1", CatalogType.GLUE);
  protected final ExternalCatalogConfig mockGlueCatalogConfig2 =
      getExternalTableConfig("glue-catalog-2", CatalogType.GLUE);
  protected final ExternalCatalogConfig mockHmsCatalogConfig1 =
      getExternalTableConfig("hms-catalog-1", CatalogType.HMS);
  protected final ExternalCatalogConfig mockHmsCatalogConfig2 =
      getExternalTableConfig("hms-catalog-2", CatalogType.HMS);

  protected ExternalCatalogConfig getExternalTableConfig(
      String catalogIdentifier, CatalogType catalogType) {
    return ExternalCatalogConfig.builder()
        .catalogIdentifier(catalogIdentifier)
        .catalogType(catalogType)
        .tableFormatsToSync(getTableFormatsToSync())
        .catalogProperties(new HashMap<>())
        .build();
  }

  protected Map<String, ExternalCatalogConfig.TableIdentifier> getTableFormatsToSync() {
    Map<String, ExternalCatalogConfig.TableIdentifier> tableFormatsToSync = new HashMap<>();
    tableFormatsToSync.put(
        TableFormat.ICEBERG,
        ExternalCatalogConfig.TableIdentifier.builder()
            .databaseName("iceberg_db")
            .tableName("iceberg_table")
            .build());
    return tableFormatsToSync;
  }

  protected List<CatalogSyncClient> getMockCatalogSyncClientsForFormat(
      String tableFormat, boolean isExternalCatalogSyncEnabled) {
    if (isExternalCatalogSyncEnabled) {
      if (tableFormat.equals(TableFormat.ICEBERG)) {
        return Arrays.asList(
            // iceberg glue clients
            mockIcebergGlueCatalogSyncClient1,
            mockIcebergGlueCatalogSyncClient2,
            // iceberg hms clients
            mockIcebergHmsCatalogSyncClient1,
            mockIcebergHmsCatalogSyncClient2);
      } else {
        return Collections.emptyList();
      }
    } else {
      return Collections.emptyList();
    }
  }

  protected void mockCreateIcebergCatalogClient() {
    // mock iceberg glue clients creation
    when(mockCatalogClientFactory.createForCatalogAndFormat(
            TableFormat.ICEBERG, mockGlueCatalogConfig1, mockConf))
        .thenReturn(Collections.singletonList(mockIcebergGlueCatalogSyncClient1));
    when(mockCatalogClientFactory.createForCatalogAndFormat(
            TableFormat.ICEBERG, mockGlueCatalogConfig2, mockConf))
        .thenReturn(Collections.singletonList(mockIcebergGlueCatalogSyncClient2));
    // mock iceberg hms clients creation
    when(mockCatalogClientFactory.createForCatalogAndFormat(
            TableFormat.ICEBERG, mockHmsCatalogConfig1, mockConf))
        .thenReturn(Collections.singletonList(mockIcebergHmsCatalogSyncClient1));
    when(mockCatalogClientFactory.createForCatalogAndFormat(
            TableFormat.ICEBERG, mockHmsCatalogConfig2, mockConf))
        .thenReturn(Collections.singletonList(mockIcebergHmsCatalogSyncClient2));
  }

  protected void verifyCreateGlueCatalogClientsForIcebergFormat(
      boolean isExternalCatalogSyncEnabled) {
    if (!isExternalCatalogSyncEnabled) {
      verify(mockCatalogClientFactory, never())
          .createForCatalogAndFormat(TableFormat.ICEBERG, mockGlueCatalogConfig1, mockConf);
      verify(mockCatalogClientFactory, never())
          .createForCatalogAndFormat(TableFormat.ICEBERG, mockGlueCatalogConfig2, mockConf);
    } else {
      verify(mockCatalogClientFactory, times(1))
          .createForCatalogAndFormat(TableFormat.ICEBERG, mockGlueCatalogConfig1, mockConf);
      verify(mockCatalogClientFactory, times(1))
          .createForCatalogAndFormat(TableFormat.ICEBERG, mockGlueCatalogConfig2, mockConf);
    }
  }

  protected void verifyCreateHmsCatalogClientsForIcebergFormat(
      boolean isExternalCatalogSyncEnabled) {
    if (!isExternalCatalogSyncEnabled) {
      verify(mockCatalogClientFactory, never())
          .createForCatalogAndFormat(TableFormat.ICEBERG, mockHmsCatalogConfig1, mockConf);
      verify(mockCatalogClientFactory, never())
          .createForCatalogAndFormat(TableFormat.ICEBERG, mockHmsCatalogConfig2, mockConf);
    } else {
      verify(mockCatalogClientFactory, times(1))
          .createForCatalogAndFormat(TableFormat.ICEBERG, mockHmsCatalogConfig1, mockConf);
      verify(mockCatalogClientFactory, times(1))
          .createForCatalogAndFormat(TableFormat.ICEBERG, mockHmsCatalogConfig2, mockConf);
    }
  }

  protected void verifyCreateGlueCatalogClientsForDeltaFormat() {
    verify(mockCatalogClientFactory, never())
        .createForCatalogAndFormat(TableFormat.DELTA, mockGlueCatalogConfig1, mockConf);
    verify(mockCatalogClientFactory, never())
        .createForCatalogAndFormat(TableFormat.DELTA, mockGlueCatalogConfig2, mockConf);
  }

  protected void verifyCreateHmsCatalogClientsForDeltaFormat() {
    verify(mockCatalogClientFactory, never())
        .createForCatalogAndFormat(TableFormat.DELTA, mockHmsCatalogConfig1, mockConf);
    verify(mockCatalogClientFactory, never())
        .createForCatalogAndFormat(TableFormat.DELTA, mockHmsCatalogConfig2, mockConf);
  }

  @SneakyThrows
  protected void verifyCatalogSyncClientsClose(boolean isExternalCatalogSyncEnabled) {
    if (isExternalCatalogSyncEnabled) {
      verify(mockIcebergGlueCatalogSyncClient1, times(1)).close();
      verify(mockIcebergGlueCatalogSyncClient2, times(1)).close();
      verify(mockIcebergHmsCatalogSyncClient1, times(1)).close();
      verify(mockIcebergHmsCatalogSyncClient1, times(1)).close();
    } else {
      verify(mockIcebergGlueCatalogSyncClient1, never()).close();
      verify(mockIcebergGlueCatalogSyncClient2, never()).close();
      verify(mockIcebergHmsCatalogSyncClient1, never()).close();
      verify(mockIcebergHmsCatalogSyncClient1, never()).close();
    }
  }
}
