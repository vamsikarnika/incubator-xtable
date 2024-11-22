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
 
package io.onetable.catalog;

import static io.onetable.model.storage.TableFormat.ICEBERG;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;

import io.onetable.catalog.glue.IcebergGlueCatalogSyncOperations;
import io.onetable.model.catalog.CatalogType;
import io.onetable.spi.sync.CatalogSyncClient;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CatalogClientFactory {
  private static final CatalogClientFactory INSTANCE = new CatalogClientFactory();

  public static CatalogClientFactory getInstance() {
    return INSTANCE;
  }

  public List<CatalogSyncClient> createForCatalogAndFormat(
      String tableFormat,
      ExternalCatalogConfig externalCatalogConfig,
      Configuration configuration) {
    List<CatalogSyncClient> catalogSyncClients = new ArrayList<>();
    switch (externalCatalogConfig.getCatalogType()) {
      case GLUE:
        catalogSyncClients.addAll(
            externalCatalogConfig.getTableFormatsToSync().keySet().stream()
                .filter(format -> format.equals(tableFormat))
                .map(format -> createGlueSyncClient(format, externalCatalogConfig, configuration))
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        return catalogSyncClients;
      case HMS:
      default:
        return catalogSyncClients;
    }
  }

  private CatalogSyncClient createGlueSyncClient(
      String tableFormat,
      ExternalCatalogConfig externalCatalogConfig,
      Configuration configuration) {
    if (tableFormat.equals(ICEBERG)) {
      CatalogSyncOperations<
              software.amazon.awssdk.services.glue.model.Database,
              software.amazon.awssdk.services.glue.model.Table>
          catalogSyncOperations =
              new IcebergGlueCatalogSyncOperations(externalCatalogConfig, configuration);
      return new CatalogSyncClientImpl<>(catalogSyncOperations, CatalogType.GLUE);
    }
    throw new UnsupportedOperationException(
        "GlueCatalogSyncClient not supported for " + tableFormat);
  }
}
