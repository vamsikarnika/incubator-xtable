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

import static io.onetable.catalog.CatalogUtils.hasStorageDescriptorLocationChanged;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;

import io.onetable.exception.CatalogRefreshException;
import io.onetable.model.OneTable;
import io.onetable.model.catalog.CatalogType;
import io.onetable.spi.sync.CatalogSyncClient;

@Log4j2
public class CatalogSyncClientImpl<Database, Table> implements CatalogSyncClient {

  private final CatalogSyncOperations<Database, Table> operations;
  private final CatalogType catalogType;

  public CatalogSyncClientImpl(
      CatalogSyncOperations<Database, Table> operations, CatalogType catalogType) {
    this.operations = operations;
    this.catalogType = catalogType;
  }

  @Override
  public void syncTable(OneTable oneTable) {
    ExternalCatalogConfig.TableIdentifier tableIdentifier = operations.getTableIdentifier();
    log.debug(
        "Running {} catalog sync for {} table: {}",
        catalogType,
        operations.getTableFormat(),
        tableIdentifier.getId());
    boolean doesDatabaseExists = operations.getDatabase(tableIdentifier.getDatabaseName()) != null;
    if (!doesDatabaseExists) {
      operations.createDatabase(tableIdentifier.getDatabaseName());
    }
    Table catalogTable = operations.getTable(tableIdentifier);
    String storageDescriptorLocation = operations.getStorageDescriptorLocation(catalogTable);
    if (catalogTable == null) {
      operations.createTable(oneTable, tableIdentifier);
    } else if (hasStorageDescriptorLocationChanged(
        storageDescriptorLocation, oneTable.getBasePath())) {
      // Replace table if there is a mismatch between hmsTable location and OneTable basePath.
      // Possible reasons could be:
      //  1) hms table (manually) created with a different location before and need to be
      // re-created with a new basePath
      //  2) OneTable basePath changes due to migration or other reasons
      String oldLocation =
          StringUtils.isEmpty(storageDescriptorLocation) ? "null" : storageDescriptorLocation;
      log.warn(
          "StorageDescriptor location changed from {} to {}, re-creating table",
          oldLocation,
          oneTable.getBasePath());
      operations.createOrReplaceTable(oneTable, tableIdentifier);
    } else {
      try {
        log.debug("Table metadata changed, refreshing table");
        operations.refreshTable(oneTable, catalogTable, tableIdentifier);
      } catch (CatalogRefreshException e) {
        log.warn("Table refresh failed, re-creating table", e);
        operations.createOrReplaceTable(oneTable, tableIdentifier);
      }
    }
    log.info(
        "{} catalog sync successful for {} table: {}",
        catalogType,
        operations.getTableFormat(),
        tableIdentifier.getId());
  }

  @Override
  public CatalogType getCatalogType() {
    return catalogType;
  }

  @Override
  public void close() throws Exception {
    operations.close();
  }
}
