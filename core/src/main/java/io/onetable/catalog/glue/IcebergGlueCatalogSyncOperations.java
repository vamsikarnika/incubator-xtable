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

import java.util.HashMap;
import java.util.Map;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.util.VisibleForTesting;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.hadoop.HadoopTables;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
import io.onetable.exception.CatalogSyncException;
import io.onetable.model.OneTable;
import io.onetable.model.exception.OneTableErrorCode;
import io.onetable.model.storage.TableFormat;

/** Glue catalog sync implementation for Iceberg table */
@Log4j2
public class IcebergGlueCatalogSyncOperations extends GlueCatalogSyncOperations {

  private final HadoopTables hadoopTables;

  public IcebergGlueCatalogSyncOperations(
      ExternalCatalogConfig externalCatalogConfig, Configuration configuration) {
    super(externalCatalogConfig, configuration);
    this.hadoopTables = new HadoopTables(configuration);
  }

  @VisibleForTesting
  IcebergGlueCatalogSyncOperations(
      TableIdentifier tableIdentifier,
      GlueClient glueClient,
      GlueCatalogConfig glueCatalogConfig,
      Configuration configuration,
      HadoopTables hadoopTables,
      GlueSchemaExtractor schemaExtractor) {
    super(tableIdentifier, glueClient, glueCatalogConfig, configuration, schemaExtractor);
    this.hadoopTables = hadoopTables;
  }

  @Override
  public String getTableFormat() {
    return TableFormat.ICEBERG;
  }

  @Override
  public void createTable(OneTable table, TableIdentifier tableIdentifier) {
    BaseTable fsTable = loadTableFromFs(table.getBasePath());
    glueClient.createTable(
        CreateTableRequest.builder()
            .catalogId(glueCatalogConfig.getCatalogId())
            .databaseName(tableIdentifier.getDatabaseName())
            .tableInput(
                TableInput.builder()
                    .name(tableIdentifier.getTableName())
                    .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                    .parameters(getTableParameters(fsTable))
                    .storageDescriptor(
                        StorageDescriptor.builder()
                            .location(table.getBasePath())
                            .columns(
                                schemaExtractor.toColumns(getTableFormat(), table.getReadSchema()))
                            .build())
                    .build())
            .build());
  }

  @Override
  public void refreshTable(OneTable table, Table glueTable, TableIdentifier tableIdentifier) {
    BaseTable fsTable = loadTableFromFs(table.getBasePath());
    Map<String, String> parameters = new HashMap<>(glueTable.parameters());
    parameters.put(PREVIOUS_METADATA_LOCATION_PROP, parameters.get(METADATA_LOCATION_PROP));
    parameters.putAll(getTableParameters(fsTable));
    try {
      glueClient.updateTable(
          UpdateTableRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseName(tableIdentifier.getDatabaseName())
              .skipArchive(true)
              .tableInput(
                  TableInput.builder()
                      .name(tableIdentifier.getTableName())
                      .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                      .parameters(parameters)
                      .storageDescriptor(
                          StorageDescriptor.builder()
                              .location(table.getBasePath())
                              .columns(
                                  schemaExtractor.toColumns(
                                      getTableFormat(), table.getReadSchema(), glueTable))
                              .build())
                      .build())
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException(
          OneTableErrorCode.CATALOG_SYNC_GENERIC_EXCEPTION, "Failed to refresh iceberg table", e);
    }
  }

  @VisibleForTesting
  Map<String, String> getTableParameters(BaseTable table) {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(TABLE_TYPE_PROP, getTableFormat());
    parameters.put(METADATA_LOCATION_PROP, getMetadataFileLocation(table));
    return parameters;
  }

  private BaseTable loadTableFromFs(String tableBasePath) {
    return (BaseTable) hadoopTables.load(tableBasePath);
  }

  private String getMetadataFileLocation(BaseTable table) {
    return table.operations().current().metadataFileLocation();
  }
}
