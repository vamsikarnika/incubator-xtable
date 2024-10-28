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

import static io.onetable.catalog.CatalogUtils.hasStorageDescriptorLocationChanged;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Table;

import io.onetable.catalog.CatalogConfigFactory;
import io.onetable.catalog.CatalogSyncOperations;
import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
import io.onetable.exception.CatalogRefreshException;
import io.onetable.model.OneTable;
import io.onetable.model.catalog.CatalogType;
import io.onetable.spi.sync.CatalogSyncClient;

@Log4j2
public abstract class GlueCatalogSyncClient
    implements CatalogSyncOperations<Database, Table>, CatalogSyncClient {
  protected static final String GLUE_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
  protected final TableIdentifier tableIdentifier;
  protected final GlueClient glueClient;
  protected final GlueCatalogConfig glueCatalogConfig;
  protected final Configuration configuration;

  public GlueCatalogSyncClient(
      ExternalCatalogConfig externalCatalogConfig, Configuration configuration) {
    this.glueCatalogConfig =
        CatalogConfigFactory.getGlueCatalogConfig(externalCatalogConfig.getCatalogProperties());
    GlueClientBuilder builder = GlueClient.builder();
    if (!StringUtils.isEmpty(glueCatalogConfig.getRegion())) {
      builder.region(Region.of(glueCatalogConfig.getRegion()));
    }
    if (!StringUtils.isEmpty(glueCatalogConfig.getClientCredentialsProviderClass())) {
      builder.credentialsProvider(DefaultCredentialsProvider.create());
    }
    // TODO: Convert CredentialsProviderClass to AWSCredentialsProvider
    // [https://app.clickup.com/t/18029943/ENG-16065]
    this.glueClient = builder.build();
    this.tableIdentifier = externalCatalogConfig.getTableFormatsToSync().get(getTableFormat());
    this.configuration = configuration;
  }

  GlueCatalogSyncClient(
      TableIdentifier tableIdentifier,
      GlueClient glueClient,
      GlueCatalogConfig glueCatalogConfig,
      Configuration configuration) {
    this.tableIdentifier = tableIdentifier;
    this.glueClient = glueClient;
    this.glueCatalogConfig = glueCatalogConfig;
    this.configuration = configuration;
  }

  @Override
  public void syncTable(OneTable table) {
    boolean doesDatabaseExists = getDatabase(tableIdentifier.getDatabaseName()) != null;
    if (!doesDatabaseExists) {
      createDatabase(tableIdentifier.getDatabaseName());
    }
    Table glueTable = getTable(tableIdentifier);
    if (glueTable == null) {
      createTable(table, tableIdentifier);
    } else if (glueTable.storageDescriptor() == null
        || hasStorageDescriptorLocationChanged(
            glueTable.storageDescriptor().location(), table.getBasePath())) {
      // Replace table if there is a mismatch between glueTable location and OneTable basePath.
      // Possible reasons could be:
      //  1) glue table (manually) created with a different location before and need to be
      // re-created with a new basePath
      //  2) OneTable basePath changes due to migration or other reasons
      String oldLocation =
          glueTable.storageDescriptor() == null
                  || StringUtils.isEmpty(glueTable.storageDescriptor().location())
              ? "null"
              : glueTable.storageDescriptor().location();
      log.warn(
          "StorageDescriptor location changed from {} to {}, re-creating table",
          oldLocation,
          table.getBasePath());
      createOrReplaceTable(table, tableIdentifier);
    } else {
      try {
        log.debug("Table metadata changed, refreshing table");
        refreshTable(table, glueTable, tableIdentifier);
      } catch (CatalogRefreshException e) {
        log.warn("Table refresh failed, re-creating table", e);
        createOrReplaceTable(table, tableIdentifier);
      }
    }
  }

  @Override
  public CatalogType getCatalogType() {
    return CatalogType.GLUE;
  }
}
