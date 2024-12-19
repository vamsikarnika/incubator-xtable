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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;

import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.exception.NotSupportedException;
import io.onetable.model.OneTable;
import io.onetable.model.storage.TableFormat;

public abstract class HMSCatalogSyncRequestProvider {

  protected final HMSCatalogConfig hmsCatalogConfig;

  public HMSCatalogSyncRequestProvider(HMSCatalogConfig hmsCatalogConfig) {
    this.hmsCatalogConfig = hmsCatalogConfig;
  }

  abstract Table getCreateTableInput(
      OneTable table, ExternalCatalogConfig.TableIdentifier tableIdentifier);

  abstract Table getUpdateTableInput(
      OneTable table, Table catalogTable, ExternalCatalogConfig.TableIdentifier tableIdentifier);

  static HMSCatalogSyncRequestProvider getInstance(
      String tableFormat,
      IMetaStoreClient metaStoreClient,
      HMSCatalogConfig hmsCatalogConfig,
      HMSSchemaExtractor schemaExtractor,
      Configuration configuration) {
    switch (tableFormat) {
      case TableFormat.HUDI:
        return new HudiHMSCatalogSyncRequestProvider(
            hmsCatalogConfig, metaStoreClient, schemaExtractor, configuration);
      default:
        throw new NotSupportedException("Unsupported table format: " + tableFormat);
    }
  }
}
