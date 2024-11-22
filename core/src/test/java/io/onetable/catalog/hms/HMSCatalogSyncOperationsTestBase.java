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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.mockito.Mock;

import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneSchema;

public class HMSCatalogSyncOperationsTestBase {

  @Mock protected IMetaStoreClient mockMetaStoreClient;
  @Mock protected HMSCatalogConfig mockCatalogConfig;
  @Mock protected HMSSchemaExtractor mockHmsSchemaExtractor;
  protected Configuration mockConfiguration = new Configuration();

  protected static final String HMS_DATABASE = "hms_db";
  protected static final String HMS_TABLE = "hms_table";
  protected static final String ONETABLE_BASE_PATH = "onetable-base-path";
  protected static final OneTable TEST_ONETABLE =
      OneTable.builder()
          .basePath(ONETABLE_BASE_PATH)
          .readSchema(OneSchema.builder().fields(Collections.emptyList()).build())
          .build();
  protected static final ExternalCatalogConfig.TableIdentifier TEST_TABLE_IDENTIFIER =
      ExternalCatalogConfig.TableIdentifier.builder()
          .databaseName(HMS_DATABASE)
          .tableName(HMS_TABLE)
          .build();

  protected Table newHmsTable(String dbName, String tableName) {
    return newHmsTable(dbName, tableName, new HashMap<>());
  }

  protected Table newHmsTable(String dbName, String tableName, Map<String, String> params) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setParameters(params);
    return table;
  }

  protected Database newDatabase(String dbName) {
    return new Database(dbName, "created by HMSCatalogSyncClient", null, Collections.emptyMap());
  }
}
