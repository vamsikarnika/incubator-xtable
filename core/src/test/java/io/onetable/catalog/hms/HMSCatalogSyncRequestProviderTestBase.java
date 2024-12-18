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

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.mockito.Mock;

import io.onetable.avro.AvroSchemaConverter;
import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;

public class HMSCatalogSyncRequestProviderTestBase {

  @Mock protected IMetaStoreClient mockMetaStoreClient;
  @Mock protected HMSCatalogConfig mockCatalogConfig;
  protected Configuration mockConfiguration = new Configuration();

  protected static final String HMS_DATABASE = "hms_db";
  protected static final String HMS_TABLE = "hms_table";
  protected static final String ONETABLE_BASE_PATH = "onetable-base-path";
  protected static final OneTable TEST_ONETABLE =
      OneTable.builder()
          .basePath(ONETABLE_BASE_PATH)
          .readSchema(OneSchema.builder().fields(Collections.emptyList()).build())
          .partitioningFields(Collections.emptyList())
          .build();
  protected static String avroSchema =
      "{\"type\":\"record\",\"name\":\"SimpleRecord\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"partitionKey\",\"type\":\"string\"}]}";
  protected static String evolvedAvroSchema =
      "{\"type\":\"record\",\"name\":\"SimpleRecord\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"partitionKey\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
  protected static final OneTable TEST_ONETABLE_WITH_SCHEMA =
      OneTable.builder()
          .basePath(ONETABLE_BASE_PATH)
          .readSchema(
              AvroSchemaConverter.getInstance().toOneSchema(new Schema.Parser().parse(avroSchema)))
          .partitioningFields(
              Collections.singletonList(
                  OnePartitionField.builder()
                      .sourceField(
                          OneField.builder()
                              .name("partitionKey")
                              .schema(OneSchema.builder().dataType(OneType.STRING).build())
                              .build())
                      .build()))
          .build();
  protected static final OneTable TEST_ONETABLE_WITH_EVOLVED_SCHEMA =
      OneTable.builder()
          .basePath(ONETABLE_BASE_PATH)
          .readSchema(
              AvroSchemaConverter.getInstance()
                  .toOneSchema(new Schema.Parser().parse(evolvedAvroSchema)))
          .partitioningFields(
              Collections.singletonList(
                  OnePartitionField.builder()
                      .sourceField(
                          OneField.builder()
                              .name("partitionKey")
                              .schema(OneSchema.builder().dataType(OneType.STRING).build())
                              .build())
                      .build()))
          .build();
  protected static final ExternalCatalogConfig.TableIdentifier TEST_TABLE_IDENTIFIER =
      ExternalCatalogConfig.TableIdentifier.builder()
          .databaseName(HMS_DATABASE)
          .tableName(HMS_TABLE)
          .build();
}
