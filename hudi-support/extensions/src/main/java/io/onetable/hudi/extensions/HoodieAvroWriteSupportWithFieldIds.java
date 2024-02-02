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
 
package io.onetable.hudi.extensions;

import static io.onetable.hudi.extensions.ParquetWriteSupportUtils.addFieldIdsToParquetSchema;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.parquet.schema.MessageType;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

/**
 * An extension of the standard {@link HoodieAvroWriteSupport} that adds field IDs to the parquet
 * schema. When used with {@link AddFieldIdsClientInitCallback}, ID values will be set on the fields
 * in the parquet file making them compatible with Iceberg readers that do not support the default
 * field id mapping.
 */
public class HoodieAvroWriteSupportWithFieldIds extends HoodieAvroWriteSupport {

  public HoodieAvroWriteSupportWithFieldIds(
      MessageType schema,
      Schema avroSchema,
      Option<BloomFilter> bloomFilterOpt,
      Properties properties) {
    super(
        addFieldIdsToParquetSchema(
            schema, avroSchema, HoodieWriteConfig.newBuilder().withProperties(properties).build()),
        avroSchema,
        bloomFilterOpt,
        properties);
  }
}
