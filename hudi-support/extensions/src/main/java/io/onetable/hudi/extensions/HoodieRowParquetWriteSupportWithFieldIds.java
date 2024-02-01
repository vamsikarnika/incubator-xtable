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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport;

import io.onetable.hudi.idtracking.IdTracker;
import io.onetable.hudi.idtracking.models.IdMapping;
import io.onetable.hudi.idtracking.models.IdTracking;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;

public class HoodieRowParquetWriteSupportWithFieldIds extends HoodieRowParquetWriteSupport {

  private static final IdTracker ID_TRACKER = IdTracker.getInstance();

  private final Schema avroSchema;
  private final HoodieWriteConfig writeConfig;

  public HoodieRowParquetWriteSupportWithFieldIds(Configuration conf, StructType structType,
                                                  Option<BloomFilter> bloomFilterOpt, HoodieWriteConfig config) {
    super(conf, structType, bloomFilterOpt, config);
    this.avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, "record");
    this.writeConfig = config;
  }

  public HoodieRowParquetWriteSupportWithFieldIds(Configuration conf, Schema avroSchema,
                                                  Option<BloomFilter> bloomFilterOpt, HoodieWriteConfig config) {
    super(conf, avroSchema, bloomFilterOpt, config);
    this.avroSchema = avroSchema;
    this.writeConfig = config;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    WriteContext superWriteContext = super.init(configuration);
    MessageType messageType = new AvroSchemaConverter().convert(avroSchema);
    return new WriteContext(addFieldIdsToParquetSchema(messageType, avroSchema, writeConfig), superWriteContext.getExtraMetaData());
  }


  private static MessageType addFieldIdsToParquetSchema(
      MessageType messageType, Schema schema, HoodieWriteConfig writeConfig) {
    Option<IdTracking> idTrackingOption = ID_TRACKER.getIdTracking(schema);
    if (!idTrackingOption.isPresent()) {
      String writeSchemaStr = writeConfig.getWriteSchema();
      // if there is a schema with ID tracking specified in the properties, fall back to inferring
      // the proper ID tracking on provided schema
      if (writeSchemaStr != null && !writeSchemaStr.isEmpty()) {
        Schema writeSchema = new Schema.Parser().parse(writeSchemaStr);
        if (ID_TRACKER.hasIdTracking(writeSchema)) {
          idTrackingOption =
              Option.of(
                  ID_TRACKER.getIdTracking(
                      schema, Option.of(writeSchema), writeConfig.populateMetaFields()));
        }
      }
    }
    return idTrackingOption
        .map(
            idTracking -> {
              List<IdMapping> idMappings = idTracking.getIdMappings();
              NameMapping nameMapping =
                  NameMapping.of(
                      idMappings.stream()
                          .map(HoodieRowParquetWriteSupportWithFieldIds::toMappedField)
                          .collect(Collectors.toList()));
              return ParquetSchemaUtil.applyNameMapping(messageType, nameMapping);
            })
        .orElse(messageType);
  }

  private static MappedField toMappedField(IdMapping idMapping) {
    MappedFields nestedFields =
        idMapping.getFields() == null
            ? null
            : MappedFields.of(
            idMapping.getFields().stream()
                .map(HoodieRowParquetWriteSupportWithFieldIds::toMappedField)
                .collect(Collectors.toList()));
    return MappedField.of(idMapping.getId(), idMapping.getName(), nestedFields);
  }
}
