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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.parquet.schema.MessageType;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.ParquetSchemaUtil;

import io.onetable.hudi.idtracking.IdTracker;
import io.onetable.hudi.idtracking.models.IdMapping;
import io.onetable.hudi.idtracking.models.IdTracking;

public class ParquetWriteSupportUtils {
  private static final IdTracker ID_TRACKER = IdTracker.getInstance();

  public static MessageType addFieldIdsToParquetSchema(
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
    return addFieldIdsToMessageType(idTrackingOption, messageType);
  }

  private static MessageType addFieldIdsToMessageType(
      Option<IdTracking> idTrackingOption, MessageType messageType) {
    return idTrackingOption
        .map(
            idTracking -> {
              List<IdMapping> idMappings = idTracking.getIdMappings();
              NameMapping nameMapping =
                  NameMapping.of(
                      idMappings.stream()
                          .map(ParquetWriteSupportUtils::toMappedField)
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
                    .map(ParquetWriteSupportUtils::toMappedField)
                    .collect(Collectors.toList()));
    return MappedField.of(idMapping.getId(), idMapping.getName(), nestedFields);
  }
}
