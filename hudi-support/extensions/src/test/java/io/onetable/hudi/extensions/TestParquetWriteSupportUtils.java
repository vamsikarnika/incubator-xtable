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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import io.onetable.hudi.idtracking.IdTracker;

public class TestParquetWriteSupportUtils {

  private final IdTracker ID_TRACKER = IdTracker.getInstance();

  @Test
  public void testAddFieldIdsToMessageType() {

    Schema fullSchema =
        Schema.createRecord(
            "testAddFieldIdsToMessageType",
            null,
            null,
            false,
            Arrays.asList(
                new Schema.Field("a", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("b", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("c", Schema.create(Schema.Type.STRING), null, null)));

    // test with ID tracking in the write config but not in the avro schema
    Schema startingSchema =
        HoodieAvroUtils.generateProjectionSchema(fullSchema, Collections.singletonList("a"));
    Schema startingSchemaWithID = ID_TRACKER.addIdTracking(startingSchema, Option.empty(), false);
    HoodieWriteConfig startingWriteConfig =
        new HoodieWriteConfig.Builder()
            .withPath("null")
            .withSchema(startingSchemaWithID.toString())
            .build();
    MessageType firstMessageType = new AvroSchemaConverter().convert(startingSchema);
    assertNoID(firstMessageType);
    MessageType firstMessageTypeWithIds =
        ParquetWriteSupportUtils.addFieldIdsToParquetSchema(
            firstMessageType, startingSchema, startingWriteConfig);
    checkMessageType(firstMessageTypeWithIds, Collections.singletonList(1));

    // test with ID tracking in the avro schema but not the write config
    Schema fullSchemaWithId =
        ID_TRACKER.addIdTracking(fullSchema, Option.of(startingSchemaWithID), false);
    MessageType fullMessageType = new AvroSchemaConverter().convert(fullSchema);
    assertNoID(fullMessageType);
    MessageType fullMessageTypeWithIds =
        ParquetWriteSupportUtils.addFieldIdsToParquetSchema(
            fullMessageType, fullSchemaWithId, null);
    checkMessageType(fullMessageTypeWithIds, Arrays.asList(1, 2, 3));

    // test with ID tracking with dropped column
    Schema dropBSchema =
        HoodieAvroUtils.generateProjectionSchema(fullSchema, Arrays.asList("a", "c"));
    MessageType dropBmessage = new AvroSchemaConverter().convert(dropBSchema);
    assertNoID(dropBmessage);
    MessageType dropBmessageWithIds =
        ParquetWriteSupportUtils.addFieldIdsToParquetSchema(dropBmessage, fullSchemaWithId, null);
    checkMessageType(dropBmessageWithIds, Arrays.asList(1, 3));
  }

  private static void assertNoID(MessageType messageType) {
    messageType.getFields().forEach(t -> assertNull(t.getId()));
  }

  private static void checkMessageType(MessageType messageType, List<Integer> expected) {
    Set<Integer> idSet =
        messageType.getFields().stream()
            .filter(t -> t.getId() != null)
            .map(t -> t.getId().intValue())
            .collect(Collectors.toSet());
    assertEquals(expected.size(), idSet.size());
    for (Integer e : expected) {
      assertTrue(idSet.contains(e));
    }
  }
}
