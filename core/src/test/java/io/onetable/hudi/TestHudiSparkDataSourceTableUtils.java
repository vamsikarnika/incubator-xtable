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
 
package io.onetable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;

public class TestHudiSparkDataSourceTableUtils {

  @Test
  void testGetSparkTableProperties() {

    List<String> partitionNames = Arrays.asList("region", "category");
    String sparkVersion = "3.2.1";
    int schemaLengthThreshold = 1000;
    OneSchema schema =
        OneSchema.builder()
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("id")
                        .schema(OneSchema.builder().dataType(OneType.INT).isNullable(false).build())
                        .build(),
                    OneField.builder()
                        .name("name")
                        .schema(
                            OneSchema.builder().dataType(OneType.STRING).isNullable(false).build())
                        .build(),
                    OneField.builder()
                        .name("region")
                        .schema(
                            OneSchema.builder().dataType(OneType.STRING).isNullable(false).build())
                        .build(),
                    OneField.builder()
                        .name("category")
                        .schema(
                            OneSchema.builder().dataType(OneType.STRING).isNullable(false).build())
                        .build()))
            .dataType(OneType.RECORD)
            .name("testSchema")
            .build();

    Map<String, String> result =
        HudiSparkDataSourceTableUtils.getSparkTableProperties(
            partitionNames, sparkVersion, schemaLengthThreshold, schema);

    // Validate results
    assertEquals("hudi", result.get("spark.sql.sources.provider"));
    assertEquals("3.2.1", result.get("spark.sql.create.version"));
    assertEquals("1", result.get("spark.sql.sources.schema.numParts"));
    assertEquals(
        "StructType(StructField(id,IntegerType,false), StructField(name,StringType,false), StructField(region,StringType,false), StructField(category,StringType,false))",
        result.get("spark.sql.sources.schema.part.0"));
    assertEquals("2", result.get("spark.sql.sources.schema.numPartCols"));
    assertEquals("region", result.get("spark.sql.sources.schema.partCol.0"));
    assertEquals("category", result.get("spark.sql.sources.schema.partCol.1"));
  }

  @Test
  void testGetSparkTablePropertiesEmptyPartitions() {
    // Setup input data with no partitions
    List<String> partitionNames = Collections.emptyList();
    int schemaLengthThreshold = 50;
    OneSchema schema =
        OneSchema.builder()
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("id")
                        .schema(OneSchema.builder().dataType(OneType.INT).isNullable(false).build())
                        .build(),
                    OneField.builder()
                        .name("name")
                        .schema(
                            OneSchema.builder().dataType(OneType.STRING).isNullable(false).build())
                        .build()))
            .dataType(OneType.RECORD)
            .name("testSchema")
            .build();

    // Call the method
    Map<String, String> result =
        HudiSparkDataSourceTableUtils.getSparkTableProperties(
            partitionNames, "", schemaLengthThreshold, schema);

    assertEquals("hudi", result.get("spark.sql.sources.provider"));
    assertNull(result.get("spark.sql.create.version"));
    assertEquals("2", result.get("spark.sql.sources.schema.numParts"));
    assertEquals(
        "StructType(StructField(id,IntegerType,false), StructField(name,StringType,false))",
        result.get("spark.sql.sources.schema.part.0")
            + result.get("spark.sql.sources.schema.part.1"));
    assertNull(result.get("spark.sql.sources.schema.numPartCols"));
  }
}
