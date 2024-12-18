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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;

public class TestOneSchema2SparkSchemaUtils {

  @Test
  public void testConvertToSparkSchemaValidRecord() {
    // Setup: Create a valid RECORD schema
    OneSchema schema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("id")
                        .schema(OneSchema.builder().dataType(OneType.INT).isNullable(false).build())
                        .build(),
                    OneField.builder()
                        .name("name")
                        .schema(
                            OneSchema.builder().dataType(OneType.STRING).isNullable(true).build())
                        .build()))
            .build();

    // Call method
    StructType sparkSchema = OneSchema2SparkSchemaUtils.convertToSparkSchema(schema);

    // Validate
    assertNotNull(sparkSchema);
    assertEquals(2, sparkSchema.fields().length);

    // Validate field details
    StructField idField = sparkSchema.fields()[0];
    assertEquals("id", idField.name());
    assertEquals(DataTypes.IntegerType, idField.dataType());
    assertFalse(idField.nullable());

    StructField nameField = sparkSchema.fields()[1];
    assertEquals("name", nameField.name());
    assertEquals(DataTypes.StringType, nameField.dataType());
    assertTrue(nameField.nullable());
  }

  @Test
  public void testConvertToSparkSchemaInvalidRootType() {
    // Setup: Non-RECORD schema at the root
    OneSchema schema = OneSchema.builder().dataType(OneType.STRING).build();

    // Call method and expect exception
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> OneSchema2SparkSchemaUtils.convertToSparkSchema(schema));

    // Validate
    assertEquals("Root schema must be a RECORD type", exception.getMessage());
  }

  @Test
  public void testConvertToSparkSchemaNestedRecord() {
    // Setup: Nested RECORD schema
    OneSchema nestedSchema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("innerId")
                        .schema(
                            OneSchema.builder().dataType(OneType.LONG).isNullable(false).build())
                        .build()))
            .build();

    OneSchema schema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("id")
                        .schema(OneSchema.builder().dataType(OneType.INT).isNullable(false).build())
                        .build(),
                    OneField.builder().name("details").schema(nestedSchema).build()))
            .build();

    // Call method
    StructType sparkSchema = OneSchema2SparkSchemaUtils.convertToSparkSchema(schema);

    // Validate
    assertNotNull(sparkSchema);
    assertEquals(2, sparkSchema.fields().length);

    StructField detailsField = sparkSchema.fields()[1];
    assertEquals("details", detailsField.name());
    assertTrue(detailsField.dataType() instanceof StructType);

    StructType nestedSparkSchema = (StructType) detailsField.dataType();
    assertEquals(1, nestedSparkSchema.fields().length);
    assertEquals("innerId", nestedSparkSchema.fields()[0].name());
    assertEquals(DataTypes.LongType, nestedSparkSchema.fields()[0].dataType());
  }

  @Test
  public void testConvertToSparkSchemaArrayType() {
    // Setup: Array schema
    OneSchema arraySchema =
        OneSchema.builder()
            .dataType(OneType.LIST)
            .fields(
                Collections.singletonList(
                    OneField.builder()
                        .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                        .schema(
                            OneSchema.builder().dataType(OneType.STRING).isNullable(false).build())
                        .build()))
            .build();

    OneSchema schema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .fields(
                Collections.singletonList(
                    OneField.builder().name("tags").schema(arraySchema).build()))
            .build();

    // Call method
    StructType sparkSchema = OneSchema2SparkSchemaUtils.convertToSparkSchema(schema);

    // Validate
    StructField tagsField = sparkSchema.fields()[0];
    assertEquals("tags", tagsField.name());
    assertTrue(tagsField.dataType() instanceof ArrayType);

    ArrayType tagsArrayType = (ArrayType) tagsField.dataType();
    assertEquals(DataTypes.StringType, tagsArrayType.elementType());
  }

  @Test
  public void testConvertToSparkSchemaMapType() {
    // Setup: Map schema
    OneSchema mapSchema =
        OneSchema.builder()
            .dataType(OneType.MAP)
            .fields(
                Collections.singletonList(
                    OneField.builder()
                        .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                        .schema(OneSchema.builder().dataType(OneType.INT).isNullable(false).build())
                        .build()))
            .build();

    OneSchema schema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .fields(
                Collections.singletonList(
                    OneField.builder().name("attributes").schema(mapSchema).build()))
            .build();

    // Call method
    StructType sparkSchema = OneSchema2SparkSchemaUtils.convertToSparkSchema(schema);

    // Validate
    StructField attributesField = sparkSchema.fields()[0];
    assertEquals("attributes", attributesField.name());
    assertTrue(attributesField.dataType() instanceof MapType);

    MapType attributesMapType = (MapType) attributesField.dataType();
    assertEquals(DataTypes.StringType, attributesMapType.keyType());
    assertEquals(DataTypes.IntegerType, attributesMapType.valueType());
  }
}
