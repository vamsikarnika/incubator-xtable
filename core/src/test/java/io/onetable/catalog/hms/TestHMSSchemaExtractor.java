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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.jupiter.api.Test;

import io.onetable.catalog.TestSchemaExtractorBase;
import io.onetable.exception.NotSupportedException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.storage.TableFormat;

public class TestHMSSchemaExtractor extends TestSchemaExtractorBase {

  private FieldSchema getFieldSchema(String name, String type) {
    return new FieldSchema(name, type, null);
  }

  @Test
  void testPrimitiveTypes() {
    int precision = 10;
    int scale = 5;
    Map<OneSchema.MetadataKey, Object> doubleMetadata = new HashMap<>();
    doubleMetadata.put(OneSchema.MetadataKey.DECIMAL_PRECISION, precision);
    doubleMetadata.put(OneSchema.MetadataKey.DECIMAL_SCALE, scale);
    String tableFormat = TableFormat.ICEBERG;

    OneSchema oneSchema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField("requiredBoolean", "boolean", OneType.BOOLEAN, false, 1),
                    getPrimitiveOneField("optionalBoolean", "boolean", OneType.BOOLEAN, true, 2),
                    getPrimitiveOneField("requiredInt", "integer", OneType.INT, false, 3),
                    getPrimitiveOneField("requiredLong", "long", OneType.LONG, false, 4),
                    getPrimitiveOneField("requiredDouble", "double", OneType.DOUBLE, false, 5),
                    getPrimitiveOneField("requiredFloat", "float", OneType.FLOAT, false, 6),
                    getPrimitiveOneField("requiredString", "string", OneType.STRING, false, 7),
                    getPrimitiveOneField("requiredBytes", "binary", OneType.BYTES, false, 8),
                    getPrimitiveOneField("requiredDate", "date", OneType.DATE, false, 9),
                    getPrimitiveOneField(
                        "requiredDecimal", "decimal", OneType.DECIMAL, false, 10, doubleMetadata),
                    getPrimitiveOneField(
                        "requiredTimestamp", "timestamp", OneType.TIMESTAMP, false, 11),
                    getPrimitiveOneField(
                        "requiredTimestampNTZ", "timestamp_ntz", OneType.TIMESTAMP_NTZ, false, 12)))
            .build();

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema("requiredBoolean", "boolean"),
            getFieldSchema("optionalBoolean", "boolean"),
            getFieldSchema("requiredInt", "int"),
            getFieldSchema("requiredLong", "bigint"),
            getFieldSchema("requiredDouble", "double"),
            getFieldSchema("requiredFloat", "float"),
            getFieldSchema("requiredString", "string"),
            getFieldSchema("requiredBytes", "binary"),
            getFieldSchema("requiredDate", "date"),
            getFieldSchema("requiredDecimal", String.format("decimal(%s,%s)", precision, scale)),
            getFieldSchema("requiredTimestamp", "timestamp"),
            getFieldSchema("requiredTimestampNTZ", "timestamp"));

    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testTimestamps() {
    String tableFormat = TableFormat.ICEBERG;
    Map<OneSchema.MetadataKey, Object> millisTimestamp =
        Collections.singletonMap(
            OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS);

    Map<OneSchema.MetadataKey, Object> microsTimestamp =
        Collections.singletonMap(
            OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MICROS);

    OneSchema oneSchema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField(
                        "requiredTimestampMillis",
                        "timestamp",
                        OneType.TIMESTAMP,
                        false,
                        1,
                        millisTimestamp),
                    getPrimitiveOneField(
                        "requiredTimestampMicros",
                        "timestamp",
                        OneType.TIMESTAMP,
                        false,
                        2,
                        microsTimestamp),
                    getPrimitiveOneField(
                        "requiredTimestampNTZMillis",
                        "timestamp_ntz",
                        OneType.TIMESTAMP_NTZ,
                        false,
                        3,
                        millisTimestamp),
                    getPrimitiveOneField(
                        "requiredTimestampNTZMicros",
                        "timestamp_ntz",
                        OneType.TIMESTAMP_NTZ,
                        false,
                        4,
                        microsTimestamp)))
            .build();

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema("requiredTimestampMillis", "timestamp"),
            getFieldSchema("requiredTimestampMicros", "timestamp"),
            getFieldSchema("requiredTimestampNTZMillis", "timestamp"),
            getFieldSchema("requiredTimestampNTZMicros", "timestamp"));

    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testMaps() {
    String tableFormat = TableFormat.ICEBERG;
    OneSchema recordMapElementSchema =
        OneSchema.builder()
            .name("struct")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    getPrimitiveOneField(
                        "requiredDouble",
                        "double",
                        OneType.DOUBLE,
                        false,
                        1,
                        "recordMap._one_field_value"),
                    getPrimitiveOneField(
                        "optionalString",
                        "string",
                        OneType.STRING,
                        true,
                        2,
                        "recordMap._one_field_value")))
            .dataType(OneType.RECORD)
            .build();

    OneSchema oneSchema =
        OneSchema.builder()
            .name("record")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("intMap")
                        .fieldId(1)
                        .schema(
                            OneSchema.builder()
                                .name("map")
                                .isNullable(false)
                                .dataType(OneType.MAP)
                                .fields(
                                    Arrays.asList(
                                        getPrimitiveOneField(
                                            OneField.Constants.MAP_KEY_FIELD_NAME,
                                            "string",
                                            OneType.STRING,
                                            false,
                                            3,
                                            "intMap"),
                                        getPrimitiveOneField(
                                            OneField.Constants.MAP_VALUE_FIELD_NAME,
                                            "integer",
                                            OneType.INT,
                                            false,
                                            4,
                                            "intMap")))
                                .build())
                        .build(),
                    OneField.builder()
                        .name("recordMap")
                        .fieldId(2)
                        .schema(
                            OneSchema.builder()
                                .name("map")
                                .isNullable(true)
                                .dataType(OneType.MAP)
                                .fields(
                                    Arrays.asList(
                                        getPrimitiveOneField(
                                            OneField.Constants.MAP_KEY_FIELD_NAME,
                                            "integer",
                                            OneType.INT,
                                            false,
                                            5,
                                            "recordMap"),
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                                            .fieldId(6)
                                            .parentPath("recordMap")
                                            .schema(recordMapElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema("intMap", "map<string,int>"),
            getFieldSchema(
                "recordMap", "map<int,struct<requiredDouble:double,optionalString:string>>"));

    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testLists() {
    String tableFormat = TableFormat.ICEBERG;
    OneSchema recordListElementSchema =
        OneSchema.builder()
            .name("struct")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    getPrimitiveOneField(
                        "requiredDouble",
                        "double",
                        OneType.DOUBLE,
                        false,
                        11,
                        "recordMap._one_field_value"),
                    getPrimitiveOneField(
                        "optionalString",
                        "string",
                        OneType.STRING,
                        true,
                        12,
                        "recordMap._one_field_value")))
            .dataType(OneType.RECORD)
            .build();

    OneSchema oneSchema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .name("record")
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("intList")
                        .fieldId(1)
                        .schema(
                            OneSchema.builder()
                                .name("list")
                                .isNullable(false)
                                .dataType(OneType.LIST)
                                .fields(
                                    Collections.singletonList(
                                        getPrimitiveOneField(
                                            OneField.Constants.ARRAY_ELEMENT_FIELD_NAME,
                                            "integer",
                                            OneType.INT,
                                            false,
                                            13,
                                            "intList")))
                                .build())
                        .build(),
                    OneField.builder()
                        .name("recordList")
                        .fieldId(2)
                        .schema(
                            OneSchema.builder()
                                .name("list")
                                .isNullable(true)
                                .dataType(OneType.LIST)
                                .fields(
                                    Collections.singletonList(
                                        OneField.builder()
                                            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .fieldId(14)
                                            .parentPath("recordList")
                                            .schema(recordListElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema("intList", "array<int>"),
            getFieldSchema(
                "recordList", "array<struct<requiredDouble:double,optionalString:string>>"));

    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testNestedRecords() {
    String tableFormat = TableFormat.ICEBERG;
    OneSchema oneSchema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .name("record")
            .isNullable(false)
            .fields(
                Collections.singletonList(
                    OneField.builder()
                        .name("nestedOne")
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .fieldId(1)
                        .schema(
                            OneSchema.builder()
                                .name("struct")
                                .dataType(OneType.RECORD)
                                .isNullable(true)
                                .fields(
                                    Arrays.asList(
                                        getPrimitiveOneField(
                                            "nestedOptionalInt",
                                            "integer",
                                            OneType.INT,
                                            true,
                                            11,
                                            "nestedOne"),
                                        getPrimitiveOneField(
                                            "nestedRequiredDouble",
                                            "double",
                                            OneType.DOUBLE,
                                            false,
                                            12,
                                            "nestedOne"),
                                        OneField.builder()
                                            .name("nestedTwo")
                                            .parentPath("nestedOne")
                                            .fieldId(13)
                                            .schema(
                                                OneSchema.builder()
                                                    .name("struct")
                                                    .dataType(OneType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Collections.singletonList(
                                                            getPrimitiveOneField(
                                                                "doublyNestedString",
                                                                "string",
                                                                OneType.STRING,
                                                                true,
                                                                14,
                                                                "nestedOne.nestedTwo")))
                                                    .build())
                                            .build()))
                                .build())
                        .build()))
            .build();

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema(
                "nestedOne",
                "struct<nestedOptionalInt:int,nestedRequiredDouble:double,nestedTwo:struct<doublyNestedString:string>>"));
    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testUnsupportedType() {
    String tableFormat = TableFormat.ICEBERG;
    // Unknown "UNION" type
    OneSchema oneSchema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField("optionalBoolean", "boolean", OneType.BOOLEAN, true, 2),
                    OneField.builder()
                        .name("unionField")
                        .schema(
                            OneSchema.builder()
                                .name("unionSchema")
                                .dataType(OneType.UNION)
                                .isNullable(true)
                                .build())
                        .fieldId(2)
                        .build()))
            .build();

    NotSupportedException exception =
        assertThrows(
            NotSupportedException.class,
            () -> HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
    assertEquals("Unsupported type: OneType.UNION(name=union)", exception.getMessage());

    // Invalid decimal type (precision and scale metadata is missing)
    OneSchema oneSchema2 =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField("optionalBoolean", "boolean", OneType.BOOLEAN, true, 1),
                    getPrimitiveOneField("optionalDecimal", "decimal", OneType.DECIMAL, true, 2)))
            .build();

    exception =
        assertThrows(
            NotSupportedException.class,
            () -> HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema2));
    assertEquals("Invalid decimal type, precision and scale is missing", exception.getMessage());

    // Invalid decimal type (scale metadata is missing)
    Map<OneSchema.MetadataKey, Object> doubleMetadata = new HashMap<>();
    doubleMetadata.put(OneSchema.MetadataKey.DECIMAL_PRECISION, 10);
    OneSchema oneSchema3 =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField("optionalBoolean", "boolean", OneType.BOOLEAN, true, 1),
                    getPrimitiveOneField(
                        "optionalDecimal", "decimal", OneType.DECIMAL, true, 2, doubleMetadata)))
            .build();

    exception =
        assertThrows(
            NotSupportedException.class,
            () -> HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema3));
    assertEquals("Invalid decimal type, scale is missing", exception.getMessage());
  }
}
