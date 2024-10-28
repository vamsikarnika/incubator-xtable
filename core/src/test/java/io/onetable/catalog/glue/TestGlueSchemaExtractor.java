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
 
package io.onetable.catalog.glue;

import static io.onetable.catalog.glue.GlueSchemaExtractor.getColumnProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.onetable.exception.NotSupportedException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.storage.TableFormat;

public class TestGlueSchemaExtractor {

  private OneField getPrimitiveOneField(
      String fieldName, String schemaName, OneType dataType, boolean isNullable, int fieldId) {
    return getPrimitiveOneField(
        fieldName, schemaName, dataType, isNullable, fieldId, Collections.emptyMap());
  }

  private OneField getPrimitiveOneField(
      String fieldName,
      String schemaName,
      OneType dataType,
      boolean isNullable,
      int fieldId,
      String parentPath) {
    return getPrimitiveOneField(
        fieldName, schemaName, dataType, isNullable, fieldId, parentPath, Collections.emptyMap());
  }

  private OneField getPrimitiveOneField(
      String fieldName,
      String schemaName,
      OneType dataType,
      boolean isNullable,
      int fieldId,
      Map<OneSchema.MetadataKey, Object> metadata) {
    return getPrimitiveOneField(
        fieldName, schemaName, dataType, isNullable, fieldId, null, metadata);
  }

  private OneField getPrimitiveOneField(
      String fieldName,
      String schemaName,
      OneType dataType,
      boolean isNullable,
      int fieldId,
      String parentPath,
      Map<OneSchema.MetadataKey, Object> metadata) {
    return OneField.builder()
        .name(fieldName)
        .parentPath(parentPath)
        .schema(
            OneSchema.builder()
                .name(schemaName)
                .dataType(dataType)
                .isNullable(isNullable)
                .metadata(metadata)
                .build())
        .fieldId(fieldId)
        .build();
  }

  private Column getCurrentGlueTableColumn(
      String tableFormat, String colName, String colType, Integer fieldId, boolean isNullable) {
    fieldId = fieldId != null ? fieldId : -1;
    return Column.builder()
        .name(colName)
        .type(colType)
        .parameters(
            ImmutableMap.of(
                getColumnProperty(tableFormat, "field.id"), Integer.toString(fieldId),
                getColumnProperty(tableFormat, "field.optional"), Boolean.toString(isNullable),
                getColumnProperty(tableFormat, "field.current"), "true"))
        .build();
  }

  private Column getPreviousGlueTableColumn(String tableFormat, String colName, String colType) {
    return Column.builder()
        .name(colName)
        .type(colType)
        .parameters(ImmutableMap.of(getColumnProperty(tableFormat, "field.current"), "false"))
        .build();
  }

  @Test
  void testPrimitiveTypes_NoExistingTable() {
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

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "requiredBoolean", "boolean", 1, false),
            getCurrentGlueTableColumn(tableFormat, "optionalBoolean", "boolean", 2, true),
            getCurrentGlueTableColumn(tableFormat, "requiredInt", "int", 3, false),
            getCurrentGlueTableColumn(tableFormat, "requiredLong", "bigint", 4, false),
            getCurrentGlueTableColumn(tableFormat, "requiredDouble", "double", 5, false),
            getCurrentGlueTableColumn(tableFormat, "requiredFloat", "float", 6, false),
            getCurrentGlueTableColumn(tableFormat, "requiredString", "string", 7, false),
            getCurrentGlueTableColumn(tableFormat, "requiredBytes", "binary", 8, false),
            getCurrentGlueTableColumn(tableFormat, "requiredDate", "date", 9, false),
            getCurrentGlueTableColumn(
                tableFormat,
                "requiredDecimal",
                String.format("decimal(%s,%s)", precision, scale),
                10,
                false),
            getCurrentGlueTableColumn(tableFormat, "requiredTimestamp", "timestamp", 11, false),
            getCurrentGlueTableColumn(tableFormat, "requiredTimestampNTZ", "timestamp", 12, false));

    assertEquals(expectedGlueColumns, GlueSchemaExtractor.toColumns(tableFormat, oneSchema));
  }

  @Test
  void testTimestamps_NoExistingTable() {
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

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(
                tableFormat, "requiredTimestampMillis", "timestamp", 1, false),
            getCurrentGlueTableColumn(
                tableFormat, "requiredTimestampMicros", "timestamp", 2, false),
            getCurrentGlueTableColumn(
                tableFormat, "requiredTimestampNTZMillis", "timestamp", 3, false),
            getCurrentGlueTableColumn(
                tableFormat, "requiredTimestampNTZMicros", "timestamp", 4, false));

    assertEquals(expectedGlueColumns, GlueSchemaExtractor.toColumns(tableFormat, oneSchema));
  }

  @Test
  void testMaps_NoExistingTable() {
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

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "intMap", "map<string,int>", 1, false),
            getCurrentGlueTableColumn(
                tableFormat,
                "recordMap",
                "map<int,struct<requiredDouble:double,optionalString:string>>",
                2,
                true));

    assertEquals(expectedGlueColumns, GlueSchemaExtractor.toColumns(tableFormat, oneSchema));
  }

  @Test
  void testLists_NoExistingTable() {
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

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "intList", "array<int>", 1, false),
            getCurrentGlueTableColumn(
                tableFormat,
                "recordList",
                "array<struct<requiredDouble:double,optionalString:string>>",
                2,
                true));

    assertEquals(expectedGlueColumns, GlueSchemaExtractor.toColumns(tableFormat, oneSchema));
  }

  @Test
  void testNestedRecords_NoExistingTable() {
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

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(
                tableFormat,
                "nestedOne",
                "struct<nestedOptionalInt:int,nestedRequiredDouble:double,nestedTwo:struct<doublyNestedString:string>>",
                1,
                true));
    assertEquals(expectedGlueColumns, GlueSchemaExtractor.toColumns(tableFormat, oneSchema));
  }

  @Test
  void testToColumns_NoColumnsFromExistingTable() {
    String tableFormat = TableFormat.ICEBERG;
    OneSchema oneSchema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField("optionalBoolean", "boolean", OneType.BOOLEAN, true, 2),
                    getPrimitiveOneField("requiredInt", "integer", OneType.INT, false, 3)))
            .build();

    List<Table> tableList =
        Arrays.asList(
            // table is null
            null,
            // storageDescriptor is null
            Table.builder().build(),
            // no columns present
            Table.builder().storageDescriptor(StorageDescriptor.builder().build()).build());

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "optionalBoolean", "boolean", 2, true),
            getCurrentGlueTableColumn(tableFormat, "requiredInt", "int", 3, false));

    for (Table table : tableList) {
      assertEquals(
          expectedGlueColumns, GlueSchemaExtractor.toColumns(tableFormat, oneSchema, table));
    }
  }

  @Test
  void testToColumns_ValidExistingTable() {
    String tableFormat = TableFormat.ICEBERG;
    OneSchema oneSchema =
        OneSchema.builder()
            .dataType(OneType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField("optionalBoolean", "boolean", OneType.BOOLEAN, true, 2),
                    getPrimitiveOneField("requiredInt", "integer", OneType.INT, false, 3)))
            .build();

    Table existingTable =
        Table.builder()
            .storageDescriptor(
                StorageDescriptor.builder()
                    .columns(
                        ImmutableList.of(
                            Column.builder().name("prev_x").type("string").build(),
                            Column.builder().name("prev_y").type("string").build()))
                    .build())
            .build();

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "optionalBoolean", "boolean", 2, true),
            getCurrentGlueTableColumn(tableFormat, "requiredInt", "int", 3, false),
            getPreviousGlueTableColumn(tableFormat, "prev_x", "string"),
            getPreviousGlueTableColumn(tableFormat, "prev_y", "string"));

    assertEquals(
        expectedGlueColumns, GlueSchemaExtractor.toColumns(tableFormat, oneSchema, existingTable));
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
            () -> GlueSchemaExtractor.toColumns(tableFormat, oneSchema));
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
            () -> GlueSchemaExtractor.toColumns(tableFormat, oneSchema2));
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
            () -> GlueSchemaExtractor.toColumns(tableFormat, oneSchema3));
    assertEquals("Invalid decimal type, scale is missing", exception.getMessage());
  }
}
