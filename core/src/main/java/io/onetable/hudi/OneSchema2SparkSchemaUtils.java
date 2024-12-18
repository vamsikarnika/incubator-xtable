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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.onetable.exception.SchemaExtractorException;
import io.onetable.exception.UnsupportedSchemaTypeException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;

/** Convert the one schema to spark schema */
public class OneSchema2SparkSchemaUtils {

  public static StructType convertToSparkSchema(OneSchema oneSchema) {
    if (oneSchema.getDataType() != OneType.RECORD) {
      throw new IllegalArgumentException("Root schema must be a RECORD type");
    }
    return new StructType(
        oneSchema.getFields().stream()
            .map(OneSchema2SparkSchemaUtils::convertField)
            .toArray(StructField[]::new));
  }

  private static StructField convertField(OneField oneField) {
    DataType sparkType = convertType(oneField.getSchema());
    return new StructField(
        oneField.getName(), sparkType, oneField.getSchema().isNullable(), Metadata.empty());
  }

  private static DataType convertType(OneSchema schema) {
    switch (schema.getDataType()) {
      case RECORD:
        StructField[] fields =
            schema.getFields().stream()
                .map(OneSchema2SparkSchemaUtils::convertField)
                .toArray(StructField[]::new);
        return new StructType(fields);
      case BYTES:
        return DataTypes.ByteType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
      case TIMESTAMP_NTZ:
        return DataTypes.LongType;
      case STRING:
        return DataTypes.StringType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case ENUM:
        return DataTypes.StringType; // Spark treats enums as strings
      case DATE:
        return DataTypes.DateType;
      case TIMESTAMP:
        return DataTypes.TimestampType;
      case LIST:
        OneField elementField =
            schema.getFields().stream()
                .filter(
                    field -> OneField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(field.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
        return DataTypes.createArrayType(convertType(elementField.getSchema()));
      case MAP:
        OneField valueField =
            schema.getFields().stream()
                .filter(field -> OneField.Constants.MAP_VALUE_FIELD_NAME.equals(field.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        return DataTypes.createMapType(DataTypes.StringType, convertType(valueField.getSchema()));
      case DECIMAL:
        int precision = (int) schema.getMetadata().get(OneSchema.MetadataKey.DECIMAL_PRECISION);
        int scale = (int) schema.getMetadata().get(OneSchema.MetadataKey.DECIMAL_SCALE);
        return DataTypes.createDecimalType(precision, scale);
      case FIXED:
        return DataTypes.BinaryType;
      default:
        throw new UnsupportedSchemaTypeException(
            "Encountered unhandled type during OneSchema to Avro conversion: "
                + schema.getDataType());
    }
  }
}
