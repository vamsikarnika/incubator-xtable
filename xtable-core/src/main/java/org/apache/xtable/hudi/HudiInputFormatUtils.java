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
 
package org.apache.xtable.hudi;

import org.apache.hudi.common.model.HoodieFileFormat;

import org.apache.xtable.exception.NotSupportedException;

public class HudiInputFormatUtils {

  public static String getInputFormatClassName(HoodieFileFormat baseFileFormat, boolean realtime) {
    switch (baseFileFormat) {
      case PARQUET:
        if (realtime) {
          return "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
        } else {
          return "org.apache.hudi.hadoop.HoodieParquetInputFormat";
        }
      case HFILE:
        if (realtime) {
          return "org.apache.hudi.hadoop.realtime.HoodieHFileRealtimeInputFormat";
        } else {
          return "org.apache.hudi.hadoop.HoodieHFileInputFormat";
        }
      case ORC:
        return "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
      default:
        throw new NotSupportedException(
            "Hudi InputFormat not implemented for base file format " + baseFileFormat);
    }
  }

  public static String getOutputFormatClassName(HoodieFileFormat baseFileFormat) {
    switch (baseFileFormat) {
      case PARQUET:
      case HFILE:
        return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
      case ORC:
        return "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
      default:
        throw new NotSupportedException("No OutputFormat for base file format " + baseFileFormat);
    }
  }

  public static String getSerDeClassName(HoodieFileFormat baseFileFormat) {
    switch (baseFileFormat) {
      case PARQUET:
      case HFILE:
        return "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
      case ORC:
        return "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
      default:
        throw new NotSupportedException("No SerDe for base file format " + baseFileFormat);
    }
  }
}
