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

import java.util.List;

import org.apache.hudi.metadata.HoodieTableMetadata;

import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.OneDataFile;

/** Interface responsible for Column stats extraction for Hudi. */
public interface HudiFileStatsExtractor {
  /**
   * Adds column stats and row count information to the provided stream of files.
   *
   * @param metadataTable the metadata table for the hudi table if it exists, otherwise null
   * @param files a stream of files that require column stats and row count information
   * @param schema the schema of the files (assumed to be the same for all files in stream)
   * @return a stream of files with column stats and row count information
   */
  List<OneDataFile> addStatsToFiles(
      HoodieTableMetadata metadataTable, List<OneDataFile> files, OneSchema schema);
}
