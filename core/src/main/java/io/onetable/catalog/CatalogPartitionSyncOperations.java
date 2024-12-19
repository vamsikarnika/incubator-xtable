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
 
package io.onetable.catalog;

import java.util.List;

import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;

/**
 * Defines operations for managing partitions in an external catalog.
 *
 * <p>This interface provides methods to perform CRUD (Create, Read, Update, Delete) operations on
 * partitions associated with a table in an external catalog system.
 */
public interface CatalogPartitionSyncOperations {

  /**
   * Retrieves all partitions associated with the specified table.
   *
   * @param tableIdentifier an object identifying the table whose partitions are to be fetched.
   * @return a list of {@link Partition} objects representing all partitions of the specified table.
   */
  List<Partition> getAllPartitions(TableIdentifier tableIdentifier);

  /**
   * Adds new partitions to the specified table in the catalog.
   *
   * @param tableIdentifier an object identifying the table where partitions are to be added.
   * @param partitionsToAdd a list of partition paths (as strings) to be added to the table.
   */
  void addPartitionsToTable(TableIdentifier tableIdentifier, List<String> partitionsToAdd);

  /**
   * Updates the specified partitions for a table in the catalog.
   *
   * @param tableIdentifier an object identifying the table whose partitions are to be updated.
   * @param changedPartitions a list of partition paths (as strings) to be updated in the table.
   */
  void updatePartitionsToTable(TableIdentifier tableIdentifier, List<String> changedPartitions);

  /**
   * Removes the specified partitions from a table in the catalog.
   *
   * @param tableIdentifier an object identifying the table from which partitions are to be dropped.
   * @param partitionsToDrop a list of partition paths (as strings) to be removed from the table.
   */
  void dropPartitions(TableIdentifier tableIdentifier, List<String> partitionsToDrop);
}
