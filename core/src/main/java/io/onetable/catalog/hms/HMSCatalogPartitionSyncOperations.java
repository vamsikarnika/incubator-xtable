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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.exception.TableNotFoundException;

import io.onetable.catalog.CatalogPartitionSyncOperations;
import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
import io.onetable.catalog.Partition;
import io.onetable.exception.CatalogSyncException;

@Log4j2
public class HMSCatalogPartitionSyncOperations implements CatalogPartitionSyncOperations {

  private final IMetaStoreClient metaStoreClient;

  public HMSCatalogPartitionSyncOperations(IMetaStoreClient metaStoreClient) {
    this.metaStoreClient = metaStoreClient;
  }

  @Override
  public List<Partition> getAllPartitions(TableIdentifier tableIdentifier) {
    try {
      return metaStoreClient
          .listPartitions(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(), (short) -1)
          .stream()
          .map(p -> new io.onetable.catalog.Partition(p.getValues(), p.getSd().getLocation()))
          .collect(Collectors.toList());
    } catch (TException e) {
      throw new CatalogSyncException(
          "Failed to get all partitions for table " + tableIdentifier, e);
    }
  }

  @Override
  public void addPartitionsToTable(
      TableIdentifier tableIdentifier, List<Partition> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      log.info("No partitions to add for " + tableIdentifier);
      return;
    }
    log.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableIdentifier);
    try {
      StorageDescriptor sd =
          metaStoreClient
              .getTable(tableIdentifier.getDatabaseName(), tableIdentifier.getTableName())
              .getSd();
      int batchSyncPartitionNum = 1000;
      for (List<Partition> batch :
          CollectionUtils.batches(partitionsToAdd, batchSyncPartitionNum)) {
        List<org.apache.hadoop.hive.metastore.api.Partition> partitionList = new ArrayList<>();
        batch.forEach(
            partition -> {
              StorageDescriptor partitionSd = new StorageDescriptor();
              partitionSd.setCols(sd.getCols());
              partitionSd.setInputFormat(sd.getInputFormat());
              partitionSd.setOutputFormat(sd.getOutputFormat());
              partitionSd.setSerdeInfo(sd.getSerdeInfo());

              partitionSd.setLocation(partition.getStorageLocation());
              partitionList.add(
                  new org.apache.hadoop.hive.metastore.api.Partition(
                      partition.getValues(),
                      tableIdentifier.getDatabaseName(),
                      tableIdentifier.getTableName(),
                      0,
                      0,
                      partitionSd,
                      null));
            });
        metaStoreClient.add_partitions(partitionList, true, false);
        log.info("HMSDDLExecutor add a batch partitions done: " + partitionList.size());
      }
    } catch (TException e) {
      log.error("{} add partition failed", tableIdentifier, e);
      throw new CatalogSyncException(tableIdentifier + " add partition failed", e);
    }
  }

  @Override
  public void updatePartitionsToTable(
      TableIdentifier tableIdentifier, List<Partition> changedPartitions) {
    try {
      Table table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
      StorageDescriptor tableSd = table.getSd();

      List<org.apache.hadoop.hive.metastore.api.Partition> updatedPartitions = new ArrayList<>();

      changedPartitions.forEach(
          p -> {
            StorageDescriptor partitionSd = new StorageDescriptor(tableSd);
            partitionSd.setLocation(p.getStorageLocation());

            org.apache.hadoop.hive.metastore.api.Partition partition =
                new org.apache.hadoop.hive.metastore.api.Partition();
            partition.setDbName(tableIdentifier.getDatabaseName());
            partition.setTableName(tableIdentifier.getTableName());
            partition.setValues(p.getValues());
            partition.setSd(partitionSd);
            updatedPartitions.add(partition);
          });

      // Update partitions (drop existing and add new ones with updated locations)
      for (org.apache.hadoop.hive.metastore.api.Partition partition : updatedPartitions) {
        metaStoreClient.dropPartition(
            tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(),
            partition.getValues(),
            false);
        metaStoreClient.add_partition(partition);
      }
    } catch (TException e) {
      throw new CatalogSyncException(
          "Failed to update partitions for the table " + tableIdentifier, e);
    }
  }

  @Override
  public void dropPartitions(TableIdentifier tableIdentifier, List<Partition> partitionsToDrop) {
    try {
      for (Partition partition : partitionsToDrop) {
        metaStoreClient.dropPartition(
            tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(),
            partition.getValues(),
            false);
      }
    } catch (TException e) {
      throw new CatalogSyncException("Failed to drop partitions for table " + tableIdentifier, e);
    }
  }

  @Override
  public Map<String, String> getLastTimeSyncedProperties(
      TableIdentifier tableIdentifier, List<String> lastSyncedKeys) {
    try {
      Table table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
      Map<String, String> tableParameters = table.getParameters();

      return lastSyncedKeys.stream()
          .filter(tableParameters::containsKey)
          .collect(Collectors.toMap(key -> key, tableParameters::get));
    } catch (TableNotFoundException | TException e) {
      throw new CatalogSyncException(
          "failed to fetch last time synced properties for table" + tableIdentifier, e);
    }
  }

  @Override
  public void updateLastTimeSyncedProperties(
      TableIdentifier tableIdentifier, Map<String, String> lastTimeSyncedProperties) {
    try {
      if (lastTimeSyncedProperties == null || lastTimeSyncedProperties.isEmpty()) {
        return;
      }

      Table table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
      Map<String, String> tableParameters = table.getParameters();
      tableParameters.putAll(lastTimeSyncedProperties);
      table.setParameters(tableParameters);
      metaStoreClient.alter_table(
          tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(), table);
    } catch (TableNotFoundException | TException e) {
      throw new CatalogSyncException(
          "failed to update last time synced properties for table" + tableIdentifier, e);
    }
  }
}
