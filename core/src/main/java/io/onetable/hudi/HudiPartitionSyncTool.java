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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import io.onetable.catalog.CatalogPartitionSyncOperations;
import io.onetable.catalog.ExternalCatalogConfig;
import io.onetable.catalog.ExternalCatalogConfig.TableIdentifier;
import io.onetable.catalog.Partition;
import io.onetable.catalog.PartitionEvent;
import io.onetable.catalog.PartitionEvent.PartitionEventType;
import io.onetable.exception.CatalogSyncException;
import io.onetable.model.OneTable;

@Log4j2
public class HudiPartitionSyncTool {

  private final HoodieTableMetaClient metaClient;
  private final CatalogPartitionSyncOperations catalogClient;
  private final PartitionValueExtractor partitionValuesExtractor;

  public static final String LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";
  public static final String LAST_COMMIT_COMPLETION_TIME_SYNC = "last_commit_completion_time_sync";

  public HudiPartitionSyncTool(
      HoodieTableMetaClient metaClient,
      CatalogPartitionSyncOperations catalogClient,
      PartitionValueExtractor partitionValueExtractor) {
    this.metaClient = metaClient;
    this.catalogClient = catalogClient;
    this.partitionValuesExtractor = partitionValueExtractor;
  }

  /**
   * Syncs all partitions on storage to the metastore, by only making incremental changes.
   *
   * @param tableIdentifier The table in the metastore.
   * @return {@code true} if one or more partition(s) are changed in the metastore; {@code false}
   *     otherwise.
   */
  public boolean syncAllPartitions(
      OneTable oneTable,
      ExternalCatalogConfig.TableIdentifier tableIdentifier,
      Configuration configuration) {
    try {
      if (oneTable.getPartitioningFields().isEmpty()) {
        return false;
      }

      List<Partition> allPartitionsInMetastore = catalogClient.getAllPartitions(tableIdentifier);
      List<String> allPartitionsOnStorage =
          getAllPartitionPathsOnStorage(oneTable.getBasePath(), configuration);
      return syncPartitions(
          tableIdentifier, getPartitionEvents(allPartitionsInMetastore, allPartitionsOnStorage));
    } catch (Exception e) {
      throw new CatalogSyncException(
          "Failed to sync partitions for table " + tableIdentifier.getTableName(), e);
    }
  }

  public boolean syncPartitions(
      OneTable table,
      TableIdentifier tableIdentifier,
      Configuration configuration,
      Option<String> lastCommitTimeSynced,
      Option<String> lastCommitCompletionTimeSynced) {
    boolean partitionsChanged;
    if (!lastCommitTimeSynced.isPresent()
        || metaClient.getActiveTimeline().isBeforeTimelineStarts(lastCommitTimeSynced.get())) {
      // If the last commit time synced is before the start of the active timeline,
      // the Hive sync falls back to list all partitions on storage, instead of
      // reading active and archived timelines for written partitions.
      log.info(
          "Sync all partitions given the last commit time synced is empty or "
              + "before the start of the active timeline. Listing all partitions in "
              + table.getBasePath());
      partitionsChanged = syncAllPartitions(table, tableIdentifier, configuration);
    } else {
      List<String> writtenPartitionsSince =
          getWrittenPartitionsSince(
              table.getBasePath(),
              lastCommitTimeSynced,
              lastCommitCompletionTimeSynced,
              configuration);
      log.info("Storage partitions scan complete. Found " + writtenPartitionsSince.size());

      // Sync the partitions if needed
      // find dropped partitions, if any, in the latest commit
      Set<String> droppedPartitions =
          getDroppedPartitionsSince(lastCommitTimeSynced, lastCommitCompletionTimeSynced);
      partitionsChanged =
          syncPartitions(tableIdentifier, writtenPartitionsSince, droppedPartitions);
    }
    return partitionsChanged;
  }

  /**
   * Gets all relative partitions paths in the Hudi table on storage.
   *
   * @return All relative partitions paths.
   */
  public List<String> getAllPartitionPathsOnStorage(String basePath, Configuration configuration) {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(configuration);
    // ToDo - if we need to config to validate assumeDatePartitioning
    // ToDo - getAllPartitionPaths is not returning expected partition paths with hive style
    // partitioning with `useFileListingFromMetadata` disabled
    return FSUtils.getAllPartitionPaths(engineContext, basePath, true, false);
  }

  public List<String> getWrittenPartitionsSince(
      String basePath,
      Option<String> lastCommitTimeSynced,
      Option<String> lastCommitCompletionTimeSynced,
      Configuration configuration) {
    if (!lastCommitTimeSynced.isPresent()) {
      log.info("Last commit time synced is not known, listing all partitions in " + basePath);
      return getAllPartitionPathsOnStorage(basePath, configuration);
    } else {
      log.info(
          "Last commit time synced is "
              + lastCommitTimeSynced.get()
              + ", Getting commits since then");
      return TimelineUtils.getWrittenPartitions(
          TimelineUtils.getCommitsTimelineAfter(
              metaClient, lastCommitTimeSynced.get(), lastCommitCompletionTimeSynced));
    }
  }

  /**
   * Get the set of dropped partitions since the last synced commit. If last sync time is not known
   * then consider only active timeline. Going through archive timeline is a costly operation, and
   * it should be avoided unless some start time is given.
   */
  public Set<String> getDroppedPartitionsSince(
      Option<String> lastCommitTimeSynced, Option<String> lastCommitCompletionTimeSynced) {
    HoodieTimeline timeline =
        lastCommitTimeSynced.isPresent()
            ? TimelineUtils.getCommitsTimelineAfter(
                metaClient, lastCommitTimeSynced.get(), lastCommitCompletionTimeSynced)
            : metaClient.getActiveTimeline();
    return new HashSet<>(TimelineUtils.getDroppedPartitions(timeline));
  }

  /**
   * Syncs added, updated, and dropped partitions to the metastore.
   *
   * @param tableIdentifier The table in the metastore.
   * @param partitionEventList The partition change event list.
   * @return {@code true} if one or more partition(s) are changed in the metastore; {@code false}
   *     otherwise.
   */
  private boolean syncPartitions(
      TableIdentifier tableIdentifier, List<PartitionEvent> partitionEventList) {
    List<String> newPartitions = filterPartitions(partitionEventList, PartitionEventType.ADD);
    if (!newPartitions.isEmpty()) {
      log.info("New Partitions " + newPartitions);
      catalogClient.addPartitionsToTable(tableIdentifier, newPartitions);
    }

    List<String> updatePartitions = filterPartitions(partitionEventList, PartitionEventType.UPDATE);
    if (!updatePartitions.isEmpty()) {
      log.info("Changed Partitions " + updatePartitions);
      catalogClient.updatePartitionsToTable(tableIdentifier, updatePartitions);
    }

    List<String> dropPartitions = filterPartitions(partitionEventList, PartitionEventType.DROP);
    if (!dropPartitions.isEmpty()) {
      log.info("Drop Partitions " + dropPartitions);
      catalogClient.dropPartitions(tableIdentifier, dropPartitions);
    }

    return !updatePartitions.isEmpty() || !newPartitions.isEmpty() || !dropPartitions.isEmpty();
  }

  private List<String> filterPartitions(List<PartitionEvent> events, PartitionEventType eventType) {
    return events.stream()
        .filter(s -> s.eventType == eventType)
        .map(s -> s.storagePartition)
        .collect(Collectors.toList());
  }

  /**
   * Syncs the list of storage partitions passed in (checks if the partition is in hive, if not adds
   * it or if the partition path does not match, it updates the partition path).
   *
   * @param tableIdentifier The table name in the metastore.
   * @param writtenPartitionsSince Partitions has been added, updated, or dropped since last synced.
   * @param droppedPartitions Partitions that are dropped since last sync.
   * @return {@code true} if one or more partition(s) are changed in the metastore; {@code false}
   *     otherwise.
   */
  private boolean syncPartitions(
      TableIdentifier tableIdentifier,
      List<String> writtenPartitionsSince,
      Set<String> droppedPartitions) {
    try {
      if (writtenPartitionsSince.isEmpty()) {
        return false;
      }

      List<Partition> hivePartitions = getTablePartitions(tableIdentifier);
      return syncPartitions(
          tableIdentifier,
          getPartitionEvents(hivePartitions, writtenPartitionsSince, droppedPartitions));
    } catch (Exception e) {
      throw new CatalogSyncException(
          "Failed to sync partitions for table " + tableIdentifier.getTableName(), e);
    }
  }

  /**
   * Fetch partitions from meta service, will try to push down more filters to avoid fetching too
   * many unnecessary partitions.
   */
  private List<Partition> getTablePartitions(TableIdentifier tableIdentifier) {
    return catalogClient.getAllPartitions(tableIdentifier);
    // ToDo - fetch table partitions based on the pushdown filter configuration
  }

  /**
   * Gets the partition events for changed partitions.
   *
   * <p>This compares the list of all partitions of a table stored in the metastore and on the
   * storage: (1) Partitions exist in the metastore, but NOT the storage: drops them in the
   * metastore; (2) Partitions exist on the storage, but NOT the metastore: adds them to the
   * metastore; (3) Partitions exist in both, but the partition path is different: update them in
   * the metastore.
   *
   * @param allPartitionsInMetastore All partitions of a table stored in the metastore.
   * @param allPartitionsOnStorage All partitions of a table stored on the storage.
   * @return partition events for changed partitions.
   */
  public List<PartitionEvent> getPartitionEvents(
      List<Partition> allPartitionsInMetastore, List<String> allPartitionsOnStorage) {
    Map<String, String> paths = getPartitionValuesToPathMapping(allPartitionsInMetastore);
    Set<String> partitionsToDrop = new HashSet<>(paths.keySet());

    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : allPartitionsOnStorage) {
      Path storagePartitionPath =
          FSUtils.getPartitionPath(metaClient.getBasePathV2(), storagePartition);
      String fullStoragePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      List<String> storagePartitionValues =
          partitionValuesExtractor.extractPartitionValuesInPath(storagePartition);

      if (!storagePartitionValues.isEmpty()) {
        String storageValue = String.join(", ", storagePartitionValues);
        // Remove partitions that exist on storage from the `partitionsToDrop` set,
        // so the remaining partitions that exist in the metastore should be dropped
        partitionsToDrop.remove(storageValue);
        if (!paths.containsKey(storageValue)) {
          events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
        } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
          events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
        }
      }
    }

    partitionsToDrop.forEach(
        storageValue -> {
          String storagePath = paths.get(storageValue);
          try {
            String relativePath =
                FSUtils.getRelativePartitionPath(
                    metaClient.getBasePathV2(), new CachingPath(storagePath));
            events.add(PartitionEvent.newPartitionDropEvent(relativePath));
          } catch (IllegalArgumentException e) {
            log.error(
                "Cannot parse the path stored in the metastore, ignoring it for "
                    + "generating DROP partition event: \""
                    + storagePath
                    + "\".",
                e);
          }
        });
    return events;
  }

  public static Map<String, String> getSerdeProperties(boolean readAsOptimized, String basePath) {
    Map<String, String> serdeProperties = new HashMap<>();
    serdeProperties.put(ConfigUtils.TABLE_SERDE_PATH, basePath);
    serdeProperties.put(ConfigUtils.IS_QUERY_AS_RO_TABLE, String.valueOf(readAsOptimized));
    return serdeProperties;
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be
   * added or updated. Generate a list of PartitionEvent based on the changes required.
   */
  public List<PartitionEvent> getPartitionEvents(
      List<Partition> partitionsInMetastore,
      List<String> writtenPartitionsOnStorage,
      Set<String> droppedPartitionsOnStorage) {
    Map<String, String> paths = getPartitionValuesToPathMapping(partitionsInMetastore);

    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : writtenPartitionsOnStorage) {
      Path storagePartitionPath =
          FSUtils.getPartitionPath(metaClient.getBasePathV2(), storagePartition);
      String fullStoragePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      List<String> storagePartitionValues =
          partitionValuesExtractor.extractPartitionValuesInPath(storagePartition);

      if (droppedPartitionsOnStorage.contains(storagePartition)) {
        events.add(PartitionEvent.newPartitionDropEvent(storagePartition));
      } else {
        if (!storagePartitionValues.isEmpty()) {
          String storageValue = String.join(", ", storagePartitionValues);
          if (!paths.containsKey(storageValue)) {
            events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
          } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
            events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
          }
        }
      }
    }
    return events;
  }

  /**
   * Gets the partition values to the absolute path mapping based on the partition information from
   * the metastore.
   *
   * @param partitionsInMetastore Partitions in the metastore.
   * @return The partition values to the absolute path mapping.
   */
  private Map<String, String> getPartitionValuesToPathMapping(
      List<Partition> partitionsInMetastore) {
    Map<String, String> paths = new HashMap<>();
    for (Partition tablePartition : partitionsInMetastore) {
      List<String> hivePartitionValues = tablePartition.getValues();
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getStorageLocation()))
              .toUri()
              .getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }
    return paths;
  }
}
