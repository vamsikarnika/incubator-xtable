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
 
package io.onetable.client;

import static io.onetable.GenericTable.getTableName;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;

import io.onetable.model.CommitsBacklog;
import io.onetable.model.IncrementalTableChanges;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.TableChange;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;
import io.onetable.model.sync.SyncResult;
import io.onetable.spi.extractor.SourceClient;
import io.onetable.spi.sync.TableFormatSync;
import io.onetable.spi.sync.TargetClient;

public class TestOneTableClient extends CatalogSyncClientTestBase {

  private final SourceClientProvider<Instant> mockSourceClientProvider =
      mock(SourceClientProvider.class);
  private final SourceClient<Instant> mockSourceClient = mock(SourceClient.class);
  private final TableFormatClientFactory mockTableFormatClientFactory =
      mock(TableFormatClientFactory.class);
  private final TableFormatSync tableFormatSync = mock(TableFormatSync.class);
  private final TargetClient mockTargetClient1 = mock(TargetClient.class);
  private final TargetClient mockTargetClient2 = mock(TargetClient.class);
  private final ExecutorService mockExecutorService = mock(ExecutorService.class);

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testAllSnapshotSyncAsPerConfig(boolean isExternalCatalogSyncEnabled) {
    SyncMode syncMode = SyncMode.FULL;
    OneTable oneTable = getOneTable();
    OneSnapshot oneSnapshot = buildOneSnapshot(oneTable, "v1");
    Instant instantBeforeHour = Instant.now().minus(Duration.ofHours(1));
    SyncResult syncResult = buildSyncResult(syncMode, instantBeforeHour);
    Map<String, SyncResult> perTableResults = new HashMap<>();
    perTableResults.put(TableFormat.ICEBERG, syncResult);
    perTableResults.put(TableFormat.DELTA, syncResult);
    if (isExternalCatalogSyncEnabled) {
      mockCreateIcebergCatalogClient();
    }
    PerTableConfig perTableConfig =
        getPerTableConfig(
            Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA),
            syncMode,
            isExternalCatalogSyncEnabled);
    mockTableFormatSnapshotSync(
        Arrays.asList(
            getSyncClients(mockTargetClient1, TableFormat.ICEBERG, isExternalCatalogSyncEnabled),
            getSyncClients(mockTargetClient2, TableFormat.DELTA, isExternalCatalogSyncEnabled)),
        oneSnapshot,
        perTableResults);
    when(mockSourceClientProvider.getSourceClientInstance(perTableConfig, mockExecutorService))
        .thenReturn(mockSourceClient);
    when(mockSourceClient.getCurrentSnapshot()).thenReturn(oneSnapshot);
    mockCreateTargetClientForTableFormat(TableFormat.ICEBERG, perTableConfig, mockTargetClient1);
    mockCreateTargetClientForTableFormat(TableFormat.DELTA, perTableConfig, mockTargetClient2);

    OneTableClient oneTableClient =
        new OneTableClient(
            mockConf,
            mockTableFormatClientFactory,
            mockCatalogClientFactory,
            tableFormatSync,
            mockExecutorService);
    Map<String, SyncResult> result = oneTableClient.sync(perTableConfig, mockSourceClientProvider);
    assertEquals(perTableResults, result);

    // verify iceberg catalog sync client interactions
    verifyCreateGlueCatalogClientsForIcebergFormat(isExternalCatalogSyncEnabled);
    verifyCreateHmsCatalogClientsForIcebergFormat(isExternalCatalogSyncEnabled);
    verifyCreateGlueCatalogClientsForDeltaFormat();
    verifyCreateHmsCatalogClientsForDeltaFormat();
    verifyCatalogSyncClientsClose(isExternalCatalogSyncEnabled);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testAllIncrementalSyncAsPerConfigAndNoFallbackNecessary(
      boolean isExternalCatalogSyncEnabled) {
    SyncMode syncMode = SyncMode.INCREMENTAL;
    Instant instantAsOfNow = Instant.now();
    Instant instantAt15 = getInstantAtLastNMinutes(instantAsOfNow, 15);
    Instant instantAt14 = getInstantAtLastNMinutes(instantAsOfNow, 14);
    Instant instantAt10 = getInstantAtLastNMinutes(instantAsOfNow, 10);
    Instant instantAt8 = getInstantAtLastNMinutes(instantAsOfNow, 8);
    Instant instantAt5 = getInstantAtLastNMinutes(instantAsOfNow, 5);
    Instant instantAt2 = getInstantAtLastNMinutes(instantAsOfNow, 2);
    when(mockSourceClient.isIncrementalSyncSafeFrom(eq(instantAt15))).thenReturn(true);
    when(mockSourceClient.isIncrementalSyncSafeFrom(eq(instantAt8))).thenReturn(true);

    // Iceberg last synced at instantAt10 and has pending instants at instantAt15.
    Instant icebergLastSyncInstant = instantAt10;
    List<Instant> pendingInstantsForIceberg = Collections.singletonList(instantAt15);
    // Delta last synced at instantAt5 and has pending instants at instantAt8.
    Instant deltaLastSyncInstant = instantAt5;
    List<Instant> pendingInstantsForDelta = Collections.singletonList(instantAt8);
    List<Instant> combinedPendingInstants =
        Stream.concat(pendingInstantsForIceberg.stream(), pendingInstantsForDelta.stream())
            .collect(Collectors.toList());
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(icebergLastSyncInstant)
            .pendingCommits(combinedPendingInstants)
            .build();
    List<Instant> instantsToProcess =
        Arrays.asList(instantAt15, instantAt14, instantAt10, instantAt8, instantAt5, instantAt2);
    CommitsBacklog<Instant> commitsBacklog =
        CommitsBacklog.<Instant>builder().commitsToProcess(instantsToProcess).build();
    Optional<OneTableMetadata> targetClient1Metadata =
        Optional.of(OneTableMetadata.of(icebergLastSyncInstant, pendingInstantsForIceberg));
    when(mockTargetClient1.getTableMetadata()).thenReturn(targetClient1Metadata);
    Optional<OneTableMetadata> targetClient2Metadata =
        Optional.of(OneTableMetadata.of(deltaLastSyncInstant, pendingInstantsForDelta));
    when(mockTargetClient2.getTableMetadata()).thenReturn(targetClient2Metadata);
    when(mockSourceClient.getCommitsBacklog(instantsForIncrementalSync)).thenReturn(commitsBacklog);
    List<TableChange> tableChanges = new ArrayList<>();
    for (Instant instant : instantsToProcess) {
      TableChange tableChange = getTableChange(instant);
      tableChanges.add(tableChange);
      when(mockSourceClient.getTableChangeForCommit(instant)).thenReturn(tableChange);
    }
    // Iceberg needs to sync last pending instant at instantAt15 and instants after last sync
    // instant
    // which is instantAt10 and so i.e. instantAt8, instantAt5, instantAt2.
    List<SyncResult> icebergSyncResults =
        buildSyncResults(Arrays.asList(instantAt15, instantAt8, instantAt5, instantAt2));
    // Delta needs to sync last pending instant at instantAt8 and instants after last sync instant
    // which is instantAt5 and so i.e. instantAt2.
    List<SyncResult> deltaSyncResults = buildSyncResults(Arrays.asList(instantAt8, instantAt2));
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder().tableChanges(tableChanges.iterator()).build();
    Map<String, List<SyncResult>> allResults = new HashMap<>();
    allResults.put(TableFormat.ICEBERG, icebergSyncResults);
    allResults.put(TableFormat.DELTA, deltaSyncResults);
    Map<TableFormatSync.TableSyncClients, OneTableMetadata> clientToMetadata = new HashMap<>();
    clientToMetadata.put(
        getSyncClients(mockTargetClient1, TableFormat.ICEBERG, isExternalCatalogSyncEnabled),
        targetClient1Metadata.get());
    clientToMetadata.put(
        getSyncClients(mockTargetClient2, TableFormat.DELTA, isExternalCatalogSyncEnabled),
        targetClient2Metadata.get());
    if (isExternalCatalogSyncEnabled) {
      mockCreateIcebergCatalogClient();
    }
    PerTableConfig perTableConfig =
        getPerTableConfig(
            Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA),
            syncMode,
            isExternalCatalogSyncEnabled);
    mockTableFormatIncrementalSync(clientToMetadata, incrementalTableChanges, allResults);
    when(mockSourceClientProvider.getSourceClientInstance(perTableConfig, mockExecutorService))
        .thenReturn(mockSourceClient);
    mockCreateTargetClientForTableFormat(TableFormat.ICEBERG, perTableConfig, mockTargetClient1);
    mockCreateTargetClientForTableFormat(TableFormat.DELTA, perTableConfig, mockTargetClient2);

    Map<String, SyncResult> expectedSyncResult = new HashMap<>();
    expectedSyncResult.put(TableFormat.ICEBERG, getLastSyncResult(icebergSyncResults));
    expectedSyncResult.put(TableFormat.DELTA, getLastSyncResult(deltaSyncResults));
    OneTableClient oneTableClient =
        new OneTableClient(
            mockConf,
            mockTableFormatClientFactory,
            mockCatalogClientFactory,
            tableFormatSync,
            mockExecutorService);
    Map<String, SyncResult> result = oneTableClient.sync(perTableConfig, mockSourceClientProvider);
    assertEquals(expectedSyncResult, result);

    // verify iceberg catalog sync client interactions
    verifyCreateGlueCatalogClientsForIcebergFormat(isExternalCatalogSyncEnabled);
    verifyCreateHmsCatalogClientsForIcebergFormat(isExternalCatalogSyncEnabled);
    verifyCreateGlueCatalogClientsForDeltaFormat();
    verifyCreateHmsCatalogClientsForDeltaFormat();
    verifyCatalogSyncClientsClose(isExternalCatalogSyncEnabled);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testIncrementalSyncFallBackToSnapshotForAllFormats(boolean isExternalCatalogSyncEnabled) {
    SyncMode syncMode = SyncMode.INCREMENTAL;
    OneTable oneTable = getOneTable();
    Instant instantBeforeHour = Instant.now().minus(Duration.ofHours(1));
    OneSnapshot oneSnapshot = buildOneSnapshot(oneTable, "v1");
    SyncResult syncResult = buildSyncResult(syncMode, instantBeforeHour);
    Map<String, SyncResult> syncResults = new HashMap<>();
    syncResults.put(TableFormat.ICEBERG, syncResult);
    syncResults.put(TableFormat.DELTA, syncResult);
    if (isExternalCatalogSyncEnabled) {
      mockCreateIcebergCatalogClient();
    }
    PerTableConfig perTableConfig =
        getPerTableConfig(
            Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA),
            syncMode,
            isExternalCatalogSyncEnabled);
    mockTableFormatSnapshotSync(
        Arrays.asList(
            getSyncClients(mockTargetClient1, TableFormat.ICEBERG, isExternalCatalogSyncEnabled),
            getSyncClients(mockTargetClient2, TableFormat.DELTA, isExternalCatalogSyncEnabled)),
        oneSnapshot,
        syncResults);
    when(mockSourceClientProvider.getSourceClientInstance(perTableConfig, mockExecutorService))
        .thenReturn(mockSourceClient);
    mockCreateTargetClientForTableFormat(TableFormat.ICEBERG, perTableConfig, mockTargetClient1);
    mockCreateTargetClientForTableFormat(TableFormat.DELTA, perTableConfig, mockTargetClient2);

    Instant instantAsOfNow = Instant.now();
    Instant instantAt5 = getInstantAtLastNMinutes(instantAsOfNow, 5);
    when(mockSourceClient.isIncrementalSyncSafeFrom(eq(instantAt5))).thenReturn(false);

    // Both Iceberg and Delta last synced at instantAt5 and have no pending instants.
    when(mockTargetClient1.getTableMetadata())
        .thenReturn(Optional.of(OneTableMetadata.of(instantAt5, Collections.emptyList())));
    when(mockTargetClient2.getTableMetadata())
        .thenReturn(Optional.of(OneTableMetadata.of(instantAt5, Collections.emptyList())));

    when(mockSourceClient.getCurrentSnapshot()).thenReturn(oneSnapshot);
    OneTableClient oneTableClient =
        new OneTableClient(
            mockConf,
            mockTableFormatClientFactory,
            mockCatalogClientFactory,
            tableFormatSync,
            mockExecutorService);
    Map<String, SyncResult> result = oneTableClient.sync(perTableConfig, mockSourceClientProvider);
    assertEquals(syncResults, result);

    // verify iceberg catalog sync client interactions
    verifyCreateGlueCatalogClientsForIcebergFormat(isExternalCatalogSyncEnabled);
    verifyCreateHmsCatalogClientsForIcebergFormat(isExternalCatalogSyncEnabled);
    verifyCreateGlueCatalogClientsForDeltaFormat();
    verifyCreateHmsCatalogClientsForDeltaFormat();
    verifyCatalogSyncClientsClose(isExternalCatalogSyncEnabled);
  }

  @Test
  void testIncrementalSyncFallbackToSnapshotForOnlySingleFormat() {
    SyncMode syncMode = SyncMode.INCREMENTAL;
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA), syncMode);
    when(mockSourceClientProvider.getSourceClientInstance(perTableConfig, mockExecutorService))
        .thenReturn(mockSourceClient);
    mockCreateTargetClientForTableFormat(TableFormat.ICEBERG, perTableConfig, mockTargetClient1);
    mockCreateTargetClientForTableFormat(TableFormat.DELTA, perTableConfig, mockTargetClient2);
    mockCreateIcebergCatalogClient();

    Instant instantAsOfNow = Instant.now();
    Instant instantAt15 = getInstantAtLastNMinutes(instantAsOfNow, 15);
    Instant instantAt10 = getInstantAtLastNMinutes(instantAsOfNow, 10);
    Instant instantAt8 = getInstantAtLastNMinutes(instantAsOfNow, 8);
    Instant instantAt5 = getInstantAtLastNMinutes(instantAsOfNow, 5);
    Instant instantAt2 = getInstantAtLastNMinutes(instantAsOfNow, 2);

    when(mockSourceClient.isIncrementalSyncSafeFrom(eq(instantAt8))).thenReturn(true);
    when(mockSourceClient.isIncrementalSyncSafeFrom(eq(instantAt15))).thenReturn(false);

    // Iceberg last synced at instantAt10 and has pending instants at instantAt15.
    Instant icebergLastSyncInstant = instantAt10;
    List<Instant> pendingInstantsForIceberg = Collections.singletonList(instantAt15);
    // Delta last synced at instantAt5 and has pending instants at instantAt8.
    Instant deltaLastSyncInstant = instantAt5;
    List<Instant> pendingInstantsForDelta = Collections.singletonList(instantAt8);
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(deltaLastSyncInstant)
            .pendingCommits(pendingInstantsForDelta)
            .build();
    List<Instant> instantsToProcess = Arrays.asList(instantAt8, instantAt2);
    CommitsBacklog<Instant> commitsBacklog =
        CommitsBacklog.<Instant>builder().commitsToProcess(instantsToProcess).build();
    when(mockTargetClient1.getTableMetadata())
        .thenReturn(
            Optional.of(OneTableMetadata.of(icebergLastSyncInstant, pendingInstantsForIceberg)));
    Optional<OneTableMetadata> targetClient2Metadata =
        Optional.of(OneTableMetadata.of(deltaLastSyncInstant, pendingInstantsForDelta));
    when(mockTargetClient2.getTableMetadata()).thenReturn(targetClient2Metadata);
    when(mockSourceClient.getCommitsBacklog(instantsForIncrementalSync)).thenReturn(commitsBacklog);
    List<TableChange> tableChanges = new ArrayList<>();
    for (Instant instant : instantsToProcess) {
      TableChange tableChange = getTableChange(instant);
      tableChanges.add(tableChange);
      when(mockSourceClient.getTableChangeForCommit(instant)).thenReturn(tableChange);
    }
    // Iceberg needs to sync by snapshot since instant15 is affected by table clean-up.
    OneTable oneTable = getOneTable();
    Instant instantBeforeHour = Instant.now().minus(Duration.ofHours(1));
    OneSnapshot oneSnapshot = buildOneSnapshot(oneTable, "v1");
    SyncResult syncResult = buildSyncResult(syncMode, instantBeforeHour);
    Map<String, SyncResult> snapshotResult =
        Collections.singletonMap(TableFormat.ICEBERG, syncResult);
    when(mockSourceClient.getCurrentSnapshot()).thenReturn(oneSnapshot);

    mockTableFormatSnapshotSync(
        Collections.singletonList(getSyncClients(mockTargetClient1, TableFormat.ICEBERG)),
        oneSnapshot,
        snapshotResult);
    // Delta needs to sync last pending instant at instantAt8 and instants after last sync instant
    // which is instantAt5 and so i.e. instantAt2.
    List<SyncResult> deltaSyncResults = buildSyncResults(Arrays.asList(instantAt8, instantAt2));
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder().tableChanges(tableChanges.iterator()).build();
    mockTableFormatIncrementalSync(
        Collections.singletonMap(
            getSyncClients(mockTargetClient2, TableFormat.DELTA), targetClient2Metadata.get()),
        incrementalTableChanges,
        Collections.singletonMap(TableFormat.DELTA, deltaSyncResults));
    Map<String, SyncResult> expectedSyncResult = new HashMap<>();
    expectedSyncResult.put(TableFormat.ICEBERG, syncResult);
    expectedSyncResult.put(TableFormat.DELTA, getLastSyncResult(deltaSyncResults));
    OneTableClient oneTableClient =
        new OneTableClient(
            mockConf,
            mockTableFormatClientFactory,
            mockCatalogClientFactory,
            tableFormatSync,
            mockExecutorService);
    Map<String, SyncResult> result = oneTableClient.sync(perTableConfig, mockSourceClientProvider);
    assertEquals(expectedSyncResult, result);
  }

  @Test
  void incrementalSyncWithNoPendingInstantsForAllFormats() {
    SyncMode syncMode = SyncMode.INCREMENTAL;
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA), syncMode);
    when(mockSourceClientProvider.getSourceClientInstance(perTableConfig, mockExecutorService))
        .thenReturn(mockSourceClient);
    mockCreateTargetClientForTableFormat(TableFormat.ICEBERG, perTableConfig, mockTargetClient1);
    mockCreateTargetClientForTableFormat(TableFormat.DELTA, perTableConfig, mockTargetClient2);
    mockCreateIcebergCatalogClient();

    Instant instantAsOfNow = Instant.now();
    Instant instantAt5 = getInstantAtLastNMinutes(instantAsOfNow, 5);

    when(mockSourceClient.isIncrementalSyncSafeFrom(eq(instantAt5))).thenReturn(true);
    when(mockSourceClient.isIncrementalSyncSafeFrom(eq(instantAt5))).thenReturn(true);

    // Iceberg last synced at instantAt5, the last instant in the source
    Instant icebergLastSyncInstant = instantAt5;
    // Delta last synced at instantAt10
    Instant deltaLastSyncInstant = instantAt5;
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder().lastSyncInstant(deltaLastSyncInstant).build();
    List<Instant> instantsToProcess = Collections.emptyList();
    CommitsBacklog<Instant> commitsBacklog =
        CommitsBacklog.<Instant>builder().commitsToProcess(instantsToProcess).build();
    Optional<OneTableMetadata> targetClient1Metadata =
        Optional.of(OneTableMetadata.of(icebergLastSyncInstant, Collections.emptyList()));
    when(mockTargetClient1.getTableMetadata()).thenReturn(targetClient1Metadata);
    Optional<OneTableMetadata> targetClient2Metadata =
        Optional.of(OneTableMetadata.of(deltaLastSyncInstant, Collections.emptyList()));
    when(mockTargetClient2.getTableMetadata()).thenReturn(targetClient2Metadata);
    when(mockSourceClient.getCommitsBacklog(instantsForIncrementalSync)).thenReturn(commitsBacklog);
    Map<TableFormatSync.TableSyncClients, OneTableMetadata> clientsWithMetadata = new HashMap<>();
    clientsWithMetadata.put(
        getSyncClients(mockTargetClient1, TableFormat.ICEBERG), targetClient1Metadata.get());
    clientsWithMetadata.put(
        getSyncClients(mockTargetClient2, TableFormat.DELTA), targetClient2Metadata.get());
    mockTableFormatIncrementalSync(
        clientsWithMetadata,
        IncrementalTableChanges.builder()
            .tableChanges(Collections.<TableChange>emptyList().iterator())
            .build(),
        Collections.emptyMap());
    // Iceberg and Delta have no commits to sync
    Map<String, SyncResult> expectedSyncResult = Collections.emptyMap();
    OneTableClient oneTableClient =
        new OneTableClient(
            mockConf,
            mockTableFormatClientFactory,
            mockCatalogClientFactory,
            tableFormatSync,
            mockExecutorService);
    Map<String, SyncResult> result = oneTableClient.sync(perTableConfig, mockSourceClientProvider);
    assertEquals(expectedSyncResult, result);
  }

  private SyncResult getLastSyncResult(List<SyncResult> syncResults) {
    return syncResults.get(syncResults.size() - 1);
  }

  private List<SyncResult> buildSyncResults(List<Instant> instantList) {
    return instantList.stream()
        .map(instant -> buildSyncResult(SyncMode.INCREMENTAL, instant))
        .collect(Collectors.toList());
  }

  private TableChange getTableChange(Instant instant) {
    return TableChange.builder().tableAsOfChange(getOneTable(instant)).build();
  }

  private SyncResult buildSyncResult(SyncMode syncMode, Instant lastSyncedInstant) {
    return SyncResult.builder()
        .mode(syncMode)
        .lastInstantSynced(lastSyncedInstant)
        .status(SyncResult.SyncStatus.SUCCESS)
        .build();
  }

  private OneSnapshot buildOneSnapshot(OneTable oneTable, String version) {
    return OneSnapshot.builder().table(oneTable).version(version).build();
  }

  private OneTable getOneTable() {
    return getOneTable(Instant.now());
  }

  private OneTable getOneTable(Instant instant) {
    return OneTable.builder().name("some_table").latestCommitTime(instant).build();
  }

  private Instant getInstantAtLastNMinutes(Instant currentInstant, int n) {
    return Instant.now().minus(Duration.ofMinutes(n));
  }

  private PerTableConfig getPerTableConfig(List<String> targetTableFormats, SyncMode syncMode) {
    return getPerTableConfig(targetTableFormats, syncMode, true);
  }

  private PerTableConfig getPerTableConfig(
      List<String> targetTableFormats, SyncMode syncMode, boolean isExternalCatalogSyncEnabled) {
    PerTableConfig.PerTableConfigBuilder builder =
        PerTableConfig.builder()
            .tableName(getTableName())
            .tableBasePath("/tmp/doesnt/matter")
            .targetTableFormats(targetTableFormats)
            .syncMode(syncMode);
    if (isExternalCatalogSyncEnabled) {
      builder.externalCatalogConfigs(
          Arrays.asList(
              mockGlueCatalogConfig1,
              mockGlueCatalogConfig2,
              mockHmsCatalogConfig1,
              mockHmsCatalogConfig2));
    }
    return builder.build();
  }

  private void mockTableFormatIncrementalSync(
      Map<TableFormatSync.TableSyncClients, OneTableMetadata> clientWithMetadata,
      IncrementalTableChanges changes,
      Map<String, List<SyncResult>> syncResult) {
    when(tableFormatSync.syncChanges(eq(clientWithMetadata), argThat(matches(changes))))
        .thenReturn(syncResult);
  }

  private void mockTableFormatSnapshotSync(
      Collection<TableFormatSync.TableSyncClients> tableSyncClients,
      OneSnapshot snapshot,
      Map<String, SyncResult> syncResult) {
    when(tableFormatSync.syncSnapshot(
            argThat(arg -> arg.containsAll(tableSyncClients)), eq(snapshot)))
        .thenReturn(syncResult);
  }

  private void mockCreateTargetClientForTableFormat(
      String tableFormat, PerTableConfig perTableConfig, TargetClient targetClient) {
    when(mockTableFormatClientFactory.createForFormat(
            tableFormat, perTableConfig, mockConf, mockExecutorService))
        .thenReturn(targetClient);
  }

  private TableFormatSync.TableSyncClients getSyncClients(
      TargetClient targetClient, String tableFormat, boolean isExternalCatalogSyncEnabled) {
    return new TableFormatSync.TableSyncClients(
        targetClient,
        getMockCatalogSyncClientsForFormat(tableFormat, isExternalCatalogSyncEnabled));
  }

  private TableFormatSync.TableSyncClients getSyncClients(
      TargetClient targetClient, String tableFormat) {
    return getSyncClients(targetClient, tableFormat, true);
  }

  private static <T> ArgumentMatcher<Collection<T>> containsAll(Collection<T> expected) {
    return actual -> actual.size() == expected.size() && actual.containsAll(expected);
  }

  private static ArgumentMatcher<IncrementalTableChanges> matches(
      IncrementalTableChanges expected) {
    return actual ->
        actual.getPendingCommits().equals(expected.getPendingCommits())
            && iteratorsMatch(actual.getTableChanges(), expected.getTableChanges());
  }

  private static <T> boolean iteratorsMatch(Iterator<T> first, Iterator<T> second) {
    while (first.hasNext() && second.hasNext()) {
      if (!first.next().equals(second.next())) {
        return false;
      }
    }
    return !first.hasNext() && !second.hasNext();
  }
}
