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
 
package io.onetable.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOneTableMetadata {
  static Stream<Arguments> parsingCases() {
    Map<String, String> fullMetadata = new HashMap<>();
    fullMetadata.put(OneTableMetadata.ONETABLE_LAST_INSTANT_SYNCED_PROP, "2024-01-01T00:00:00Z");
    fullMetadata.put(
        OneTableMetadata.INFLIGHT_COMMITS_TO_CONSIDER_FOR_NEXT_SYNC_PROP,
        "2024-01-02T00:00:00Z,2024-01-03T00:00:00Z");
    fullMetadata.put("foo", "bar");
    return Stream.of(
        Arguments.of(Collections.emptyMap(), Optional.empty()), // empty map
        Arguments.of(null, Optional.empty()), // null map
        Arguments.of(
            Collections.singletonMap("foo", "bar"), Optional.empty()), // irrelevant metadata in map
        Arguments.of(
            Collections.singletonMap(
                OneTableMetadata.ONETABLE_LAST_INSTANT_SYNCED_PROP, "2024-01-01T00:00:00Z"),
            Optional.of(
                OneTableMetadata.of(
                    Instant.parse("2024-01-01T00:00:00Z"),
                    Collections.emptyList()))), // missing instantsToConsiderForNextSync
        Arguments.of(
            fullMetadata,
            Optional.of(
                OneTableMetadata.of(
                    Instant.parse("2024-01-01T00:00:00Z"),
                    Arrays.asList(
                        Instant.parse("2024-01-02T00:00:00Z"),
                        Instant.parse("2024-01-03T00:00:00Z"))))) // all fields set
        );
  }

  @ParameterizedTest
  @MethodSource("parsingCases")
  void parseFromMap(Map<String, String> input, Optional<OneTableMetadata> expected) {
    assertEquals(expected, OneTableMetadata.fromMap(input));
  }
}
