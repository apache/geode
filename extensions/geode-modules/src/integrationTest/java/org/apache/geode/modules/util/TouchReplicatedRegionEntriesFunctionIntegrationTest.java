/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.modules.util;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class TouchReplicatedRegionEntriesFunctionIntegrationTest {
  private static final String REGION_NAME = "testRegion";
  private Region<String, String> region;
  private final Map<String, Long> originalAccessedTime = new HashMap<>();

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  @Before
  public void setUp() {
    region = server.getCache()
        .<String, String>createRegionFactory(RegionShortcut.REPLICATE)
        .setStatisticsEnabled(true)
        .create(REGION_NAME);

    IntStream.range(0, 20).forEach(i -> {
      String key = "key_" + i;
      region.put(key, "value_" + i);

      Long laTime = ((InternalRegion) region).getEntry(key).getStatistics().getLastAccessedTime();
      originalAccessedTime.put(key, laTime);
    });

    FunctionService.registerFunction(new TouchReplicatedRegionEntriesFunction());
  }

  private void executeFunctionAndAssertLastAccessedTimeIsNotUpdated(Object[] args) {
    @SuppressWarnings("unchecked")
    Execution<Set, Boolean, Boolean> execution = FunctionService.onMembers().setArguments(args);
    ResultCollector collector = execution.execute(new TouchReplicatedRegionEntriesFunction());

    // Verify function results
    assertThat(collector.getResult()).isEqualTo(Collections.singletonList(Boolean.TRUE));

    // Verify lastAccessedTime changed for touched keys
    originalAccessedTime.forEach((key, savedLastAccessedTime) -> {
      Long laTime = ((InternalRegion) region).getEntry(key).getStatistics().getLastAccessedTime();
      assertThat(laTime - savedLastAccessedTime).isEqualTo(0);
    });
  }

  @Test
  public void executeShouldDoNothingWhenRegionDoesNotExist() {
    Object[] arguments = new Object[] {SEPARATOR + "nonExistingRegion", null};
    executeFunctionAndAssertLastAccessedTimeIsNotUpdated(arguments);
  }

  @Test
  public void executeShouldDoNothingWhenKeySetIsEmpty() {
    Object[] arguments = new Object[] {REGION_NAME, Collections.emptySet()};
    executeFunctionAndAssertLastAccessedTimeIsNotUpdated(arguments);
  }

  @Test
  public void executeShouldUpdateEntryLastAccessedTimeForKeysPresentInTheFilter() {
    Set<String> keysToTouch = new HashSet<>(Arrays.asList("key_1", "key_5", "key_3"));
    Object[] args = new Object[] {REGION_NAME, keysToTouch};

    // Wait until time passes.
    long currentTime = System.nanoTime();
    await().untilAsserted(() -> assertThat(System.nanoTime()).isGreaterThan(currentTime));

    // Execute function on specific keys.
    @SuppressWarnings("unchecked")
    Execution<Set, Boolean, Boolean> execution = FunctionService.onMembers().setArguments(args);
    ResultCollector collector = execution.execute(new TouchReplicatedRegionEntriesFunction());

    // Verify function results
    assertThat(collector.getResult()).isEqualTo(Collections.singletonList(Boolean.TRUE));

    // Verify lastAccessedTime changed for touched keys
    originalAccessedTime.forEach((key, savedLastAccessedTime) -> {
      Long laTime = ((InternalRegion) region).getEntry(key).getStatistics().getLastAccessedTime();

      if (!keysToTouch.contains(key)) {
        assertThat(laTime - savedLastAccessedTime).isEqualTo(0);
      } else {
        assertThat(laTime - savedLastAccessedTime).isNotEqualTo(0);
      }
    });
  }
}
