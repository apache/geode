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
package org.apache.geode.internal.cache.wan.serial;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

public class TestCacheWriterDelayWritingOfEntry<K, V> implements CacheWriter<K, V> {
  private static final String REGION_NAME = "test1";

  private final Map.Entry<Integer, Integer> entryToDelay;

  private final Map.Entry<Integer, Integer> waitUntilEntry;

  public static boolean ENTRY_CONFLICT_WINNER_HAS_REACHED_THE_REDUNDANT_SERVER;

  public TestCacheWriterDelayWritingOfEntry(Map.Entry<Integer, Integer> entryToDelay,
      Map.Entry<Integer, Integer> waitUntilEntry) {
    this.entryToDelay = entryToDelay;
    this.waitUntilEntry = waitUntilEntry;
  }

  @Override
  public void beforeUpdate(EntryEvent<K, V> event) throws CacheWriterException {
    Region<Integer, Integer> region = ClusterStartupRule.getCache().getRegion("/" + REGION_NAME);
    int value = (Integer) event.getNewValue();
    int key = (Integer) event.getKey();
    if (key == entryToDelay.getKey() && value == entryToDelay.getValue()) {
      ENTRY_CONFLICT_WINNER_HAS_REACHED_THE_REDUNDANT_SERVER = true;
      await().untilAsserted(() -> assertThat(region.get(waitUntilEntry.getKey()))
          .isEqualTo(waitUntilEntry.getValue()));
    }
  }

  @Override
  public void beforeCreate(EntryEvent<K, V> event) throws CacheWriterException {
    Region<Integer, Integer> region = ClusterStartupRule.getCache().getRegion("/" + REGION_NAME);
    int value = (Integer) event.getNewValue();
    int key = (Integer) event.getKey();
    if (key == entryToDelay.getKey() && value == entryToDelay.getValue()) {
      ENTRY_CONFLICT_WINNER_HAS_REACHED_THE_REDUNDANT_SERVER = true;
      await().untilAsserted(() -> assertThat(region.get(waitUntilEntry.getKey()))
          .isEqualTo(waitUntilEntry.getValue()));
    }
  }

  @Override
  public void beforeDestroy(EntryEvent<K, V> event) throws CacheWriterException {

  }

  @Override
  public void beforeRegionDestroy(RegionEvent<K, V> event) throws CacheWriterException {

  }

  @Override
  public void beforeRegionClear(RegionEvent<K, V> event) throws CacheWriterException {

  }
}
