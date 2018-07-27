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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.util.CacheWriterAdapter;

public class CacheWriterGetOldValueIntegrationTest {

  private final Map<String, String> expectedValues = new HashMap<>();

  private InternalCache cache;
  private Region<String, String> region;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    cache = (InternalCache) new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();

    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {temporaryFolder.getRoot()});

    DiskStore diskStore = diskStoreFactory.create(testName.getMethodName());

    RegionFactory<String, String> regionFactory = cache.createRegionFactory();
    regionFactory.setEvictionAttributes(createLRUEntryAttributes(1, OVERFLOW_TO_DISK));
    regionFactory.setDataPolicy(DataPolicy.NORMAL);
    regionFactory.setDiskStoreName(diskStore.getName());

    region = regionFactory.create(testName.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    expectedValues.clear();
  }

  @Test
  public void doPutAll() {
    put(region, "k1", "v1");
    put(region, "k2", "v2");

    PutAllCacheWriter<String, String> cacheWriter = new PutAllCacheWriter<>();
    region.getAttributesMutator().setCacheWriter(cacheWriter);

    Map<String, String> putAllMap = new HashMap<>();
    putAllMap.put("k1", "update1");
    putAllMap.put("k2", "update2");
    region.putAll(putAllMap);

    assertThat(cacheWriter.getSeenEntries()).isEqualTo(expectedValues);
  }

  @Test
  public void doRemoveAll() {
    put(region, "k1", "v1");
    put(region, "k2", "v2");

    RemoveAllCacheWriter<String, String> cacheWriter = new RemoveAllCacheWriter<>();
    region.getAttributesMutator().setCacheWriter(cacheWriter);

    region.removeAll(Arrays.asList("k1", "k2"));

    assertThat(cacheWriter.getSeenEntries()).isEqualTo(expectedValues);
  }

  @Test
  public void getOldValueInCacheWriterReturnsValueOfEvictedEntry() {
    doOldValueTest(false);
  }

  @Test
  public void getOldValueWithTransactionInCacheWriterReturnsValueOfEvictedEntry() {
    doOldValueTest(true);
  }

  private void doOldValueTest(boolean useTx) {
    put(region, "k1", "v1");
    put(region, "k2", "v2");

    beginTx(useTx);
    String unevictedKey = getUnevictedKey(region);
    String unevictedValue = expectedValues.get(unevictedKey);
    CacheWriterWithExpectedOldValue<String, String> cacheWriter =
        new CacheWriterWithExpectedOldValue<>(unevictedValue);
    region.getAttributesMutator().setCacheWriter(cacheWriter);
    assertThat(put(region, unevictedKey, "update1")).isEqualTo(unevictedValue);
    assertThat(cacheWriter.getUnexpectedEvents()).isEmpty();
    endTx(useTx);

    beginTx(useTx);
    String evictedKey = getEvictedKey(region);
    String evictedValue = expectedValues.get(evictedKey);
    cacheWriter = new CacheWriterWithExpectedOldValue<>(evictedValue);
    region.getAttributesMutator().setCacheWriter(cacheWriter);
    assertThat(put(region, evictedKey, "update2")).isEqualTo(useTx ? evictedValue : null);
    assertThat(cacheWriter.getUnexpectedEvents()).isEmpty();
    endTx(useTx);

    beginTx(useTx);
    evictedKey = getEvictedKey(region);
    evictedValue = expectedValues.get(evictedKey);
    cacheWriter = new CacheWriterWithExpectedOldValue<>(evictedValue);
    region.getAttributesMutator().setCacheWriter(cacheWriter);
    assertThat(region.destroy(evictedKey)).isEqualTo(useTx ? evictedValue : null);
    assertThat(cacheWriter.getUnexpectedEvents()).isEmpty();
    endTx(useTx);
  }

  private void beginTx(boolean useTx) {
    if (useTx) {
      cache.getCacheTransactionManager().begin();
    }
  }

  private void endTx(boolean useTx) {
    if (useTx) {
      cache.getCacheTransactionManager().commit();
    }
  }

  private String put(Map<String, String> region, String key, String value) {
    String result = region.put(key, value);
    expectedValues.put(key, value);
    return result;
  }

  private String getEvictedKey(Region<String, String> region) {
    InternalRegion internalRegion = (InternalRegion) region;
    RegionEntry regionEntry = internalRegion.getRegionEntry("k1");

    String evictedKey = null;
    if (regionEntry.getValueAsToken() == null) {
      evictedKey = "k1";
    }
    regionEntry = internalRegion.getRegionEntry("k2");
    if (regionEntry.getValueAsToken() == null) {
      evictedKey = "k2";
    }

    assertThat(evictedKey).isNotNull();
    return evictedKey;
  }

  private String getUnevictedKey(Region<String, String> region) {
    InternalRegion internalRegion = (InternalRegion) region;
    RegionEntry regionEntry = internalRegion.getRegionEntry("k1");

    String unevictedKey = null;
    if (regionEntry.getValueAsToken() != null) {
      unevictedKey = "k1";
    }
    regionEntry = internalRegion.getRegionEntry("k2");
    if (regionEntry.getValueAsToken() != null) {
      unevictedKey = "k2";
    }

    assertThat(unevictedKey).isNotNull();
    return unevictedKey;
  }

  private static class CacheWriterWithExpectedOldValue<K, V> extends CacheWriterAdapter<K, V> {
    private final V expectedOldValue;
    private final List<EntryEvent<K, V>> unexpectedEvents = new ArrayList<>();

    CacheWriterWithExpectedOldValue(V expectedOldValue) {
      this.expectedOldValue = expectedOldValue;
    }

    public List<EntryEvent<K, V>> getUnexpectedEvents() {
      return unexpectedEvents;
    }

    private void checkEvent(EntryEvent<K, V> event) {
      if (expectedOldValue == null) {
        if (event.getOldValue() != null) {
          unexpectedEvents.add(event);
        }
      } else {
        if (!expectedOldValue.equals(event.getOldValue())) {
          unexpectedEvents.add(event);
        }
      }
    }

    @Override
    public void beforeCreate(EntryEvent<K, V> event) throws CacheWriterException {
      checkEvent(event);
    }

    @Override
    public void beforeUpdate(EntryEvent<K, V> event) throws CacheWriterException {
      checkEvent(event);
    }

    @Override
    public void beforeDestroy(EntryEvent<K, V> event) throws CacheWriterException {
      checkEvent(event);
    }
  }

  private static class PutAllCacheWriter<K, V> extends CacheWriterAdapter<K, V> {
    private final Map<K, V> seenEntries = new HashMap<>();

    public Map<K, V> getSeenEntries() {
      return seenEntries;
    }

    @Override
    public void beforeCreate(EntryEvent<K, V> event) throws CacheWriterException {
      fail("did not expect beforeCreate to be called by putAll");
    }

    @Override
    public void beforeUpdate(EntryEvent<K, V> event) throws CacheWriterException {
      seenEntries.put(event.getKey(), event.getOldValue());
    }

    @Override
    public void beforeDestroy(EntryEvent<K, V> event) throws CacheWriterException {
      fail("did not expect beforeDestroy to be called by putAll");
    }
  }

  private static class RemoveAllCacheWriter<K, V> extends CacheWriterAdapter<K, V> {
    private final Map<K, V> seenEntries = new HashMap<>();

    public Map<K, V> getSeenEntries() {
      return seenEntries;
    }

    @Override
    public void beforeCreate(EntryEvent<K, V> event) throws CacheWriterException {
      fail("did not expect beforeCreate to be called by removeAll");
    }

    @Override
    public void beforeUpdate(EntryEvent<K, V> event) throws CacheWriterException {
      fail("did not expect beforeUpdate to be called by removeAll");
    }

    @Override
    public void beforeDestroy(EntryEvent<K, V> event) throws CacheWriterException {
      seenEntries.put(event.getKey(), event.getOldValue());
    }
  }
}
