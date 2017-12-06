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

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class CacheWriterGetOldValueIntegrationTest {

  private GemFireCacheImpl cache = null;

  @Before
  public void setUp() throws Exception {
    createCache();
  }

  private void createCache() {
    cache =
        (GemFireCacheImpl) new CacheFactory().set("locators", "").set("mcast-port", "0").create();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    expectedValues.clear();
  }

  @Test
  public void getOldValueInCacheWriterReturnsValueOfEvictedEntry() {
    doTest(false);
  }

  @Test
  public void getOldValueWithTransactionInCacheWriterReturnsValueOfEvictedEntry() {
    doTest(true);
  }

  @Test
  public void doPutAll() {
    PutAllCacheWriter<String, String> cw;
    Region<String, String> r = createOverflowRegion();
    put(r, "k1", "v1");
    put(r, "k2", "v2");
    cw = new PutAllCacheWriter<>();
    r.getAttributesMutator().setCacheWriter(cw);
    HashMap<String, String> putAllMap = new HashMap<>();
    putAllMap.put("k1", "update1");
    putAllMap.put("k2", "update2");
    r.putAll(putAllMap);

    assertThat(cw.getSeenEntries()).isEqualTo(this.expectedValues);
  }

  @Test
  public void doRemoveAll() {
    RemoveAllCacheWriter<String, String> cw;
    Region<String, String> r = createOverflowRegion();
    put(r, "k1", "v1");
    put(r, "k2", "v2");
    cw = new RemoveAllCacheWriter<>();
    r.getAttributesMutator().setCacheWriter(cw);
    r.removeAll(Arrays.asList("k1", "k2"));

    assertThat(cw.getSeenEntries()).isEqualTo(this.expectedValues);
  }

  private void doTest(boolean useTx) {
    String evictedKey;
    String unevictedKey;
    String evictedValue;
    String unevictedValue;
    CacheWriterWithExpectedOldValue<String, String> cw;

    Region<String, String> r = createOverflowRegion();
    put(r, "k1", "v1");
    put(r, "k2", "v2");

    beginTx(useTx);
    unevictedKey = getUnevictedKey(r);
    unevictedValue = expectedValues.get(unevictedKey);
    cw = new CacheWriterWithExpectedOldValue<>(unevictedValue);
    r.getAttributesMutator().setCacheWriter(cw);
    assertThat(put(r, unevictedKey, "update1")).isEqualTo(unevictedValue);
    assertThat(cw.getUnexpectedEvents()).isEmpty();
    endTx(useTx);

    beginTx(useTx);
    evictedKey = getEvictedKey(r);
    evictedValue = expectedValues.get(evictedKey);
    cw = new CacheWriterWithExpectedOldValue<>(evictedValue);
    r.getAttributesMutator().setCacheWriter(cw);
    assertThat(put(r, evictedKey, "update2")).isEqualTo(useTx ? evictedValue : null);
    assertThat(cw.getUnexpectedEvents()).isEmpty();
    endTx(useTx);

    beginTx(useTx);
    evictedKey = getEvictedKey(r);
    evictedValue = expectedValues.get(evictedKey);
    cw = new CacheWriterWithExpectedOldValue<>(evictedValue);
    r.getAttributesMutator().setCacheWriter(cw);
    assertThat(r.destroy(evictedKey)).isEqualTo(useTx ? evictedValue : null);
    assertThat(cw.getUnexpectedEvents()).isEmpty();
    endTx(useTx);
  }

  private void beginTx(boolean useTx) {
    if (useTx) {
      this.cache.getCacheTransactionManager().begin();
    }
  }

  private void endTx(boolean useTx) {
    if (useTx) {
      this.cache.getCacheTransactionManager().commit();
    }
  }

  private final HashMap<String, String> expectedValues = new HashMap<>();

  private String put(Region<String, String> r, String k, String v) {
    String result = r.put(k, v);
    expectedValues.put(k, v);
    return result;
  }

  private String getEvictedKey(Region<String, String> r) {
    InternalRegion<String, String> ir = (InternalRegion<String, String>) r;
    String evictedKey = null;
    RegionEntry re = ir.getRegionEntry("k1");
    if (re.getValueAsToken() == null) {
      evictedKey = "k1";
    }
    re = ir.getRegionEntry("k2");
    if (re.getValueAsToken() == null) {
      evictedKey = "k2";
    }
    assertThat(evictedKey).isNotNull();
    return evictedKey;
  }

  private String getUnevictedKey(Region<String, String> r) {
    InternalRegion<String, String> ir = (InternalRegion<String, String>) r;
    String unevictedKey = null;
    RegionEntry re = ir.getRegionEntry("k1");
    if (re.getValueAsToken() != null) {
      unevictedKey = "k1";
    }
    re = ir.getRegionEntry("k2");
    if (re.getValueAsToken() != null) {
      unevictedKey = "k2";
    }
    assertThat(unevictedKey).isNotNull();
    return unevictedKey;
  }

  private static class CacheWriterWithExpectedOldValue<K, V> extends CacheWriterAdapter<K, V> {
    private final V expectedOldValue;
    private final ArrayList<EntryEvent<K, V>> unexpectedEvents = new ArrayList<>();

    CacheWriterWithExpectedOldValue(V expectedOldValue) {
      this.expectedOldValue = expectedOldValue;
    }

    public List<EntryEvent<K, V>> getUnexpectedEvents() {
      return this.unexpectedEvents;
    }

    private void checkEvent(EntryEvent<K, V> event) {
      if (this.expectedOldValue == null) {
        if (event.getOldValue() != null) {
          this.unexpectedEvents.add(event);
        }
      } else {
        if (!this.expectedOldValue.equals(event.getOldValue())) {
          this.unexpectedEvents.add(event);
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
    private final HashMap<K, V> seenEntries = new HashMap<>();

    public HashMap<K, V> getSeenEntries() {
      return this.seenEntries;
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
    private final HashMap<K, V> seenEntries = new HashMap<>();

    public HashMap<K, V> getSeenEntries() {
      return this.seenEntries;
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

  private <K, V> Region<K, V> createOverflowRegion() {
    RegionFactory<K, V> regionFactory = cache.createRegionFactory();
    regionFactory.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    regionFactory.setDataPolicy(DataPolicy.NORMAL);
    return regionFactory.create("region");
  }

}
