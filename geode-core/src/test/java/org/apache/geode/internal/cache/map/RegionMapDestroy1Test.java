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
package org.apache.geode.internal.cache.map;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.AbstractRegionMap;
import org.apache.geode.internal.cache.AbstractRegionMapTest;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXRmtEvent;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VMLRURegionMap;
import org.apache.geode.internal.cache.entries.HashRegionEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.region.entry.RegionEntryFactoryBuilder;
import org.apache.geode.internal.util.concurrent.ConcurrentMapWithReusableEntries;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntryCreator;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionMapDestroy1Test {

  private static final Object KEY = "key";

  private LocalRegion localRegion;
  private FocusedRegionMap focusedRegionMap;
  private CacheModificationLock cacheModificationLock;
  private EntryEventImpl event;
  private RegionEntryFactory regionEntryFactory;
  private RegionEntry regionEntry;
  private ConcurrentMapWithReusableEntries entryMap;
  private HashEntryCreator hashEntryCreator;

  private boolean inTokenMode;
  private boolean duringRI;
  private boolean cacheWrite;
  private boolean isEviction;
  private Object expectedOldValue;
  private boolean removeRecoveredEntry;

  private boolean isLRU;
  private boolean isDisk;
  private boolean withVersioning;
  private boolean offHeap;

  private RegionMapDestroy regionMapDestroy;

  @Before
  public void setUp() throws Exception {
    cacheModificationLock = mock(CacheModificationLock.class);
    localRegion = mock(LocalRegion.class);
    regionEntry = mock(RegionEntry.class);
    hashEntryCreator = mock(HashEntryCreator.class);

    when(hashEntryCreator.newEntry(any(), anyInt(), any(), any()))
        .thenReturn(mock(HashRegionEntry.class));
    when(localRegion.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(localRegion.getConcurrencyChecksEnabled()).thenReturn(false);
    when(localRegion.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(localRegion.getKeyInfo(KEY)).thenReturn(new KeyInfo(KEY, null, null));
    when(regionEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);

    event =
        spy(EntryEventImpl.create(localRegion, Operation.DESTROY, KEY, false, null, true, false));

    when(event.getKey()).thenReturn(new Object());
    when(event.getLocalRegion()).thenReturn(localRegion);
    when(event.getOperation()).thenReturn(Operation.DESTROY);

    RegionMap.Attributes attributes = new RegionMap.Attributes();
    isLRU = false;
    isDisk = false;
    withVersioning = localRegion.getConcurrencyChecksEnabled();
    offHeap = false;

    entryMap = createEntryMap(attributes.initialCapacity, attributes.loadFactor,
        attributes.concurrencyLevel, hashEntryCreator);
    regionEntryFactory = new RegionEntryFactoryBuilder().create(attributes.statisticsEnabled, isLRU,
        isDisk, withVersioning, offHeap);
    focusedRegionMap =
        spy(new TestableAbstractRegionMap(localRegion, entryMap, regionEntryFactory));

    when(focusedRegionMap.getEntryFactory()).thenReturn(regionEntryFactory);
    when(focusedRegionMap.getEntryMap()).thenReturn(entryMap);

    inTokenMode = false;
    duringRI = false;
    cacheWrite = false;
    isEviction = false;
    expectedOldValue = null;
    removeRecoveredEntry = false;

    regionMapDestroy = new RegionMapDestroy(localRegion, focusedRegionMap, cacheModificationLock);
  }

  private ConcurrentMapWithReusableEntries createEntryMap(int initialCapacity, float loadFactor,
      int concurrencyLevel, HashEntryCreator hashEntryCreator) {
    return new CustomEntryConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel, false,
        hashEntryCreator);
  }

  @Test
  public void destroyWithEmptyRegionThrowsException() {
    doThrow(EntryNotFoundException.class).when(localRegion).checkEntryNotFound(any());

    assertThatThrownBy(() -> regionMapDestroy.destroy(event, inTokenMode, duringRI, cacheWrite,
        isEviction, expectedOldValue, removeRecoveredEntry))
            .isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeAddsAToken() {
    // final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    // final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    // final Object expectedOldValue = null;
    // final boolean inTokenMode = true;
    // final boolean duringRI = false;
    // doThrow(EntryNotFoundException.class).when(localRegion).checkEntryNotFound(any());

    // inTokenMode = true;

    boolean result = regionMapDestroy.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry);
    assertThat(result).isTrue();

    assertThat(focusedRegionMap.getEntryMap().containsKey(event.getKey())).isTrue();
    RegionEntry regionEntry = (RegionEntry) focusedRegionMap.getEntryMap().get(event.getKey());
    assertThat(regionEntry.getValueAsToken()).isEqualTo(Token.DESTROYED);

    // boolean result = arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue,
    // false);
    // assertThat(result).isTrue();
    // assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    // RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    // assertThat(re.getValueAsToken()).isEqualTo(Token.DESTROYED);

    boolean invokeCallbacks = true;

    verify(localRegion, times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode), eq(false),
        eq(duringRI), eq(invokeCallbacks));
    verify(localRegion, times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode), eq(duringRI),
        eq(invokeCallbacks), eq(expectedOldValue));
  }

  private EntryEventImpl createEventForDestroy(LocalRegion lr) {
    when(lr.getKeyInfo(KEY)).thenReturn(new KeyInfo(KEY, null, null));
    return EntryEventImpl.create(lr, Operation.DESTROY, KEY, false, null, true, false);
  }

  private EntryEventImpl createEventForInvalidate(LocalRegion lr) {
    when(lr.getKeyInfo(KEY)).thenReturn(new KeyInfo(KEY, null, null));
    return EntryEventImpl.create(lr, Operation.INVALIDATE, KEY, false, null, true, false);
  }

  private void addEntry(AbstractRegionMap arm) {
    addEntry(arm, "value");
  }

  private void addEntry(AbstractRegionMap arm, Object value) {
    RegionEntry entry = arm.getEntryFactory().createEntry(arm._getOwner(), KEY, value);
    arm.getEntryMap().put(KEY, entry);
  }

  private void verifyTxApplyInvalidate(TxTestableAbstractRegionMap arm, Object key, RegionEntry re,
      Token token) throws RegionClearedException {
    re.setValue(arm._getOwner(), token);
    arm.txApplyInvalidate(key, Token.INVALID, false,
        new TXId(mock(InternalDistributedMember.class), 1), mock(TXRmtEvent.class), false,
        mock(EventID.class), null, null, null, null, null, null, 1);
    assertEquals(re.getValueAsToken(), token);
  }

  /**
   * TestableAbstractRegionMap
   */
  private static class TestableAbstractRegionMap extends AbstractRegionMap {

    // protected TestableAbstractRegionMap() {
    // this(false);
    // }
    //
    // protected TestableAbstractRegionMap(boolean withConcurrencyChecks) {
    // this(withConcurrencyChecks, null, null);
    // }

    protected TestableAbstractRegionMap(InternalRegion internalRegion,
        ConcurrentMapWithReusableEntries map, RegionEntryFactory regionEntryFactory) {
      super(null);
      initialize(internalRegion, new Attributes(), null, false);
      if (map != null) {
        setEntryMap(map);
      }
      if (regionEntryFactory != null) {
        setEntryFactory(regionEntryFactory);
      }
    }

    // TestableAbstractRegionMap(InternalRegion localRegion, CustomEntryConcurrentHashMap map,
    // RegionEntryFactory factory) {
    // super(null);
    // initialize(localRegion, new Attributes(), null, false);
    // if (map != null) {
    // setEntryMap(map);
    // }
    // if (factory != null) {
    // setEntryFactory(factory);
    // }
    // }
  }

  /**
   * TestableVMLRURegionMap
   */
  private static class TestableVMLRURegionMap extends VMLRURegionMap {
    private static EvictionAttributes evictionAttributes =
        EvictionAttributes.createLRUEntryAttributes();

    protected TestableVMLRURegionMap() {
      this(false);
    }

    private static LocalRegion createOwner(boolean withConcurrencyChecks) {
      LocalRegion owner = mock(LocalRegion.class);
      CachePerfStats cachePerfStats = mock(CachePerfStats.class);
      when(owner.getCachePerfStats()).thenReturn(cachePerfStats);
      when(owner.getEvictionAttributes()).thenReturn(evictionAttributes);
      when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
      when(owner.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
      doThrow(EntryNotFoundException.class).when(owner).checkEntryNotFound(any());
      return owner;
    }

    private static EvictionController createEvictionController() {
      EvictionController result = mock(EvictionController.class);
      when(result.getEvictionAlgorithm()).thenReturn(evictionAttributes.getAlgorithm());
      EvictionCounters evictionCounters = mock(EvictionCounters.class);
      when(result.getCounters()).thenReturn(evictionCounters);
      return result;
    }

    protected TestableVMLRURegionMap(boolean withConcurrencyChecks) {
      super(createOwner(withConcurrencyChecks), new Attributes(), null, createEvictionController());
    }

    protected TestableVMLRURegionMap(boolean withConcurrencyChecks,
        ConcurrentMapWithReusableEntries hashMap) {
      this(withConcurrencyChecks);
      setEntryMap(hashMap);
    }
  }

  /**
   * TxTestableAbstractRegionMap
   */
  private static class TxTestableAbstractRegionMap extends AbstractRegionMap {

    protected TxTestableAbstractRegionMap() {
      super(null);
      LocalRegion owner = mock(LocalRegion.class);
      when(owner.getCache()).thenReturn(mock(InternalCache.class));
      when(owner.isAllEvents()).thenReturn(true);
      when(owner.isInitialized()).thenReturn(true);
      initialize(owner, new Attributes(), null, false);
    }
  }

  private static class SimpleFocusedRegionMap implements FocusedRegionMap {

    Map<Object, Object> map = new HashMap<>();

    @Override
    public RegionEntry getEntry(EntryEventImpl event) {
      return (RegionEntry) map.get(event.getKey());
    }

    @Override
    public RegionEntryFactory getEntryFactory() {
      return null;
    }

    @Override
    public RegionEntry putEntryIfAbsent(Object key, RegionEntry regionEntry) {
      return (RegionEntry) map.putIfAbsent(key, regionEntry);
    }

    @Override
    public Map<Object, Object> getEntryMap() {
      return map;
    }

    @Override
    public boolean confirmEvictionDestroy(RegionEntry regionEntry) {
      return false;
    }

    @Override
    public void lruEntryDestroy(RegionEntry regionEntry) {

    }

    @Override
    public void removeEntry(Object key, RegionEntry regionEntry, boolean updateStat) {

    }

    @Override
    public void removeEntry(Object key, RegionEntry regionEntry, boolean updateStat,
        EntryEventImpl event, InternalRegion internalRegion) {

    }

    @Override
    public void processVersionTag(RegionEntry regionEntry, EntryEventImpl event) {

    }
  }

}
