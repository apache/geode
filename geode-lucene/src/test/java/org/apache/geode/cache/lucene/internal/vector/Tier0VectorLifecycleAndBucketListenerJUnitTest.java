/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * Unit tests covering {@code PR2-B1} ({@link Tier0VectorRegistry} cleanup on index destroy,
 * region destroy, and cache close) and {@code PR2-B3} ({@link Tier0VectorBucketListener} PDX
 * read-serialized hydration and zero tombstone resurrection under concurrent {@code afterPrimary}
 * and {@code afterDestroy}).
 */
@Category({LuceneTest.class})
public class Tier0VectorLifecycleAndBucketListenerJUnitTest {

  private static final String REGION_PATH = "/vectors";
  private static final String INDEX_NAME = "vecIdx";

  private InternalCache cache;

  @Before
  public void setUp() {
    cache = Fakes.cache();
  }

  @After
  public void tearDown() {
    Tier0VectorRegistry.removeCache(cache);
  }

  @Test
  public void luceneServiceDestroyIndexRegionAndCloseCleanTier0Registry() {
    LuceneServiceImpl service = new LuceneServiceImpl();
    service.init(cache);

    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    registry.recordPut(REGION_PATH, INDEX_NAME, 0, "k1", new float[] {1f, 0f}, Map.of());
    registry.recordDestroy(REGION_PATH, INDEX_NAME, 1, "k2");
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).hasSize(2);

    // 1. Define index and destroy via destroyDefinedIndex -> must clean Tier-0 registry
    LuceneServiceImpl spyService = org.mockito.Mockito.spy(service);
    LuceneIndexImpl mockIndex = mock(LuceneIndexImpl.class);
    when(mockIndex.getName()).thenReturn(INDEX_NAME);
    when(mockIndex.getRegionPath()).thenReturn(REGION_PATH);
    org.mockito.Mockito.doReturn(mockIndex).when(spyService).getIndex(INDEX_NAME, REGION_PATH);

    spyService.destroyIndex(INDEX_NAME, REGION_PATH);
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).isEmpty();

    // 2. Repopulate two indexes on same region and call destroyIndexes(regionPath)
    registry.recordPut(REGION_PATH, INDEX_NAME, 0, "stale-1", new float[] {1f, 0f}, Map.of());
    registry.recordPut(REGION_PATH, "otherIdx", 2, "stale-2", new float[] {0f, 1f}, Map.of());
    LuceneIndexImpl mockIndex2 = mock(LuceneIndexImpl.class);
    when(mockIndex2.getName()).thenReturn("otherIdx");
    when(mockIndex2.getRegionPath()).thenReturn(REGION_PATH);
    org.mockito.Mockito.doReturn(java.util.List.of(mockIndex, mockIndex2))
        .when(spyService).getAllIndexes();

    spyService.destroyIndexes(REGION_PATH);
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).isEmpty();
    assertThat(registry.getBuckets(REGION_PATH, "otherIdx")).isEmpty();

    // 3. beforeRegionDestroyed cleans any residual region buffers
    registry.recordPut(REGION_PATH, INDEX_NAME, 3, "stale-3", new float[] {1f, 0f}, Map.of());
    org.mockito.Mockito.doReturn(Collections.emptyList()).when(spyService).getIndexes(REGION_PATH);
    Region<?, ?> mockRegion = mock(Region.class);
    when(mockRegion.getFullPath()).thenReturn(REGION_PATH);
    spyService.beforeRegionDestroyed(mockRegion);
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).isEmpty();

    // 4. LuceneServiceImpl.close() removes the entire cache registry
    registry.recordPut(REGION_PATH, INDEX_NAME, 4, "stale-4", new float[] {1f, 0f}, Map.of());
    assertThat(Tier0VectorRegistry.hasCache(cache)).isTrue();
    spyService.close();
    assertThat(Tier0VectorRegistry.hasCache(cache)).isFalse();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void afterRegionDestroyCleansUpTier0Registry() {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    registry.recordPut(REGION_PATH, INDEX_NAME, 0, "k1", new float[] {1f, 0f}, Map.of());
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).hasSize(1);

    Tier0VectorBucketListener<String, float[]> listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
    Region<String, float[]> mockRegion = mock(Region.class);
    when(mockRegion.getFullPath()).thenReturn(REGION_PATH);
    when(mockRegion.getRegionService()).thenReturn(cache);
    RegionEvent<String, float[]> event = mock(RegionEvent.class);
    when(event.getRegion()).thenReturn(mockRegion);

    listener.afterRegionDestroy(event);
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).isEmpty();
  }

  @Test
  public void afterPrimaryReadsPdxWithReadSerializedOverrideWithoutDeserializingDomainClass() {
    AtomicBoolean pdxOverrideFlag = new AtomicBoolean(false);
    doAnswer(inv -> {
      pdxOverrideFlag.set(inv.getArgument(0));
      return null;
    }).when(cache).setPdxReadSerializedOverride(anyBoolean());
    when(cache.getPdxReadSerializedOverride()).thenAnswer(inv -> pdxOverrideFlag.get());

    PdxInstance pdx = mock(PdxInstance.class);
    when(pdx.getField("embedding")).thenReturn(new float[] {0.6f, 0.8f});
    when(pdx.getField("cat")).thenReturn("news");
    // Ensure domain-class deserialization (getObject) would explode if ever called
    when(pdx.getObject()).thenThrow(new AssertionError("Must never call PdxInstance.getObject()"));

    EntrySnapshot snapshot = mock(EntrySnapshot.class);
    when(snapshot.getRawValue(true)).thenAnswer(inv -> {
      assertThat(pdxOverrideFlag.get())
          .as("pdxReadSerializedOverride must be true while reading bucket entry")
          .isTrue();
      return pdx;
    });

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.isEmpty()).thenReturn(false);
    when(bucket.keySet()).thenReturn(Collections.singleton("pdx-k1"));
    when(bucket.getEntry("pdx-k1")).thenReturn(snapshot);

    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(dataStore.getLocalBucketById(5)).thenReturn(bucket);

    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getFullPath()).thenReturn(REGION_PATH);
    when(pr.getRegionService()).thenReturn(cache);
    when(pr.getDataStore()).thenReturn(dataStore);

    Tier0VectorBucketListener<String, Object> listener = new Tier0VectorBucketListener<>(
        INDEX_NAME,
        v -> (float[]) ((PdxInstance) v).getField("embedding"),
        v -> Map.of("cat", ((PdxInstance) v).getField("cat")));
    PartitionListener partitionListener = listener.createPartitionListener();
    partitionListener.afterRegionCreate(pr);

    partitionListener.afterPrimary(5);

    // Override restored to original false value
    assertThat(pdxOverrideFlag.get()).isFalse();
    verify(pdx, never()).getObject();

    Tier0VectorDeltaBuffer<Object> buffer =
        Tier0VectorRegistry.forCache(cache).getBuffer(REGION_PATH, INDEX_NAME, 5);
    assertThat(buffer).isNotNull();
    Tier0VectorDeltaBuffer.Snapshot<Object> snap = buffer.snapshot();
    assertThat(snap.getLiveEntries()).containsOnlyKeys("pdx-k1");
    assertThat(snap.getLiveEntries().get("pdx-k1").getVector()).containsExactly(0.6f, 0.8f);
    assertThat(snap.getLiveEntries().get("pdx-k1").getMetadata()).containsEntry("cat", "news");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void afterPrimaryHydrationNeverResurrectsConcurrentlyDestroyedTombstone()
      throws Exception {
    final int bucketId = 3;
    final int numKeys = 100;
    Map<Object, float[]> backingEntries = new ConcurrentHashMap<>();
    Set<Object> allKeys = new LinkedHashSet<>();
    for (int i = 0; i < numKeys; i++) {
      String key = "k-" + i;
      backingEntries.put(key, new float[] {1f, 0f});
      allKeys.add(key);
    }

    CountDownLatch hydrationReadFirst = new CountDownLatch(1);
    CountDownLatch destroyDone = new CountDownLatch(1);

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.isEmpty()).thenReturn(false);
    when(bucket.keySet()).thenReturn(allKeys);
    when(bucket.getEntry(any())).thenAnswer(inv -> {
      Object k = inv.getArgument(0);
      float[] v = backingEntries.get(k);
      if (v == null) {
        return null;
      }
      EntrySnapshot es = mock(EntrySnapshot.class);
      when(es.getRawValue(true)).thenAnswer(rawInv -> {
        if ("k-0".equals(k)) {
          // Pause hydration mid-flight after reading k-0 so concurrent afterDestroy runs first
          hydrationReadFirst.countDown();
          destroyDone.await(5, TimeUnit.SECONDS);
        }
        return v;
      });
      return es;
    });

    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);

    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getFullPath()).thenReturn(REGION_PATH);
    when(pr.getRegionService()).thenReturn(cache);
    when(pr.getDataStore()).thenReturn(dataStore);
    KeyInfo keyInfo = mock(KeyInfo.class);
    when(keyInfo.getBucketId()).thenReturn(bucketId);
    when(pr.getKeyInfo(any(), any())).thenReturn(keyInfo);

    Tier0VectorBucketListener<String, float[]> listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
    PartitionListener partitionListener = listener.createPartitionListener();
    partitionListener.afterRegionCreate(pr);

    // Pre-mark k-1 as updated with a newer vector before afterPrimary even starts
    Tier0VectorRegistry.forCache(cache).recordPut(
        REGION_PATH, INDEX_NAME, bucketId, "k-1", new float[] {0f, 1f}, Map.of());

    Set<String> destroyedKeys = new java.util.HashSet<>();
    ExecutorService exec = Executors.newFixedThreadPool(2);
    try {
      Future<?> hydrator = exec.submit(() -> partitionListener.afterPrimary(bucketId));

      assertThat(hydrationReadFirst.await(5, TimeUnit.SECONDS)).isTrue();

      // Concurrently destroy even keys (including k-0 mid-read!) via afterDestroy
      for (int i = 0; i < numKeys; i += 2) {
        String key = "k-" + i;
        destroyedKeys.add(key);
        backingEntries.remove(key);
        EntryEvent<String, float[]> destroyEvent = mock(EntryEvent.class);
        when(destroyEvent.getRegion()).thenReturn(pr);
        when(destroyEvent.getKey()).thenReturn(key);
        listener.afterDestroy(destroyEvent);
      }
      destroyDone.countDown();

      hydrator.get(10, TimeUnit.SECONDS);
    } finally {
      exec.shutdownNow();
    }

    Tier0VectorDeltaBuffer<Object> buffer =
        Tier0VectorRegistry.forCache(cache).getBuffer(REGION_PATH, INDEX_NAME, bucketId);
    assertThat(buffer).isNotNull();
    Tier0VectorDeltaBuffer.Snapshot<Object> snap = buffer.snapshot();

    // Every concurrently destroyed key MUST remain tombstoned (zero resurrections!)
    for (String destroyed : destroyedKeys) {
      assertThat(snap.isTombstoned(destroyed))
          .as("Destroyed key %s must not be resurrected by afterPrimary", destroyed)
          .isTrue();
      assertThat(snap.getLiveEntries()).doesNotContainKey(destroyed);
    }

    // Newer live put on k-1 must NOT be overwritten by stale bucket value
    assertThat(snap.getLiveEntries().get("k-1").getVector()).containsExactly(0f, 1f);

    // Surviving odd keys were hydrated cleanly
    for (int i = 3; i < numKeys; i += 2) {
      assertThat(snap.getLiveEntries()).containsKey("k-" + i);
    }
  }
}
