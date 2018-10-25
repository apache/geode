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
package org.apache.geode.internal.cache.eviction;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.lang.SystemPropertyHelper;

public class LRUListWithAsyncSortingTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private BucketRegion bucketRegion;
  private EvictionCounters stats;
  private EvictionController controller;
  private ExecutorService executor = mock(ExecutorService.class);

  @Before
  public void setup() {
    bucketRegion = mock(BucketRegion.class);
    stats = mock(EvictionCounters.class);
    controller = mock(EvictionController.class);
    when(controller.getCounters()).thenReturn(stats);
  }

  @Test
  public void scansOnlyWhenOverThreshold() throws Exception {
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, executor, 1);
    for (int i = 0; i < 5; i++) {
      list.appendEntry(mock(EvictionNode.class));
    }

    list.incrementRecentlyUsed();
    verifyNoMoreInteractions(executor);

    list.incrementRecentlyUsed();
    verify(executor).submit(any(Runnable.class));
  }

  @Test
  public void clearResetsRecentlyUsedCounter() throws Exception {
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, executor, 1);
    list.incrementRecentlyUsed();
    assertThat(list.getRecentlyUsedCount()).isEqualTo(1);

    list.clear(null, null);
    assertThat(list.getRecentlyUsedCount()).isZero();
  }

  @Test
  public void doesNotRunScanOnEmptyList() throws Exception {
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, executor, 1);
    list.incrementRecentlyUsed();
    verifyNoMoreInteractions(executor);
  }

  @Test
  public void usesSystemPropertyThresholdIfSpecified() throws Exception {
    System.setProperty("geode." + SystemPropertyHelper.EVICTION_SCAN_THRESHOLD_PERCENT, "55");
    try {
      LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, executor, 1);

      list.appendEntry(mock(EvictionNode.class));
      list.appendEntry(mock(EvictionNode.class));
      list.incrementRecentlyUsed();
      verifyNoMoreInteractions(executor);

      list.incrementRecentlyUsed();
      verify(executor).submit(any(Runnable.class));
    } finally {
      System.clearProperty("geode." + SystemPropertyHelper.EVICTION_SCAN_THRESHOLD_PERCENT);
    }
  }

  @Test
  public void evictingFromEmptyListTest() throws Exception {
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, executor, 1);
    assertThat(list.getEvictableEntry()).isNull();
    assertThat(list.size()).isZero();
  }

  @Test
  public void evictingFromNonEmptyListTest() throws Exception {
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, executor, 1);
    EvictionNode node = mock(EvictableEntry.class);
    list.appendEntry(node);
    assertThat(list.size()).isEqualTo(1);

    when(node.next()).thenReturn(list.tail);
    when(node.previous()).thenReturn(list.head);
    assertThat(list.getEvictableEntry()).isSameAs(node);
    assertThat(list.size()).isZero();
  }

  @Test
  public void doesNotEvictNodeInTransaction() throws Exception {
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, executor, 1);
    EvictionNode nodeInTransaction = mock(EvictableEntry.class, "nodeInTransaction");
    when(nodeInTransaction.isInUseByTransaction()).thenReturn(true);
    EvictionNode nodeNotInTransaction = mock(EvictableEntry.class, "nodeNotInTransaction");
    list.appendEntry(nodeInTransaction);
    list.appendEntry(nodeNotInTransaction);
    assertThat(list.size()).isEqualTo(2);

    when(nodeInTransaction.next()).thenReturn(nodeNotInTransaction);
    when(nodeInTransaction.previous()).thenReturn(list.head);
    when(nodeNotInTransaction.next()).thenReturn(list.tail);
    when(nodeNotInTransaction.previous()).thenReturn(list.head);
    assertThat(list.getEvictableEntry()).isSameAs(nodeNotInTransaction);
    assertThat(list.size()).isZero();
  }

  @Test
  public void doesNotEvictNodeThatIsEvicted() throws Exception {
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, executor, 1);

    EvictionNode evictedNode = mock(EvictableEntry.class);
    when(evictedNode.isEvicted()).thenReturn(true);

    EvictionNode node = mock(EvictableEntry.class);
    list.appendEntry(evictedNode);
    list.appendEntry(node);
    assertThat(list.size()).isEqualTo(2);

    when(evictedNode.next()).thenReturn(node);
    when(evictedNode.previous()).thenReturn(list.head);
    when(node.next()).thenReturn(list.tail);
    when(node.previous()).thenReturn(list.head);
    assertThat(list.getEvictableEntry()).isSameAs(node);
    assertThat(list.size()).isZero();
  }

  @Test
  public void scanUnsetsRecentlyUsedOnNode() throws Exception {
    ExecutorService realExecutor = Executors.newSingleThreadExecutor();
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, realExecutor, 1);

    EvictionNode recentlyUsedNode = mock(EvictableEntry.class);
    when(recentlyUsedNode.previous()).thenReturn(list.head);
    when(recentlyUsedNode.isRecentlyUsed()).thenReturn(true);

    list.appendEntry(recentlyUsedNode);
    when(recentlyUsedNode.next()).thenReturn(list.tail);
    list.incrementRecentlyUsed();

    // unsetRecentlyUsed() is called once during scan
    await()
        .untilAsserted(() -> verify(recentlyUsedNode, times(1)).unsetRecentlyUsed());
    realExecutor.shutdown();
  }

  @Test
  public void scanEndsOnlyUpToSize() throws Exception {
    ExecutorService realExecutor = Executors.newSingleThreadExecutor();
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, realExecutor, 1);

    EvictionNode recentlyUsedNode = mock(EvictableEntry.class);
    when(recentlyUsedNode.previous()).thenReturn(list.head);
    when(recentlyUsedNode.isRecentlyUsed()).thenReturn(true);

    list.appendEntry(recentlyUsedNode);
    when(recentlyUsedNode.next()).thenReturn(recentlyUsedNode);
    list.incrementRecentlyUsed();

    // unsetRecentlyUsed() is called once during scan
    await()
        .untilAsserted(() -> verify(recentlyUsedNode, times(1)).unsetRecentlyUsed());
    realExecutor.shutdown();
  }

  @Test
  public void scanMovesRecentlyUsedNodeToTail() throws Exception {
    ExecutorService realExecutor = Executors.newSingleThreadExecutor();
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, realExecutor, 1);

    EvictionNode recentlyUsedNode = mock(EvictableEntry.class, "first");
    EvictionNode secondNode = mock(EvictableEntry.class, "second");
    EvictionNode thirdNode = mock(EvictableEntry.class, "third");

    list.appendEntry(recentlyUsedNode);
    list.appendEntry(secondNode);
    list.appendEntry(thirdNode);

    when(recentlyUsedNode.next()).thenReturn(secondNode);
    when(recentlyUsedNode.previous()).thenReturn(list.head);
    when(secondNode.next()).thenReturn(thirdNode);
    // The second node is moved to first. Its previous will be head.
    when(secondNode.previous()).thenReturn(list.head);
    when(thirdNode.next()).thenReturn(list.tail);
    when(thirdNode.previous()).thenReturn(secondNode);

    when(recentlyUsedNode.isRecentlyUsed()).thenReturn(true);
    list.incrementRecentlyUsed();

    // unsetRecentlyUsed() is called once during scan
    await()
        .untilAsserted(() -> verify(recentlyUsedNode, times(1)).unsetRecentlyUsed());
    assertThat(list.tail.previous()).isEqualTo(recentlyUsedNode);
    realExecutor.shutdown();
  }

  @Test
  public void startScanIfEvictableEntryIsRecentlyUsed() throws Exception {
    List<EvictionNode> nodes = new ArrayList<>();
    LRUListWithAsyncSorting lruEvictionList = new LRUListWithAsyncSorting(controller, executor, 1);
    IntStream.range(0, 11).forEach(i -> {
      EvictionNode node = new LRUTestEntry(i);
      nodes.add(node);
      lruEvictionList.appendEntry(node);
      node.setRecentlyUsed(mock(RegionEntryContext.class));
    });

    assertThat(lruEvictionList.getEvictableEntry().isRecentlyUsed()).isTrue();
    verify(executor).submit(any(Runnable.class));
  }

  @Test
  public void scanNotStartedIfSizeBelowMaxEvictionAttempts() {
    LRUListWithAsyncSorting list = new LRUListWithAsyncSorting(controller, executor, 2);

    EvictionNode recentlyUsedNode = mock(EvictableEntry.class, "first");
    EvictionNode secondNode = mock(EvictableEntry.class, "second");

    list.appendEntry(recentlyUsedNode);
    list.incrementRecentlyUsed();
    verifyNoMoreInteractions(executor);

    list.appendEntry(secondNode);
    list.incrementRecentlyUsed();
    verify(executor).submit(any(Runnable.class));
  }
}
