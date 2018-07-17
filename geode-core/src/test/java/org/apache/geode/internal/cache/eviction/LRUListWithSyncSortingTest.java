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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.ExpectedException;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.lang.SystemPropertyHelper;

public class LRUListWithSyncSortingTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private BucketRegion bucketRegion;
  private EvictionCounters stats;
  private EvictionController controller;

  @Before
  public void setup() {
    bucketRegion = mock(BucketRegion.class);
    stats = mock(EvictionCounters.class);
    controller = mock(EvictionController.class);
    when(controller.getCounters()).thenReturn(stats);
  }

  @Test
  public void evictingFromEmptyListTest() throws Exception {
    LRUListWithSyncSorting list = new LRUListWithSyncSorting(controller);
    assertThat(list.getEvictableEntry()).isNull();
    assertThat(list.size()).isZero();
  }

  @Test
  public void evictingFromNonEmptyListTest() throws Exception {
    LRUListWithSyncSorting list = new LRUListWithSyncSorting(controller);
    EvictionNode node = mock(EvictableEntry.class);
    list.appendEntry(node);
    assertThat(list.size()).isEqualTo(1);

    when(node.next()).thenReturn(list.tail);
    when(node.previous()).thenReturn(list.head);
    assertThat(list.getEvictableEntry()).isSameAs(node);
    assertThat(list.size()).isZero();
  }

  @Test
  public void doesNotEvictRecentlyUsed() throws Exception {
    LRUListWithSyncSorting list = new LRUListWithSyncSorting(controller);
    EvictionNode recentlyUsedNode = mock(EvictableEntry.class);
    list.appendEntry(recentlyUsedNode);
    when(recentlyUsedNode.isRecentlyUsed()).thenReturn(true);
    EvictionNode node = mock(EvictableEntry.class);
    list.appendEntry(node);

    when(recentlyUsedNode.next()).thenReturn(node).thenReturn(null);
    when(recentlyUsedNode.previous()).thenReturn(list.head);
    when(node.next()).thenReturn(list.tail);
    when(node.previous()).thenReturn(recentlyUsedNode);

    assertThat(list.getEvictableEntry()).isSameAs(node);
    assertThat(list.tail.previous()).isSameAs(recentlyUsedNode);
    verify(recentlyUsedNode, atLeast(1)).unsetRecentlyUsed();
    assertThat(list.size()).isOne();
  }

  @Test
  public void doesNotEvictNodeInTransaction() throws Exception {
    LRUListWithSyncSorting list = new LRUListWithSyncSorting(controller);
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
    LRUListWithSyncSorting list = new LRUListWithSyncSorting(controller);

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
  public void verifyRecentlyUsedNodeIsGreedilyEvicted() throws Exception {
    System.setProperty("geode." + SystemPropertyHelper.EVICTION_SEARCH_MAX_ENTRIES, "1");

    LRUListWithSyncSorting list = new LRUListWithSyncSorting(controller);
    EvictionNode recentlyUsedNode1 = mock(EvictableEntry.class, "RecentlyUsed1");
    list.appendEntry(recentlyUsedNode1);
    when(recentlyUsedNode1.isRecentlyUsed()).thenReturn(true);
    EvictionNode recentlyUsedNode2 = mock(EvictableEntry.class, "RecentlyUsed2");
    when(recentlyUsedNode2.isRecentlyUsed()).thenReturn(true);
    list.appendEntry(recentlyUsedNode2);

    when(recentlyUsedNode1.next()).thenReturn(recentlyUsedNode2).thenReturn(null);
    when(recentlyUsedNode1.previous()).thenReturn(list.head);
    when(recentlyUsedNode2.next()).thenReturn(list.tail);
    when(recentlyUsedNode2.previous()).thenReturn(recentlyUsedNode1);

    assertThat(list.getEvictableEntry()).isSameAs(recentlyUsedNode2);
    assertThat(list.tail.previous()).isSameAs(recentlyUsedNode1);
    verify(recentlyUsedNode1, atLeast(1)).unsetRecentlyUsed();
    assertThat(list.size()).isOne();
  }

}
