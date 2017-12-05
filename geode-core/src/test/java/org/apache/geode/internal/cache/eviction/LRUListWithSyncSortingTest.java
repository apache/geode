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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LRUListWithSyncSortingTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private BucketRegion bucketRegion;
  private InternalEvictionStatistics stats;

  @Before
  public void setup() {
    bucketRegion = mock(BucketRegion.class);
    stats = mock(InternalEvictionStatistics.class);
  }

  @Test
  public void evictingFromEmptyListTest() throws Exception {
    LRUListWithSyncSorting list = new LRUListWithSyncSorting(stats, bucketRegion);
    assertThat(list.getEvictableEntry()).isNull();
    assertThat(list.size()).isZero();
  }

  @Test
  public void evictingFromNonEmptyListTest() throws Exception {
    LRUListWithSyncSorting list = new LRUListWithSyncSorting(stats, bucketRegion);
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
    LRUListWithSyncSorting list = new LRUListWithSyncSorting(stats, bucketRegion);
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
    LRUListWithSyncSorting list = new LRUListWithSyncSorting(stats, bucketRegion);

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

}
