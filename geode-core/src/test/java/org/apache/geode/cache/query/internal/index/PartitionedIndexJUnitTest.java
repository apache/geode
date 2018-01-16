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
package org.apache.geode.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class PartitionedIndexJUnitTest {

  @Test
  public void mapIndexKeysMustContainTheCorrectNumberOfKeysWhenThereIsConcurrentAccess() {

    final int DATA_SIZE_TO_BE_POPULATED = 10000;
    final int THREAD_POOL_SIZE = 20;

    Region region = mock(Region.class);
    Cache cache = mock(Cache.class);
    when(region.getCache()).thenReturn(cache);
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
    PartitionedIndex partitionedIndex = new PartitionedIndex(IndexType.FUNCTIONAL, "dummyString",
        region, "dummyString", "dummyString", "dummyString");
    Runnable populateSetTask = () -> {
      for (int i = 0; i < DATA_SIZE_TO_BE_POPULATED; i++) {
        partitionedIndex.mapIndexKeys.add("" + i);
      }
    };
    Thread[] threads = new Thread[THREAD_POOL_SIZE];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(populateSetTask);
      threads[i].start();
    }
    try {
      for (int i = 0; i < threads.length; i++) {
        threads[i].join();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    assertEquals(DATA_SIZE_TO_BE_POPULATED, partitionedIndex.mapIndexKeys.size());

  }
}
