/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.wan.parallel;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue.MetaRegionFactory;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue.ParallelGatewaySenderQueueMetaRegion;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import static org.mockito.Mockito.*;


@Category(UnitTest.class)
public class ParallelGatewaySenderQueueJUnitTest {
  
  private ParallelGatewaySenderQueue queue;
  private MetaRegionFactory metaRegionFactory;
  private GemFireCacheImpl cache;

  @Before
  public void createParallelGatewaySenderQueue() {
    cache = mock(GemFireCacheImpl.class);
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(sender.getCancelCriterion()).thenReturn(cancelCriterion);
    when(sender.getCache()).thenReturn(cache);
    when(sender.getMaximumQueueMemory()).thenReturn(100);
    when(sender.getLifeCycleLock()).thenReturn(new ReentrantReadWriteLock());
    metaRegionFactory = mock(MetaRegionFactory.class);
    queue = new ParallelGatewaySenderQueue(sender, Collections.emptySet(), 0, 1, metaRegionFactory);
  }

  @Test
  public void testLocalSize() throws TimeoutException, RegionExistsException, ClassNotFoundException, IOException {
    ParallelGatewaySenderQueueMetaRegion mockMetaRegion = mock(ParallelGatewaySenderQueueMetaRegion.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(mockMetaRegion.getDataStore()).thenReturn(dataStore);
    when(dataStore.getSizeOfLocalPrimaryBuckets()).thenReturn(3); 
    when(metaRegionFactory.newMetataRegion(any(), any(), any(), any(), anyBoolean())).thenReturn(mockMetaRegion);
    when(cache.createVMRegion(any(), any(), any())).thenReturn(mockMetaRegion);
    
    queue.addShadowPartitionedRegionForUserPR(mockPR("region1"));
    
    assertEquals(3, queue.localSize());
  }

  private PartitionedRegion mockPR(String name) {
    PartitionedRegion region = mock(PartitionedRegion.class);
    when(region.getFullPath()).thenReturn(name);
    when(region.getPartitionAttributes()).thenReturn(new PartitionAttributesFactory<>().create());
    when(region.getTotalNumberOfBuckets()).thenReturn(113);
    when(region.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    return region;
  }

}
