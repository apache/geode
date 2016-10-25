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
package org.apache.geode.internal.cache.lru;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.offheap.MemoryAllocator;

/**
 * Triggers centralized eviction(asynchronously) when the ResourceManager sends
 * an eviction event for off-heap regions. This is registered with the ResourceManager.
 *
 * @since Geode 1.0
 */
public class OffHeapEvictor extends HeapEvictor {
  private static final String EVICTOR_THREAD_GROUP_NAME = "OffHeapEvictorThreadGroup";
  
  private static final String EVICTOR_THREAD_NAME = "OffHeapEvictorThread";
  
  private long bytesToEvictWithEachBurst;
  
  public OffHeapEvictor(Cache gemFireCache) {
    super(gemFireCache);    
    calculateEvictionBurst();
  }

  private void calculateEvictionBurst() {
    float evictionBurstPercentage = Float
        .parseFloat(System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "HeapLRUCapacityController.evictionBurstPercentage", "0.4"));
    
    MemoryAllocator allocator = ((GemFireCacheImpl) this.cache).getOffHeapStore();
    
    /*
     * Bail if there is no off-heap memory to evict.
     */
    if(null == allocator) {
      throw new IllegalStateException(LocalizedStrings.MEMSCALE_EVICTION_INIT_FAIL.toLocalizedString());
    }
    
    bytesToEvictWithEachBurst = (long)(allocator.getTotalMemory() * 0.01 * evictionBurstPercentage);       
  }
  
  protected int getEvictionLoopDelayTime() {
    if (numEvictionLoopsCompleted < Math.max(3, numFastLoops)) {
      return 250;
    }
    
    return 1000;
  }
  
  protected boolean includePartitionedRegion(PartitionedRegion region) {
    return (region.getEvictionAttributes().getAlgorithm().isLRUHeap() 
        && (region.getDataStore() != null) 
        && region.getAttributes().getOffHeap());
  }
  
  protected boolean includeLocalRegion(LocalRegion region) {
    return (region.getEvictionAttributes().getAlgorithm().isLRUHeap() 
        && region.getAttributes().getOffHeap());
  }
  
  protected String getEvictorThreadGroupName() {
    return OffHeapEvictor.EVICTOR_THREAD_GROUP_NAME;
  }
  
  protected String getEvictorThreadName() {
    return OffHeapEvictor.EVICTOR_THREAD_NAME;
  }

  public long getTotalBytesToEvict() {
    return bytesToEvictWithEachBurst;
  }

  @Override
  protected ResourceType getResourceType() {
    return ResourceType.OFFHEAP_MEMORY;
  }  
}
