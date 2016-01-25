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
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.lru.HeapEvictor;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Performs eviction dunit tests for off-heap memory.
 * @author rholmes
 */
public class OffHeapEvictionDUnitTest extends EvictionDUnitTest {
  public OffHeapEvictionDUnitTest(String name) {
    super(name);
  }  
  
  @Override
  public void tearDown2() throws Exception {
    SerializableRunnable checkOrphans = new SerializableRunnable() {

      @Override
      public void run() {
        if(hasCache()) {
          OffHeapTestUtil.checkOrphans();
        }
      }
    };
    invokeInEveryVM(checkOrphans);
    try {
      checkOrphans.run();
    } finally {
      super.tearDown2();
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();    
    properties.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "200m");    
    
    return properties;
  }

  public void createCache() {
    try {
      Properties props = new Properties();
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(getDistributedSystemProperties());
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
      getLogWriter().info("cache= " + cache);
      getLogWriter().info("cache closed= " + cache.isClosed());
      cache.getResourceManager().setEvictionOffHeapPercentage(85);
      ((GemFireCacheImpl) cache).getResourceManager().getOffHeapMonitor().stopMonitoring(true);
      getLogWriter().info("eviction= "+cache.getResourceManager().getEvictionOffHeapPercentage());
      getLogWriter().info("critical= "+cache.getResourceManager().getCriticalOffHeapPercentage());
    }
    catch (Exception e) {
      fail("Failed while creating the cache", e);
    }
  }

  public void raiseFakeNotification(VM vm, final String prName,
      final int noOfExpectedEvictions) {
    vm.invoke(new CacheSerializableRunnable("fakeNotification") {
      @Override
      public void run2() throws CacheException {
        final LocalRegion region = (LocalRegion)cache.getRegion(prName);
        getEvictor().testAbortAfterLoopCount = 1;
        
        ((GemFireCacheImpl) cache).getResourceManager().getOffHeapMonitor().updateStateAndSendEvent(188743680);
          
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            // we have a primary
            final long currentEvictions = ((AbstractLRURegionMap)region.entries)
                ._getLruList().stats().getEvictions();
            if (Math.abs(currentEvictions - noOfExpectedEvictions) <= 1) { // Margin of error is 1
              return true;
            }
            else if (currentEvictions > noOfExpectedEvictions) {
              fail(description());
            }
            return false;
          }
          public String description() {
            return "expected "+noOfExpectedEvictions+" evictions, but got "+
            ((AbstractLRURegionMap)region.entries)._getLruList().stats()
            .getEvictions();
          }
        };
        DistributedTestCase.waitForCriterion(wc, 60000, 1000, true);
      }
    });
  }
  
  @Override
  public boolean getOffHeapEnabled() {
    return true;
  }    

  public HeapEvictor getEvictor() {
    return ((GemFireCacheImpl)cache).getOffHeapEvictor();
  }  

  public ResourceType getResourceType() {
    return ResourceType.OFFHEAP_MEMORY;
  }
}