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
package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystemConfigProperties;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class TxReleasesOffHeapOnCloseJUnitTest {
  
  protected Cache cache;
  
  protected void createCache() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(DistributedSystemConfigProperties.OFF_HEAP_MEMORY_SIZE, "1m");
    cache = new CacheFactory(props).create();
  }
  
  // Start a tx and have it modify an entry on an offheap region.
  // Close the cache and verify that the offheap memory was released
  // even though the tx was not commited or rolled back.
  @Test
  public void testTxReleasesOffHeapOnClose() {
    createCache();
    MemoryAllocatorImpl sma = MemoryAllocatorImpl.getAllocator();
    RegionFactory rf = cache.createRegionFactory();
    rf.setOffHeap(true);
    Region r = rf.create("testTxReleasesOffHeapOnClose");
    r.put("key", "value");
    CacheTransactionManager txmgr = cache.getCacheTransactionManager();
    txmgr.begin();
    r.put("key", "value2");
    cache.close();
    assertEquals(0, sma.getUsedMemory());
  }
}
