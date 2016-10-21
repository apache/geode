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
package org.apache.geode.internal.offheap;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.query.*;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test off-heap regions with indexes.
 * 
 *
 */
@Category(IntegrationTest.class)
public class OffHeapIndexJUnitTest {
  private GemFireCacheImpl gfc;

  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, "100m");
    this.gfc = (GemFireCacheImpl) new CacheFactory(props).create();
  }

  @After
  public void tearDown() {
    this.gfc.close();
    MemoryAllocatorImpl.freeOffHeapMemory();
    // TODO cleanup default disk store files
  }

  @Test
  public void testUnsupportedAsyncIndexes() throws RegionNotFoundException, IndexInvalidException,
      IndexNameConflictException, IndexExistsException {
    RegionFactory<Object, Object> rf = this.gfc.createRegionFactory();
    rf.setOffHeap(true);
    rf.setIndexMaintenanceSynchronous(false);
    rf.create("r");
    QueryService qs = this.gfc.getQueryService();
    try {
      qs.createIndex("idx", "age", "/r");
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertEquals(
          "Asynchronous index maintenance is currently not supported for off-heap regions. The off-heap region is /r",
          expected.getMessage());
    }
  }

  @Test
  public void testUnsupportedMultiIteratorIndexes() throws RegionNotFoundException,
      IndexInvalidException, IndexNameConflictException, IndexExistsException {
    RegionFactory<Object, Object> rf = this.gfc.createRegionFactory();
    rf.setOffHeap(true);
    rf.setIndexMaintenanceSynchronous(true);
    rf.create("r");
    QueryService qs = this.gfc.getQueryService();
    try {
      qs.createIndex("idx", "addr", "/r r, r.addresses addr");
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertEquals(
          "From clauses having multiple iterators(collections) are not supported for off-heap regions. The off-heap region is /r",
          expected.getMessage());
    }
  }
}
