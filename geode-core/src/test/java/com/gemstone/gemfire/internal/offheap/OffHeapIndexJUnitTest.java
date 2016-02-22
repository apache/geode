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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test off-heap regions with indexes.
 * 
 * @author darrel
 *
 */
@Category(IntegrationTest.class)
public class OffHeapIndexJUnitTest {
  private GemFireCacheImpl gfc;
  
  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty("locators", "");
    props.setProperty("mcast-port", "0");
    props.setProperty("off-heap-memory-size", "100m");
    this.gfc = (GemFireCacheImpl) new CacheFactory(props).create();
  }
  @After
  public void tearDown() {
    this.gfc.close();
    SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    // TODO cleanup default disk store files
  }
  
  @Test
  public void testUnsupportedAsyncIndexes() throws RegionNotFoundException, IndexInvalidException, IndexNameConflictException, IndexExistsException {
    RegionFactory<Object, Object> rf = this.gfc.createRegionFactory();
    rf.setOffHeap(true);
    rf.setIndexMaintenanceSynchronous(false);
    rf.create("r");
    QueryService qs = this.gfc.getQueryService();
    try {
      qs.createIndex("idx", "age", "/r");
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertEquals("Asynchronous index maintenance is currently not supported for off-heap regions. The off-heap region is /r", expected.getMessage());
    }
  }
  @Test
  public void testUnsupportedMultiIteratorIndexes() throws RegionNotFoundException, IndexInvalidException, IndexNameConflictException, IndexExistsException {
    RegionFactory<Object, Object> rf = this.gfc.createRegionFactory();
    rf.setOffHeap(true);
    rf.setIndexMaintenanceSynchronous(true);
    rf.create("r");
    QueryService qs = this.gfc.getQueryService();
    try {
      qs.createIndex("idx", "addr", "/r r, r.addresses addr");
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertEquals("From clauses having multiple iterators(collections) are not supported for off-heap regions. The off-heap region is /r", expected.getMessage());
    }
  }
}
