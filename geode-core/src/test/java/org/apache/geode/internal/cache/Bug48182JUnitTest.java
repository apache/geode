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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.*;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TestCase that emulates the conditions that produce defect 48182 and ensures that the fix works under those conditions.
 * 48182: Unexpected EntryNotFoundException while shutting down members with off-heap
 * https://svn.gemstone.com/trac/gemfire/ticket/48182 
 */
@Category(IntegrationTest.class)
public class Bug48182JUnitTest {
  /**
   * A region entry key.
   */
  private static final String KEY = "KEY";
  
  /**
   * A region entry value.
   */
  private static final String VALUE = " Vestibulum quis lobortis risus. Cras cursus eget dolor in facilisis. Curabitur purus arcu, dignissim ac lorem non, venenatis condimentum tellus. Praesent at erat dapibus, bibendum nunc sed, congue nulla";
  
  /**
   * A cache.
   */
  private GemFireCacheImpl cache = null;


  @Before
  public void setUp() throws Exception {
    // Create our cache
    this.cache = createCache();
  }

  @After
  public void tearDown() throws Exception {
    // Cleanup our cache
    closeCache(this.cache);
  }

  /**
   * @return the test's cache.
   */
  protected GemFireCacheImpl getCache() {
    return this.cache;
  }
  
  /**
   * Close a cache.
   * @param gfc the cache to close.
   */
  protected void closeCache(GemFireCacheImpl gfc) {
    gfc.close();
  }
  
  /**
   * @return the test's off heap memory size.
   */
  protected String getOffHeapMemorySize() {
    return "2m";
  }
  
  /**
   * @return the type of region for the test.
   */
  protected RegionShortcut getRegionShortcut() {
    return RegionShortcut.REPLICATE;
  }
  
  /**
   * @return the region containing our test data.
   */
  protected String getRegionName() {
    return "region1";
  }
  
  /**
   * Creates and returns the test region with concurrency checks enabled.
   */
  protected Region<Object,Object> createRegion() {
    return createRegion(true);
  }
  
  /**
   * Creates and returns the test region.
   * @param concurrencyChecksEnabled concurrency checks will be enabled if true.
   */
  protected Region<Object,Object> createRegion(boolean concurrencyChecksEnabled) {
    return getCache().createRegionFactory(getRegionShortcut()).setOffHeap(true).setConcurrencyChecksEnabled(concurrencyChecksEnabled).create(getRegionName());    
  }

  /**
   * Creates and returns the test cache.
   */
  protected GemFireCacheImpl createCache() {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, getOffHeapMemorySize());
    GemFireCacheImpl result = (GemFireCacheImpl) new CacheFactory(props).create();
    return result;
  }

  /**
   * Simulates the conditions for 48182 by setting a test hook boolean in {@link AbstractRegionMap}.  This test 
   * hook forces a cache close during a destroy in an off-heap region.  This test asserts that a CacheClosedException
   * is thrown rather than an EntryNotFoundException (or any other exception type for that matter).
   */
  @Test
  public void test48182WithCacheClose() throws Exception {
    AbstractRegionMap.testHookRunnableFor48182 = new Runnable() {
      @Override
      public void run() {
        getCache().close();
      }      
    };
    
    // True when the correct exception has been triggered.
    boolean correctException = false;
    
    Region<Object,Object> region = createRegion();
    region.put(KEY, VALUE);
    
    try {
      region.destroy(KEY);
    } catch(CacheClosedException e) {
      correctException = true;
//      e.printStackTrace();
    } catch(Exception e) {
//      e.printStackTrace();
      fail("Did not receive a CacheClosedException.  Received a " + e.getClass().getName() + " instead.");
    }
    
    assertTrue("A CacheClosedException was not triggered",correctException);
  }  

  /**
   * Simulates the conditions similar to 48182 by setting a test hook boolean in {@link AbstractRegionMap}.  This test 
   * hook forces a region destroy during a destroy operation in an off-heap region.  This test asserts that a RegionDestroyedException
   * is thrown rather than an EntryNotFoundException (or any other exception type for that matter).
   */
  @Test
  public void test48182WithRegionDestroy() throws Exception {
    AbstractRegionMap.testHookRunnableFor48182 = new Runnable() {
      @Override
      public void run() {
        getCache().getRegion(getRegionName()).destroyRegion();
      }      
    };

    // True when the correct exception has been triggered.
    boolean correctException = false;
    
    Region<Object,Object> region = createRegion();
    region.put(KEY, VALUE);
    
    try {
      region.destroy(KEY);
    } catch(RegionDestroyedException e) {
      correctException = true;
//      e.printStackTrace();
    } catch(Exception e) {
//      e.printStackTrace();
      fail("Did not receive a RegionDestroyedException.  Received a " + e.getClass().getName() + " instead.");
    }

    assertTrue("A RegionDestroyedException was not triggered",correctException);    
  }
}
