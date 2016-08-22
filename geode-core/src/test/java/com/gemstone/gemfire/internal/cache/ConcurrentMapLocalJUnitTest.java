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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class ConcurrentMapLocalJUnitTest { // TODO: reformat

  private Cache cache;

    @Before
  public void setUp() throws Exception {
      this.cache = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, "").create();
    }

    @After
  public void tearDown() throws Exception {
      this.cache.close();
    }
    
    private void cmOpsUnsupported(Region r) {
      Object key = "key";
      Object value = "value";
      try {
        r.putIfAbsent(key, value);
        fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException expected) {
      }
      try {
        r.remove(key, value);
        fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException expected) {
      }
      try {
        r.replace(key, value);
        fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException expected) {
      }
      try {
        r.replace(key, value, "newValue");
        fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException expected) {
      }
    }
    
    @Test
  public void testEmptyRegion() {
      cmOpsUnsupported(this.cache.createRegionFactory(RegionShortcut.REPLICATE_PROXY).create("empty"));
    }
    @Test
  public void testNormalRegion() {
      cmOpsUnsupported(this.cache.createRegionFactory(RegionShortcut.REPLICATE).setDataPolicy(DataPolicy.NORMAL).create("normal"));
    }
    @Test
  public void testLocalRegion() {
      Region r = this.cache.createRegionFactory(RegionShortcut.LOCAL).create("local");
      Object key = "key";
      assertEquals(null, r.putIfAbsent(key, "value"));
      assertEquals("value", r.putIfAbsent(key, "value1"));
      assertEquals("value", r.get(key));
      assertEquals("value", r.replace(key, "value2"));
      assertEquals("value2", r.get(key));
      assertEquals(true, r.replace(key, "value2", "value3"));
      assertEquals(false, r.replace(key, "value2", "value3"));
      assertEquals(false, r.remove(key, "value2"));
      assertEquals(true, r.containsKey(key));
      assertEquals(true, r.remove(key, "value3"));
      assertEquals(false, r.containsKey(key));
    }

}
