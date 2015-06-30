/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import junit.framework.TestCase;

@Category(IntegrationTest.class)
public class ConcurrentMapLocalJUnitTest {

  private Cache cache;

    @Before
  public void setUp() throws Exception {
      this.cache = new CacheFactory().set("mcast-port", "0").set("locators", "").create();
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
