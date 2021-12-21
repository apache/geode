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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;

public class ConcurrentMapLocalJUnitTest { // TODO: reformat

  private Cache cache;

  @Before
  public void setUp() throws Exception {
    cache = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, "").create();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
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
    cmOpsUnsupported(
        cache.createRegionFactory(RegionShortcut.REPLICATE_PROXY).create("empty"));
  }

  @Test
  public void testNormalRegion() {
    cmOpsUnsupported(cache.createRegionFactory(RegionShortcut.REPLICATE)
        .setDataPolicy(DataPolicy.NORMAL).create("normal"));
  }

  @Test
  public void testLocalRegion() {
    Region r = cache.createRegionFactory(RegionShortcut.LOCAL).create("local");
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
