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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.DistributedSystem;

public class MapInterfaceJUnitTest {

  protected boolean hasBeenNotified = false;

  protected Region region2 = null;
  protected int counter = 0;

  @Test
  public void testLocalClear() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = null;
    Region region = null;
    AttributesFactory factory = null;
    try {
      cache = CacheFactory.create(ds);
      factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      region = cache.createRegion("testingRegion", factory.create());
    } catch (Exception e) {
      throw new AssertionError(" failed due to ", e);
    }
    for (int i = 0; i < 100; i++) {
      region.put(i, i);
    }
    assertEquals(50, region.get(50));
    region.localClear();
    assertEquals(null, region.get(50));
    region.close();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    try {
      region = cache.createRegion("testingRegion", factory.create());
    } catch (Exception e) {
      throw new AssertionError(" failed in creating region due to ", e);
    }
    boolean exceptionOccurred = false;
    try {
      region.localClear();
    } catch (UnsupportedOperationException e) {
      exceptionOccurred = true;
    }
    if (!exceptionOccurred) {
      fail(" exception did not occur when it was supposed to occur");
    }
    region.close();
    cache.close();
    ds.disconnect();
  }

  /**
   * Make sure putAll works on Scope.LOCAL (see bug 35087)
   */
  @Test
  public void testLocalPutAll() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = null;
    Region region = null;
    AttributesFactory factory = null;
    try {
      cache = CacheFactory.create(ds);
      factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      region = cache.createRegion("testingRegion", factory.create());
    } catch (Exception e) {
      throw new AssertionError(" failed due to ", e);
    }
    HashMap m = new HashMap();
    m.put("aKey", "aValue");
    m.put("bKey", "bValue");
    region.putAll(m);
    assertEquals("aValue", region.get("aKey"));
    assertEquals("bValue", region.get("bKey"));
    for (int i = 0; i < 100; i++) {
      region.put(i, i);
    }
    assertEquals(50, region.get(50));
    region.localClear();
    assertEquals(null, region.get(50));
    region.close();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    try {
      region = cache.createRegion("testingRegion", factory.create());
    } catch (Exception e) {
      throw new AssertionError(" failed in creating region due to ", e);
    }
    boolean exceptionOccurred = false;
    try {
      region.localClear();
    } catch (UnsupportedOperationException e) {
      exceptionOccurred = true;
    }
    if (!exceptionOccurred) {
      fail(" exception did not occur when it was supposed to occur");
    }
    region.close();
    cache.close();
    ds.disconnect();
  }

  @Test
  public void testBeforeRegionClearCallBack() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = null;
    Region region = null;
    AttributesFactory factory = null;
    try {
      cache = CacheFactory.create(ds);
      factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setCacheWriter(new CacheWriterAdapter() {

        @Override
        public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
          synchronized (this) {
            notify();
            hasBeenNotified = true;
          }
        }
      });
      region = cache.createRegion("testingRegion", factory.create());
      DoesClear doesClear = new DoesClear(region);
      new Thread(doesClear).start();
      synchronized (this) {
        if (!hasBeenNotified) {
          wait(3000);
        }
      }
      if (!hasBeenNotified) {
        fail(" beforeRegionClear call back did not come");
      }
    } catch (Exception e) {
      throw new AssertionError(" failed due to ", e);
    }
    for (int i = 0; i < 100; i++) {
      region.put(i, i);
    }
    assertEquals(50, region.get(50));
    region.localClear();
    assertEquals(null, region.get(50));
    region.close();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    try {
      region = cache.createRegion("testingRegion", factory.create());
    } catch (Exception e) {
      throw new AssertionError(" failed in creating region due to ", e);
    }
    boolean exceptionOccurred = false;
    try {
      region.localClear();
    } catch (UnsupportedOperationException e) {
      exceptionOccurred = true;
    }
    if (!exceptionOccurred) {
      fail(" exception did not occur when it was supposed to occur");
    }
    region.close();
    cache.close();
    ds.disconnect();
  }

  @Test
  public void testSetValue() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = null;

    AttributesFactory factory = null;
    try {
      cache = CacheFactory.create(ds);
      factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setCacheWriter(new CacheWriterAdapter() {

        @Override
        public void beforeUpdate(EntryEvent event) throws CacheWriterException {
          synchronized (this) {
            notify();
            counter++;
            hasBeenNotified = true;
          }
        }

      });
      region2 = cache.createRegion("testingRegion", factory.create());
      region2.put(2, 2);
      hasBeenNotified = false;
      DoesPut doesPut = new DoesPut();
      new Thread(doesPut).start();
      synchronized (this) {
        if (!hasBeenNotified) {
          wait(3000);
        }
      }
      if (!hasBeenNotified) {
        fail(" beforeCreate call back did not come");
      }

      assertEquals(counter, 1);
    } catch (Exception e) {
      throw new AssertionError(" failed due to ", e);
    }
  }

  class DoesClear implements Runnable {

    private final Region region;

    DoesClear(Region reg) {
      region = reg;
    }

    @Override
    public void run() {
      region.clear();
    }
  }

  class DoesPut implements Runnable {

    DoesPut() {}

    @Override
    public void run() {
      ((Map.Entry) (region2.entrySet().iterator().next()))
          .setValue(8);
    }
  }
}
