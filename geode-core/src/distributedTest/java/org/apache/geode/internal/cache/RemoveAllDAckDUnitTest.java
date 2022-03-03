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
/*
 * RemoveAllDAckDunitTest.java
 *
 * Created on September 15, 2005, 5:51 PM
 */
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * Adapted from RemoveAllDAckDUnitTest
 */

public class RemoveAllDAckDUnitTest extends JUnit4DistributedTestCase { // TODO: reformat

  static Cache cache;
  static Properties props = new Properties();
  static Properties propsWork = new Properties();
  static DistributedSystem ds = null;
  static Region region;
  static boolean beforeDestroy = false;
  static int beforeDestroyRemoveAllcounter = 0;

  static boolean flag = false;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(RemoveAllDAckDUnitTest::createCacheForVM0);
    vm1.invoke(RemoveAllDAckDUnitTest::createCacheForVM1);
    LogWriterUtils.getLogWriter().fine("Cache created successfully");
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(RemoveAllDAckDUnitTest::closeCache);
    vm1.invoke(RemoveAllDAckDUnitTest::closeCache);
  }

  public static void createCacheForVM0() throws Exception {
    ds = (new RemoveAllDAckDUnitTest()).getSystem(props);
    cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    RegionAttributes attr = factory.create();
    region = cache.createRegion("map", attr);
  }

  public static void createCacheForVM1() throws Exception {
    CacheWriter aWriter = new BeforeDestroyCallback();
    ds = (new RemoveAllDAckDUnitTest()).getSystem(props);
    cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setCacheWriter(aWriter);
    RegionAttributes attr = factory.create();
    region = cache.createRegion("map", attr);
  }

  public static void closeCache() throws Exception {
    // getLogWriter().fine("closing cache cache cache cache cache 33333333");
    cache.close();
    ds.disconnect();
    // getLogWriter().fine("closed cache cache cache cache cache 44444444");
  }

  // test methods

  @Test
  public void testRemoveAllRemoteVM() {
    // Test PASS.
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    Object[] objArr = new Object[1];
    for (int i = 0; i < 3; i++) {
      objArr[0] = "" + i;
      vm0.invoke(RemoveAllDAckDUnitTest.class, "putMethod", objArr);
    }
    vm0.invoke(RemoveAllDAckDUnitTest::removeAllMethod);
    flag = vm1.invoke(RemoveAllDAckDUnitTest::getFlagVM1);
    assertEquals(true, flag);

    vm1.invoke(new CacheSerializableRunnable("temp1") {
      @Override
      public void run2() throws CacheException {
        assertEquals(2, beforeDestroyRemoveAllcounter);
      }
    });

  }// end of test case1


  public static Object putMethod(Object ob) {
    Object obj = null;
    try {
      if (ob != null) {
        String str = "first";
        obj = region.put(ob, str);
      }
    } catch (Exception ex) {
      Assert.fail("Failed while region.put", ex);
    }
    return obj;
  }// end of putMethod

  public static void removeAllMethod() {
    assertEquals(3, region.size());
    ArrayList l = new ArrayList();
    for (int i = 0; i < 2; i++) {
      l.add("" + i);
    }
    region.removeAll(l, "removeAllCallback");
    assertEquals(1, region.size());
  }

  static class BeforeDestroyCallback extends CacheWriterAdapter {
    @Override
    public void beforeDestroy(EntryEvent event) {
      beforeDestroyRemoveAllcounter++;
      assertEquals(true, event.getOperation().isRemoveAll());
      assertEquals("removeAllCallback", event.getCallbackArgument());
      LogWriterUtils.getLogWriter().fine("*******BeforeDestroy*****");
      beforeDestroy = true;
    }
  }

  public static boolean getFlagVM1() {
    return beforeDestroy;
  }

}// end of class
