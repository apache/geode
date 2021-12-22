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
 * ClearMultiVmCallBkDUnitTest.java
 *
 * Created on August 11, 2005, 7:37 PM
 */
package org.apache.geode.cache30;

import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class ClearMultiVmCallBkDUnitTest extends JUnit4DistributedTestCase { // TODO: reformat

  static Cache cache;
  static Properties props = new Properties();
  static Properties propsWork = new Properties();
  static DistributedSystem ds = null;
  static Region region;
  static Region paperWork;
  static CacheTransactionManager cacheTxnMgr;
  static boolean afterClear = false;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(ClearMultiVmCallBkDUnitTest::createCache);
    vm1.invoke(ClearMultiVmCallBkDUnitTest::createCache);
    LogWriterUtils.getLogWriter().fine("Cache created in successfully");
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(ClearMultiVmCallBkDUnitTest::closeCache);
    vm1.invoke(ClearMultiVmCallBkDUnitTest::closeCache);
  }

  public static void createCache() {
    CacheListener aListener = new ListenerCallBk();
    ds = (new ClearMultiVmCallBkDUnitTest()).getSystem(props);

    cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);

    // Set Cachelisterner : aListener

    factory.setCacheListener(aListener);
    RegionAttributes attr = factory.create();

    region = cache.createRegion("map", attr);
  }

  public static void closeCache() {
    cache.close();
    ds.disconnect();
  }

  // test methods

  @Test
  public void testClearSingleVM() {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    // VM vm1 = host.getVM(1);

    // Object obj0;
    // Object obj1;
    Object[] objArr = new Object[1];
    for (int i = 1; i < 4; i++) {
      objArr[0] = "" + i;
      vm0.invoke(ClearMultiVmCallBkDUnitTest.class, "putMethod", objArr);

    }
    LogWriterUtils.getLogWriter().fine("Did all puts successfully");

    vm0.invoke(ClearMultiVmCallBkDUnitTest::clearMethod);
    LogWriterUtils.getLogWriter().fine("Did clear successfully");

    while (afterClear) {
    }

    int Regsize = vm0.invoke(ClearMultiVmCallBkDUnitTest::sizeMethod);
    assertEquals(1, Regsize);


  }// end of test case1

  @Test
  public void testClearMultiVM() {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    Object[] objArr = new Object[1];
    for (int i = 1; i < 4; i++) {
      objArr[0] = "" + i;
      vm0.invoke(ClearMultiVmCallBkDUnitTest.class, "putMethod", objArr);
      vm1.invoke(ClearMultiVmCallBkDUnitTest.class, "getMethod", objArr);
    }
    LogWriterUtils.getLogWriter().fine("Did all puts successfully");
    // vm0.invoke(() -> ClearMultiVmCallBkDUnitTest.putMethod());
    vm1.invoke(ClearMultiVmCallBkDUnitTest::clearMethod);
    LogWriterUtils.getLogWriter().fine("Did clear successfully");

    while (afterClear) {
    }

    int Regsize = vm0.invoke(ClearMultiVmCallBkDUnitTest::sizeMethod);
    assertEquals(1, Regsize);


  }// end of test case2

  public static Object putMethod(Object ob) {
    Object obj = null;
    try {
      if (ob != null) {
        String str = "first";
        obj = region.put(ob, str);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed while region.put", ex);
    }
    return obj;
  }

  public static Object getMethod(Object ob) {
    Object obj = null;
    try {
      obj = region.get(ob);
    } catch (Exception ex) {
      fail("Failed while region.get", ex);
    }
    return obj;
  }

  public static boolean containsValueMethod(Object ob) {
    boolean flag = false;
    try {
      flag = region.containsValue(ob);
    } catch (Exception ex) {
      fail("Failed while region.containsValueMethod", ex);
    }
    return flag;
  }

  public static int sizeMethod() {
    int i = 0;
    try {
      i = region.size();
    } catch (Exception ex) {
      fail("Failed while region.size", ex);
    }
    return i;
  }

  public static void clearMethod() {
    try {
      region.clear();
    } catch (Exception ex) {
      fail("clearMethod failed", ex);
    }
  }

  public static boolean getBoolean() {
    return afterClear;
  }

  static class ListenerCallBk extends CacheListenerAdapter {

    @Override
    public void afterRegionClear(RegionEvent event) {
      LogWriterUtils.getLogWriter().fine("In afterClear:: CacheListener Callback");
      try {
        int i = 7;
        region.put("" + i, "inAfterClear");
        afterClear = true;
      } catch (Exception e) {
        fail("afterRegionClear failed", e);
      }
    }
  }
}// end of class
