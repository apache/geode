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
 * PutAllCallBkSingleVMDUnitTest.java
 *
 * Created on August 31, 2005, 4:17 PM
 */
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class PutAllCallBkSingleVMDUnitTest extends JUnit4DistributedTestCase {

  static volatile Cache cache;
  static Properties props = new Properties();
  static Properties propsWork = new Properties();
  static volatile DistributedSystem ds = null;
  static volatile Region region;
  static boolean afterCreate = false;
  static boolean afterUpdate = false;
  static int putAllcounter = 0;
  static int afterUpdateputAllcounter = 0;
  static boolean beforeCreate = false;
  static boolean beforeUpdate = false;
  static int beforeCreateputAllcounter = 0;
  static int beforeUpdateputAllcounter = 0;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(PutAllCallBkSingleVMDUnitTest::createCache);
    vm1.invoke(PutAllCallBkSingleVMDUnitTest::createCache);
    LogWriterUtils.getLogWriter().fine("Cache created in successfully");
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(PutAllCallBkSingleVMDUnitTest::closeCache);
    vm1.invoke(PutAllCallBkSingleVMDUnitTest::closeCache);
  }

  public static synchronized void createCache() {
    try {
      CacheListener aListener = new AfterCreateCallback();
      CacheWriter aWriter = new BeforeCreateCallback();
      ds = (new PutAllCallBkSingleVMDUnitTest()).getSystem(props);
      cache = CacheFactory.create(ds);
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setCacheWriter(aWriter);
      factory.setCacheListener(aListener);
      RegionAttributes attr = factory.create();

      region = cache.createRegion("map", attr);


    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static synchronized void closeCache() {
    region = null;
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (ds != null) {
      ds.disconnect();
      ds = null;
    }
  }

  // test methods
  @Test
  public void testputAllSingleVM() {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    // Object obj2;
    Object[] objArr = new Object[1];
    for (int i = 0; i < 5; i++) {
      objArr[0] = "" + i;
      vm0.invoke(PutAllCallBkSingleVMDUnitTest.class, "putMethod", objArr);

    }

    vm0.invoke(PutAllCallBkSingleVMDUnitTest::putAllMethod);

    vm0.invoke(new CacheSerializableRunnable("temp1") {
      @Override
      public void run2() throws CacheException {
        if (!afterCreate) {
          fail("FAILED in aftercreate call back");
        }
        assertEquals(region.size(), putAllcounter);
        assertEquals(region.size(), beforeCreateputAllcounter);
      }
    });

    vm0.invoke(new CacheSerializableRunnable("abc") {
      @Override
      public void run2() throws CacheException {
        CacheListener bListener = new AfterUpdateCallback();
        CacheWriter bWriter = new BeforeUpdateCallback();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setCacheWriter(bWriter);
        factory.setCacheListener(bListener);
        RegionAttributes attr = factory.create();
        Region tempRegion = cache.createRegion("temp", attr);

        // to invoke afterUpdate we should make sure that entries are already present
        for (int i = 0; i < 5; i++) {
          tempRegion.put(i, "region" + i);
        }

        Map m = new HashMap();
        for (int i = 0; i < 5; i++) {
          m.put(i, "map" + i);
        }

        tempRegion.putAll(m, "putAllAfterUpdateCallback");

        // now, verifying callbacks
        if (!afterUpdate) {
          fail("FAILED in afterupdate call back");
        }
        assertEquals(tempRegion.size(), afterUpdateputAllcounter);
        assertEquals(tempRegion.size(), beforeUpdateputAllcounter);
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

  public static void putAllMethod() {
    Map m = new HashMap();
    int i = 5, cntr = 0;
    try {
      while (cntr < 21) {
        m.put(i, "map" + i);
        i++;
        cntr++;
      }

      region.putAll(m, "putAllCreateCallback");

    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed while region.putAll");
    }
  }// end of putAllMethod

  public static void putAllAfterUpdate() {
    Map m = new HashMap();
    int cntr = 0;
    try {
      for (int i = 0; i < 5; i++) {
        m.put("" + i, "map_AfterUpdate" + i);
        cntr++;
      }
      region.putAll(m, "putAllAfterUpdateCallback");
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed while region.putAll");
    }
  }// end of putAllAfterUpdate

  public static Object getMethod(Object ob) {
    Object obj = null;
    try {
      obj = region.get(ob);
    } catch (Exception ex) {
      fail("Failed while region.get");
    }
    return obj;
  }

  public static boolean containsValueMethod(Object ob) {
    boolean flag = false;
    try {
      flag = region.containsValue(ob);
    } catch (Exception ex) {
      fail("Failed while region.containsValueMethod");
    }
    return flag;
  }

  public static int sizeMethod() {
    int i = 0;
    try {
      i = region.size();
    } catch (Exception ex) {
      fail("Failed while region.size");
    }
    return i;
  }

  public static void clearMethod() {
    try {
      region.clear();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  static class AfterCreateCallback extends CacheListenerAdapter {
    @Override
    public void afterCreate(EntryEvent event) {
      putAllcounter++;
      LogWriterUtils.getLogWriter().fine("In afterCreate" + putAllcounter);
      if (event.getOperation().isPutAll()) {
        assertEquals("putAllCreateCallback", event.getCallbackArgument());
      }
      if (putAllcounter == 25) {
        LogWriterUtils.getLogWriter().fine("performingtrue");
        afterCreate = true;
      }
    }
  }

  static class AfterUpdateCallback extends CacheListenerAdapter {
    @Override
    public void afterUpdate(EntryEvent event) {
      afterUpdateputAllcounter++;
      LogWriterUtils.getLogWriter().fine("In afterUpdate" + afterUpdateputAllcounter);
      if (event.getOperation().isPutAll()) {
        assertEquals("putAllAfterUpdateCallback", event.getCallbackArgument());
      }
      if (afterUpdateputAllcounter == 5) {
        LogWriterUtils.getLogWriter().fine("performingtrue afterUpdate");
        afterUpdate = true;
      }
    }
  }
  static class BeforeCreateCallback extends CacheWriterAdapter {
    @Override
    public void beforeCreate(EntryEvent event) {
      beforeCreateputAllcounter++;
      LogWriterUtils.getLogWriter().fine("In beforeCreate" + beforeCreateputAllcounter);
      if (event.getOperation().isPutAll()) {
        assertEquals("putAllCreateCallback", event.getCallbackArgument());
      }
      if (beforeCreateputAllcounter == 25) {
        LogWriterUtils.getLogWriter().fine("performingtrue beforeCreateputAll");
        beforeCreate = true;
      }
    }
  }
  static class BeforeUpdateCallback extends CacheWriterAdapter {
    @Override
    public void beforeUpdate(EntryEvent event) {
      beforeUpdateputAllcounter++;
      LogWriterUtils.getLogWriter().fine("In beforeUpdate" + beforeUpdateputAllcounter);
      if (event.getOperation().isPutAll()) {
        assertEquals("putAllAfterUpdateCallback", event.getCallbackArgument());
      }
      if (beforeUpdateputAllcounter == 5) {
        LogWriterUtils.getLogWriter().fine("performingtrue beforeUpdate");
        beforeUpdate = true;
      }
    }
  }

}// end of class
