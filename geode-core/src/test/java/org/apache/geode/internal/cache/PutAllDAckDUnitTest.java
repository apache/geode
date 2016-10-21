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
 * PutAllDAckDunitTest.java
 *
 * Created on September 15, 2005, 5:51 PM
 */
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
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
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class PutAllDAckDUnitTest extends JUnit4DistributedTestCase { // TODO: reformat

  static Cache cache;
  static Properties props = new Properties();
  static Properties propsWork = new Properties();
  static DistributedSystem ds = null;
  static Region region;
  static CacheTransactionManager cacheTxnMgr;
  static boolean beforeCreate = false;
  static int beforeCreateputAllcounter = 0;

  static boolean flag = false;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(() -> PutAllDAckDUnitTest.createCacheForVM0());
    vm1.invoke(() -> PutAllDAckDUnitTest.createCacheForVM1());
    LogWriterUtils.getLogWriter().fine("Cache created successfully");
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(() -> PutAllDAckDUnitTest.closeCache());
    vm1.invoke(() -> PutAllDAckDUnitTest.closeCache());
  }

  public static void createCacheForVM0() throws Exception {
    ds = (new PutAllDAckDUnitTest()).getSystem(props);
    cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    RegionAttributes attr = factory.create();
    region = cache.createRegion("map", attr);
  }

  public static void createCacheForVM1() throws Exception {
    CacheWriter aWriter = new BeforeCreateCallback();
    ds = (new PutAllDAckDUnitTest()).getSystem(props);
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
  public void testputAllRemoteVM() {
    // Test PASS.
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    Object[] objArr = new Object[1];
    for (int i = 0; i < 2; i++) {
      objArr[0] = "" + i;
      vm0.invoke(PutAllDAckDUnitTest.class, "putMethod", objArr);
    }
    vm0.invoke(() -> PutAllDAckDUnitTest.putAllMethod());
    flag = vm1.invoke(() -> PutAllDAckDUnitTest.getFlagVM1());

    vm1.invoke(new CacheSerializableRunnable("temp1") {
      public void run2() throws CacheException {
        if (flag) {

          assertEquals(region.size(), beforeCreateputAllcounter);
        }
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
    int i = 2, cntr = 0;
    try {
      while (cntr < 2) {
        m.put(new Integer(i), new String("map" + i));
        i++;
        cntr++;
      }

      region.putAll(m);

    } catch (Exception ex) {
      Assert.fail("Failed while region.putAll", ex);
    }
  }// end of putAllMethod


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

  static class BeforeCreateCallback extends CacheWriterAdapter {
    public void beforeCreate(EntryEvent event) {
      // try{
      // Thread.sleep(20000);
      // }catch(InterruptedException ex) {
      // //
      // }

      beforeCreateputAllcounter++;
      LogWriterUtils.getLogWriter().fine("*******BeforeCreate*****");
      beforeCreate = true;
    }
  }

  public static boolean getFlagVM1() {
    return beforeCreate;
  }

}// end of class
