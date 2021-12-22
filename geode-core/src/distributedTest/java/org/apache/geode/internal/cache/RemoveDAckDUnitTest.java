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
 * RemoveDAckDUnitTest.java
 *
 * Created on September 15, 2005, 12:41 PM
 */
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class RemoveDAckDUnitTest extends JUnit4DistributedTestCase { // TODO: reformat

  static Cache cache;
  static Properties props = new Properties();
  static Properties propsWork = new Properties();
  static DistributedSystem ds = null;
  static Region region;
  static CacheTransactionManager cacheTxnMgr;
  static volatile boolean IsBeforeDestroy = false;
  static boolean flag = false;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(RemoveDAckDUnitTest::createCacheVM0);
    vm1.invoke(RemoveDAckDUnitTest::createCacheVM1);
    LogWriterUtils.getLogWriter().fine("Cache created in successfully");
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(RemoveDAckDUnitTest::closeCache);
    vm1.invoke(RemoveDAckDUnitTest::closeCache);
  }

  public static void createCacheVM0() {
    try {
      ds = (new RemoveDAckDUnitTest()).getSystem(props);
      cache = CacheFactory.create(ds);
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      RegionAttributes attr = factory.create();

      region = cache.createRegion("map", attr);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  } // end of create cache for VM0

  public static void createCacheVM1() {
    try {
      ds = (new RemoveDAckDUnitTest()).getSystem(props);
      AttributesFactory factory = new AttributesFactory();
      cache = CacheFactory.create(ds);
      factory.setScope(Scope.DISTRIBUTED_ACK);
      RegionAttributes attr = factory.create();
      region = cache.createRegion("map", attr);

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static void closeCache() {
    try {
      cache.close();
      ds.disconnect();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }


  @Test
  public void testRemoveMultiVM() {
    // Commented the Test.As it is failing @ line no 133 : AssertionError
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Object obj1;
    Object[] objArr = new Object[1];
    for (int i = 1; i < 5; i++) {
      objArr[0] = i;
      vm0.invoke(RemoveDAckDUnitTest.class, "putMethod", objArr);
    }

    vm1.invoke(new CacheSerializableRunnable("get object") {
      @Override
      public void run2() throws CacheException {
        for (int i = 1; i < 5; i++) {
          region.get(i);
        }
      }
    });


    int i = 2;
    objArr[0] = i;
    vm0.invoke(RemoveDAckDUnitTest.class, "removeMethod", objArr);

    int Regsize = vm1.invoke(RemoveDAckDUnitTest::sizeMethod);
    assertEquals(3, Regsize);

  }// end of test case

  public static Object putMethod(Object ob) {
    Object obj = null;
    try {
      if (ob != null) {
        String str = "first";
        obj = region.put(ob, str);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed while region.put");
    }
    return obj;
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

  public static Object removeMethod(Object obR) {
    Object objR = null;
    try {
      if (obR != null) {
        objR = region.remove(obR);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed while region.remove");
    }
    return objR;

  }


}// end of class
