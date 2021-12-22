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
 * DistAckMapMethodsDUnitTest.java
 *
 * Created on August 4, 2005, 12:36 PM
 */
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class DistAckMapMethodsDUnitTest extends JUnit4DistributedTestCase { // TODO: reformat

  static Cache cache;
  static Properties props = new Properties();
  static DistributedSystem ds = null;
  static Region region;
  static Region mirroredRegion;
  static Region remRegion;
  static boolean afterDestroy = false;


  // helper class referece objects
  static Object afterDestroyObj;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(DistAckMapMethodsDUnitTest::createCache);
    vm1.invoke(DistAckMapMethodsDUnitTest::createCache);
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(DistAckMapMethodsDUnitTest::closeCache);
    vm1.invoke(DistAckMapMethodsDUnitTest::closeCache);
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        cache = null;
      }
    });
  }

  public static void createCache() {
    try {
      // props.setProperty(DistributionConfig.SystemConfigurationProperties.MCAST_PORT, "1234");
      // ds = DistributedSystem.connect(props);
      ds = (new DistAckMapMethodsDUnitTest()).getSystem(props);
      cache = CacheFactory.create(ds);
      AttributesFactory factory = new AttributesFactory();
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

  public static void createMirroredRegion() {
    try {
      AttributesFactory factory1 = new AttributesFactory();
      factory1.setScope(Scope.DISTRIBUTED_ACK);
      factory1.setDataPolicy(DataPolicy.REPLICATE);
      RegionAttributes attr1 = factory1.create();
      mirroredRegion = cache.createRegion("mirrored", attr1);

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static void createRegionToTestRemove() {
    try {
      AttributesFactory factory2 = new AttributesFactory();
      factory2.setScope(Scope.DISTRIBUTED_ACK);
      CacheWriter cacheWriter = new RemoveCacheWriter();
      CacheListener cacheListener = new RemoveCacheListener();
      factory2.setCacheWriter(cacheWriter);
      factory2.setCacheListener(cacheListener);
      RegionAttributes attr2 = factory2.create();
      remRegion = cache.createRegion("remove", attr2);

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  // testMethods

  @Test
  public void testPutMethod() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    Object obj1;
    // put from one and get from other
    int i = 1;
    Object[] objArr = new Object[1];
    objArr[0] = "" + i;
    // Integer in = new Integer(i);
    // objArr[0] = (Object) in;
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    obj1 = vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);
    if (obj1 == null) {
      fail("region.put(key, value) from one vm does not match with region.get(key) from other vm");
    }

    // put from both vms for same key
    i = 2;
    objArr[0] = "" + i;
    // in = new Integer(i);
    // objArr[0] = (Object) in;
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    obj1 = vm1.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    if (obj1 != null) {// here if some dummy object is returned on first time put then that should
                       // be checked
      fail("failed while region.put from both vms for same key");
    }
  }

  @Test
  public void testRemoveMethod() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    Object obj1, obj2;
    boolean ret;
    // put from one and get from other
    int i = 1;
    Object[] objArr = new Object[1];
    objArr[0] = "" + i;
    // Integer in = new Integer(i);
    // objArr[0] = (Object) in;
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "removeMethod", objArr);
    // validate if vm0 has that key value entry
    ret = vm0.invoke(() -> containsKeyMethod("" + i));
    if (ret) {// if returned true means that the key is still there
      fail("region.remove failed with distributed ack scope");
    }

    // test if the correct value is returned
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    obj1 = vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);// to make sure that
                                                                             // vm1 region has the
                                                                             // entry
    obj2 = vm1.invoke(DistAckMapMethodsDUnitTest.class, "removeMethod", objArr);
    LogWriterUtils.getLogWriter().fine("111111111" + obj1);
    LogWriterUtils.getLogWriter().fine("2222222222" + obj2);
    if (obj1 == null) {
      fail("region1.getMethod returned null");
    }
    if (!(obj1.equals(obj2))) {
      fail("region.remove failed with distributed ack scope");
    }
  }

  @Test
  public void testRemoveMethodDetails() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(DistAckMapMethodsDUnitTest::createRegionToTestRemove);
    vm1.invoke(DistAckMapMethodsDUnitTest::createRegionToTestRemove);

    vm0.invoke(DistAckMapMethodsDUnitTest::removeMethodDetails);
    vm1.invoke(new CacheSerializableRunnable("testRemoveMethodDetails") {
      @Override
      public void run2() throws CacheException {
        Object ob1 = remRegion.get(1);
        assertEquals("beforeDestroy", ob1.toString());
        // wait till listeber switches afterDestroy to true
        // while(!afterDestroy){
        // //wait
        // }
        assertEquals("afterDestroy", remRegion.get(3).toString());
      }
    });
  }// end of testRemoveMethodDetails

  @Test
  public void testIsEmptyMethod() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    // boolean ret;
    // put from one and get from other
    int i = 1;
    Object[] objArr = new Object[1];
    objArr[0] = "" + i;
    // Integer in = new Integer(i);
    // objArr[0] = (Object) in;
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    boolean val = vm1.invoke(DistAckMapMethodsDUnitTest::isEmptyMethod);
    if (!val) {// val should be true
      fail("Failed in region.isEmpty");
    }

    vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);
    boolean val1 = vm1.invoke(DistAckMapMethodsDUnitTest::isEmptyMethod);
    if (val1) {
      fail("Failed in region.isEmpty");
    }
  }

  @Test
  public void testContainsValueMethod() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    // boolean ret;
    // put from one and get from other
    int i = 1;
    Object[] objArr = new Object[1];
    objArr[0] = "" + i;
    // Integer in = new Integer(i);
    // objArr[0] = (Object) in;
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    boolean val = vm1.invoke(() -> containsValueMethod("first"));
    if (val) {// val should be false.
      fail("Failed in region.ContainsValue");
    }

    vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);
    boolean val1 = vm1.invoke(() -> containsValueMethod("first"));
    if (!val1) {// val1 should be true.
      fail("Failed in region.ContainsValue");
    }
  }

  @Test
  public void testKeySetMethod() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    int i = 1;
    Object[] objArr = new Object[1];
    objArr[0] = "" + i;
    // Integer in = new Integer(i);
    // objArr[0] = (Object) in;
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    int temp = vm1.invoke(DistAckMapMethodsDUnitTest::keySetMethod);
    if (temp != 0) {
      fail("failed in keySetMethodtest method");
    }

    vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);// to make sure that vm1
                                                                      // region has the entry
    temp = vm1.invoke(DistAckMapMethodsDUnitTest::keySetMethod);
    if (temp == 0) {
      fail("failed in keySetMethodtest method");
    }
    // in the above scenarion we can test this for mirrorred region scenarion as well
    temp = 0;
    vm0.invoke(DistAckMapMethodsDUnitTest::createMirroredRegion);
    vm1.invoke(DistAckMapMethodsDUnitTest::createMirroredRegion);
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    temp = vm1.invoke(DistAckMapMethodsDUnitTest::keySetMethod);
    if (temp == 0) {
      fail("failed in keySetMethodtest method");
    }
  }


  @Test
  public void testEntrySetMethod() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    int i = 1;
    Object[] objArr = new Object[1];
    objArr[0] = "" + i;
    // Integer in = new Integer(i);
    // objArr[0] = (Object) in;
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    int temp = vm1.invoke(DistAckMapMethodsDUnitTest::entrySetMethod);
    if (temp != 0) {
      fail("failed in entrySetMethodtest method");
    }

    vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);// to make sure that vm1
                                                                      // region has the entry
    temp = vm1.invoke(DistAckMapMethodsDUnitTest::entrySetMethod);
    if (temp == 0) {
      fail("failed in entrySetMethodtest method");
    }
    // in the above scenarion we can test this for mirrorred region scenarion as well
    temp = 0;
    vm0.invoke(DistAckMapMethodsDUnitTest::createMirroredRegion);
    vm1.invoke(DistAckMapMethodsDUnitTest::createMirroredRegion);
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putOnMirroredRegion", objArr);
    temp = vm1.invoke(DistAckMapMethodsDUnitTest::entrySetMethod);
    if (temp == 0) {
      fail("failed in entrySetMethodtest method");
    }
  }

  @Test
  public void testSizeMethod() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    int i = 1, j = 0;
    Object[] objArr = new Object[1];
    objArr[0] = "" + i;
    // Integer in = new Integer(i);
    // objArr[0] = (Object) in;
    vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
    j = vm1.invoke(DistAckMapMethodsDUnitTest::sizeMethod);
    if (j != 0) {
      fail("failed in region.size method");
    }

    vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);// to make sure that vm1
                                                                      // region has the entry
    j = vm1.invoke(DistAckMapMethodsDUnitTest::sizeMethod);
    if (j == 0) {
      fail("failed in region.size method");
    }
  }

  @Test
  public void testallMethodsArgs() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(DistAckMapMethodsDUnitTest::allMethodsArgs);
  }


  // following is the implementation of the methods of Map to use in dunit test cases.
  public static Object putMethod(Object ob) {
    Object obj = null;
    try {
      if (ob != null) {
        String str = "first";
        obj = region.put(ob, str);
      }
    } catch (Exception ex) {
      fail("Failed while region.put");
    }
    return obj;
  }

  public static Object getMethod(Object ob) {
    Object obj = null;
    try {
      obj = region.get(ob);

    } catch (Exception ex) {
      fail("Failed while region.get");
    }
    return obj;
  }

  public static Object removeMethod(Object ob) {
    Object obj = null;
    try {
      obj = region.remove(ob);
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed while region.remove");
    }
    return obj;
  }

  public static boolean containsKeyMethod(Object ob) {
    boolean flag = false;
    try {
      flag = region.containsKey(ob);
    } catch (Exception ex) {
      fail("Failed while region.containsKey");
    }
    return flag;
  }

  public static boolean isEmptyMethod() {
    boolean flag = false;
    try {
      flag = region.isEmpty();
    } catch (Exception ex) {
      fail("Failed while region.isEmpty");
    }
    return flag;
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

  public static int keySetMethod() {
    Set set = new HashSet();
    int i = 0;
    try {
      set = region.keySet();
      i = set.size();
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed while region.keySet");
    }
    return i;
  }

  public static int entrySetMethod() {
    Set set = new HashSet();
    int i = 0;
    try {
      set = region.entrySet();
      i = set.size();
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed while region.entrySet");
    }
    return i;
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

  // following are methods for put on and get from mirrored regions

  public static Object putOnMirroredRegion(Object ob) {
    Object obj = null;
    try {
      String str = "mirror";
      obj = mirroredRegion.put(ob, str);
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed while mirroredRegion.put");
    }
    return obj;
  }

  public static Object getFromMirroredRegion(Object ob) {
    Object obj = null;
    try {
      obj = mirroredRegion.get(ob);

    } catch (Exception ex) {
      fail("Failed while mirroredRegion.get");
    }
    return obj;
  }

  public static void removeMethodDetails() {
    Object ob1;
    // Object ob2;
    Integer inOb1 = 1;
    try {
      region.put(inOb1, "first");
      ob1 = region.remove(inOb1);
      assertEquals("first", ob1.toString());
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    // to test EntryNotFoundException
    try {
      region.remove(2);
      // fail("Should have thrown EntryNotFoundException");
    } // catch (EntryNotFoundException e){
    catch (Exception e) {
      // pass
      // e.printStackTrace();
    }

    // to test NullPointerException
    try {
      Integer inOb2 = 2;
      region.put(inOb2, "second");
      inOb2 = null;
      region.remove(inOb2);
      fail("Should have thrown NullPointerException ");
    } catch (NullPointerException e) {
      // pass
    }

    // to test the cache writers and listeners
    try {
      // createRegionToTestRemove();
      Integer inOb2 = 2;
      remRegion.put(inOb2, "second");
      remRegion.remove(inOb2);

      // to test cacheWriter
      inOb2 = 1;
      assertEquals("beforeDestroy", remRegion.get(inOb2).toString());

      // wait till listeber switches afterDestroy to true
      while (!afterDestroy) {
      }
      // to test cacheListener
      inOb2 = 3;
      assertEquals("afterDestroy", remRegion.get(inOb2).toString());

      // verify that entryEventvalue is correct for listener
      assertNotNull(afterDestroyObj);

    } catch (Exception ex) {
      ex.printStackTrace();
    }


  }// end of removeMethodDetail

  public static void allMethodsArgs() {
    // testing args for put method
    try {
      region.put(1, "first");
      region.put(2, "second");
      region.put(3, "third");

      // test args for get method
      Object ob1 = region.get(1);
      assertEquals("first", ob1.toString());

      // test args for containsKey method
      boolean val1 = region.containsKey(2);
      assertEquals(true, val1);

      // test args for containsKey method
      boolean val2 = region.containsValue("second");
      // assertIndexDetailsEquals(true, val2);

      // test args for remove method
      try {
        region.remove(3);
      } // catch (EntryNotFoundException ex){
      catch (Exception ex) {
        ex.printStackTrace();
        fail("failed while region.remove(new Object())");
      }

      // verifying the correct exceptions are thrown by the methods

      Object key = null, value = null;
      // testing put method
      try {
        region.put(key, value);
        fail("should have thrown NullPointerException");
      } catch (NullPointerException iex) {
        // pass
      }

      // testing containsValue method
      try {
        region.containsValue(value);
        fail("should have thrown NullPointerException");
      } catch (NullPointerException iex) {
        // pass
      }

      // RegionDestroyedException
      key = 5;
      value = "fifth";

      region.localDestroyRegion();
      // test put method
      try {
        region.put(key, value);
        fail("should have thrown RegionDestroyedException");
      } catch (RegionDestroyedException iex) {
        // pass
      }

      // test remove method
      try {
        region.remove(key);
        fail("should have thrown RegionDestroyedException");
      } catch (RegionDestroyedException iex) {
        // pass
      }

      // test containsValue method
      try {
        region.containsValue(value);
        fail("should have thrown RegionDestroyedException");
      } catch (RegionDestroyedException iex) {
        // pass
      }

      // test size method
      try {
        region.size();
        fail("should have thrown RegionDestroyedException");
      } catch (RegionDestroyedException iex) {
        // pass
      }

      // test keySet method
      try {
        region.keySet();
        fail("should have thrown RegionDestroyedException");
      } catch (RegionDestroyedException iex) {
        // pass
      }

      // test entrySet method
      try {
        region.entrySet();
        fail("should have thrown RegionDestroyedException");
      } catch (RegionDestroyedException iex) {
        // pass
      }


    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }// end of allMethodsArgs

  // helper classes

  static class RemoveCacheWriter extends CacheWriterAdapter {

    @Override
    public void beforeDestroy(EntryEvent entryEvent)
        throws org.apache.geode.cache.CacheWriterException {
      Integer o1 = 1;
      remRegion.put(o1, "beforeDestroy");
    }

  }// end of RemoveCacheWriter


  static class RemoveCacheListener extends CacheListenerAdapter {

    @Override
    public void afterDestroy(EntryEvent entryEvent)
        throws org.apache.geode.cache.CacheWriterException {
      Integer o1 = 3;
      remRegion.put(o1, "afterDestroy");

      afterDestroyObj = entryEvent.getKey();

      // to continue main thread where region.remove has actually occurred
      afterDestroy = true;
    }

  }// end of RemoveCacheListener


}// end of class
