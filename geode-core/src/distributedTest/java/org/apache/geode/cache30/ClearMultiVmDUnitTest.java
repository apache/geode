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
 * ClearMultiVmDUnitTest.java
 *
 * Created on August 11, 2005, 7:37 PM
 */
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class ClearMultiVmDUnitTest extends JUnit4DistributedTestCase { // TODO: reformat

  static Cache cache;
  static Properties props = new Properties();
  static Properties propsWork = new Properties();
  static DistributedSystem ds = null;
  static Region region;
  static Region paperWork;
  static Region mirroredRegion;
  static CacheTransactionManager cacheTxnMgr;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(() -> ClearMultiVmDUnitTest.createCache());
    vm1.invoke(() -> ClearMultiVmDUnitTest.createCache());
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(() -> ClearMultiVmDUnitTest.closeCache());
    vm1.invoke(() -> ClearMultiVmDUnitTest.closeCache());
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
      ds = (new ClearMultiVmDUnitTest()).getSystem(props);
      cache = CacheFactory.create(ds);
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setConcurrencyChecksEnabled(true);
      RegionAttributes attr = factory.create();
      region = cache.createRegion("map", attr);

      AttributesFactory factory1 = new AttributesFactory();
      factory1.setScope(Scope.DISTRIBUTED_ACK);
      factory.setConcurrencyChecksEnabled(true);
      factory1.setDataPolicy(DataPolicy.REPLICATE);
      paperWork = cache.createRegion("paperWork", factory1.create());

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

  // test methods

  @Test
  public void testClearSimpleScenarios() {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // verifying Single VM clear functionalities
    vm0.invoke(new CacheSerializableRunnable("temp1") {
      @Override
      public void run2() throws CacheException {
        region.put(new Integer(1), "first");
        region.put(new Integer(2), "second");
        region.put(new Integer(3), "third");
        region.clear();
        assertEquals(0, region.size());
      }
    });

    vm1.invoke(new CacheSerializableRunnable("temp1vm1") {
      @Override
      public void run2() throws CacheException {
        assertEquals(0, region.size());
      }
    });

    // verifying Single VM and single transaction clear functionalities
    vm1.invoke(new CacheSerializableRunnable("temp2") {
      @Override
      public void run2() throws CacheException {
        try {
          region.put(new Integer(1), "first");
          region.put(new Integer(2), "second");
          region.put(new Integer(3), "third");
          cacheTxnMgr = cache.getCacheTransactionManager();
          cacheTxnMgr.begin();
          region.put(new Integer(4), "forth");
          try {
            region.clear();
            fail("expected exception not thrown");
          } catch (UnsupportedOperationInTransactionException e) {
            // expected
          }
          region.put(new Integer(5), "fifth");
          cacheTxnMgr.commit();
          assertEquals(5, region.size());
          assertEquals("fifth", region.get(new Integer(5)).toString());
        } catch (CacheException ce) {
          ce.printStackTrace();
        } finally {
          if (cacheTxnMgr.exists()) {
            try {
              cacheTxnMgr.commit();
            } catch (Exception cce) {
              cce.printStackTrace();
            }
          }
        }

      }
    });

    // verifying that region.clear does not clear the entries from sub region
    vm0.invoke(new CacheSerializableRunnable("temp3") {
      @Override
      public void run2() throws CacheException {
        region.put(new Integer(1), "first");
        region.put(new Integer(2), "second");
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        RegionAttributes attr = factory.create();
        Region subRegion = region.createSubregion("subr", attr);
        subRegion.put(new Integer(3), "third");
        subRegion.put(new Integer(4), "forth");
        region.clear();
        assertEquals(0, region.size());
        assertEquals(2, subRegion.size());
      }
    });

  }// end of test case

  @Test
  public void testClearMultiVM() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // put 3 key/values in vm0 and get from vm1
    Object[] objArr = new Object[1];
    // Integer in = new Integer(i);
    // objArr[0] = (Object) in;
    for (int i = 1; i < 4; i++) {
      objArr[0] = "" + i;
      vm0.invoke(ClearMultiVmDUnitTest.class, "putMethod", objArr);
      vm1.invoke(ClearMultiVmDUnitTest.class, "getMethod", objArr);
    }

    AsyncInvocation as1 = vm0.invokeAsync(() -> ClearMultiVmDUnitTest.firstVM());
    AsyncInvocation as2 = vm1.invokeAsync(() -> ClearMultiVmDUnitTest.secondVM());
    ThreadUtils.join(as1, 30 * 1000);
    ThreadUtils.join(as2, 30 * 1000);

    if (as1.exceptionOccurred()) {
      Assert.fail("as1 failed", as1.getException());
    }

    if (as2.exceptionOccurred()) {
      Assert.fail("as2 failed", as2.getException());
    }

    int j = vm0.invoke(() -> ClearMultiVmDUnitTest.sizeMethod());
    assertEquals(0, j);

    j = vm1.invoke(() -> ClearMultiVmDUnitTest.sizeMethod());
    assertEquals(1, j);



    int i = 6;
    objArr[0] = "" + i;
    vm1.invoke(ClearMultiVmDUnitTest.class, "getMethod", objArr);

    boolean val = vm1.invoke(() -> containsValueMethod("secondVM"));
    assertEquals(true, val);

  }// end of testClearMultiVM

  @Test
  public void testClearExceptions() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm1.invoke(() -> ClearMultiVmDUnitTest.localDestroyRegionMethod());
    vm0.invoke(new CacheSerializableRunnable("exception in vm0") {
      @Override
      public void run2() throws CacheException {
        try {
          region.clear();
        } catch (RegionDestroyedException rdex) {
          fail("Should NOT have thrown RegionDestroyedException");
        }
      }
    });

    vm1.invoke(new CacheSerializableRunnable("exception in vm1") {
      @Override
      public void run2() throws CacheException {
        try {
          region.clear();
          fail("Should have thrown RegionDestroyedException");
        } catch (RegionDestroyedException rdex) {
          // pass
        }
      }
    });

  }// end of testClearExceptions

  @Test
  public void testGiiandClear() throws Throwable {
    if (false) {
      getSystem().getLogWriter().severe("testGiiandClear skipped because of bug 34963");
    } else {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);

      SerializableRunnable create = new CacheSerializableRunnable("create mirrored region") {
        @Override
        public void run2() throws CacheException {
          AttributesFactory factory1 = new AttributesFactory();
          factory1.setScope(Scope.DISTRIBUTED_ACK);
          factory1.setDataPolicy(DataPolicy.REPLICATE);
          RegionAttributes attr1 = factory1.create();
          mirroredRegion = cache.createRegion("mirrored", attr1);
          // reset slow
          org.apache.geode.internal.cache.InitialImageOperation.slowImageProcessing = 0;
        }
      };


      vm0.invoke(create);

      vm0.invoke(new CacheSerializableRunnable("put initial data") {
        @Override
        public void run2() throws CacheException {
          for (int i = 0; i < 1000; i++) {
            mirroredRegion.put(new Integer(i), (new Integer(i)).toString());
          }
        }
      });

      // slow down image processing to make it more likely to get async updates
      vm1.invoke(new SerializableRunnable("set slow image processing") {
        @Override
        public void run() {
          // if this is a no_ack test, then we need to slow down more because of the
          // pauses in the nonblocking operations
          int pause = 50;
          org.apache.geode.internal.cache.InitialImageOperation.slowImageProcessing = pause;
        }
      });

      // now do the get initial image in vm1
      AsyncInvocation async1 = vm1.invokeAsync(create);

      // try to time a distributed clear to happen in the middle of gii
      vm0.invoke(new SerializableRunnable("call clear when gii") {
        @Override
        public void run() {
          try {
            Thread.sleep(3 * 1000);
          } catch (InterruptedException ex) {
            fail("interrupted");
          }
          mirroredRegion.clear();
          assertEquals(0, mirroredRegion.size());
        }
      });

      ThreadUtils.join(async1, 30 * 1000);
      if (async1.exceptionOccurred()) {
        Assert.fail("async1 failed", async1.getException());
      }

      SerializableRunnable validate = new CacheSerializableRunnable("validate for region size") {
        @Override
        public void run2() throws CacheException {
          assertEquals(0, mirroredRegion.size());
        }
      };

      vm0.invoke(validate);
      vm1.invoke(validate);
    }

  }// end of testGiiandClear


  // remote vm methods

  public static void firstVM() {
    // put one entry
    int i = 5;
    region.put(i, "firstVM");

    // test localClear
    region.localClear();
    assertFalse(region.containsKey(i));

    paperWork.put("localClear", "true");

    // wait unit clear on 2nd VM
    boolean val = false;
    while (!val) {
      val = paperWork.containsKey("localClearVerified");
    }
    region.put("key10", "value");
    region.put("key11", "value");

    // test distributed clear
    region.clear();
    paperWork.put("clear", "value");

    val = false;
    while (!val) {
      val = paperWork.containsKey("clearVerified");
    }

  }// end of firstVM

  public static void secondVM() {
    // verify that localClear on the other VM does not affect size on this VM
    boolean val = false;
    while (!val) {
      val = paperWork.containsKey("localClear");
    }
    assertEquals(3, region.size());

    paperWork.put("localClearVerified", "value");

    // wait for clear
    val = false;
    while (!val) {
      val = paperWork.containsKey("clear");
    }
    assertEquals(0, region.size());

    paperWork.put("clearVerified", "value");
    region.put("" + 6, "secondVM");
  }// end of secondVM

  // helper methods


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

  public static void localDestroyRegionMethod() {
    try {
      region.localDestroyRegion();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }// end of localDestroyRegionMethod

  public static void clearExceptions() {
    try {
      region.clear();
      // fail("Should have thrown RegionDestroyedException");
    } catch (RegionDestroyedException rdex) {
      // pass
    }
  }// end of clearExceptions

}// end of class
