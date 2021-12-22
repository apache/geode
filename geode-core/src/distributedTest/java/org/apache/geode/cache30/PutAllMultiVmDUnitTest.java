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
 * PutAllMultiVmDUnitTest.java
 *
 * Created on September 1, 2005, 12:19 PM
 */
package org.apache.geode.cache30;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

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
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class PutAllMultiVmDUnitTest extends JUnit4DistributedTestCase { // TODO: reformat

  static Cache cache;
  static Properties props = new Properties();
  static Properties propsWork = new Properties();
  static DistributedSystem ds = null;
  static Region region;
  static Region mirroredRegion;
  static CacheTransactionManager cacheTxnMgr;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(() -> PutAllMultiVmDUnitTest.createCache(DataPolicy.REPLICATE));
    vm1.invoke(() -> PutAllMultiVmDUnitTest.createCache(DataPolicy.REPLICATE));
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(PutAllMultiVmDUnitTest::closeCache);
    vm1.invoke(PutAllMultiVmDUnitTest::closeCache);
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        cache = null;
      }
    });
  }

  public static void createCache(DataPolicy dataPolicy) {
    try {
      props.setProperty("log-level", "info");
      ds = (new PutAllMultiVmDUnitTest()).getSystem(props);
      cache = CacheFactory.create(ds);
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(dataPolicy);
      RegionAttributes attr = factory.create();
      region = cache.createRegion("map", attr);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }// end of createCache

  public static void createMirroredRegion() {
    try {
      AttributesFactory factory = new AttributesFactory();
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setScope(Scope.DISTRIBUTED_ACK);
      RegionAttributes attr = factory.create();
      mirroredRegion = cache.createRegion("mirrored", attr);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }// end of createCache

  public static void closeCache() {
    try {
      // System.out.println("closing cache cache cache cache cache 33333333");
      cache.close();
      ds.disconnect();
      // System.out.println("closed cache cache cache cache cache 44444444");
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }// end of closeCache

  private AsyncInvocation invokeClear(VM vm) {
    AsyncInvocation async = vm.invokeAsync(() -> region.clear());
    return async;
  }

  private AsyncInvocation invokeBulkOp(VM vm) {
    AsyncInvocation async = vm.invokeAsync(() -> {
      Map m = new HashMap();
      for (int i = 0; i < 20; i++) {
        m.put(i, "map" + i);
      }
      region.putAll(m);

      HashSet m2 = new HashSet();
      for (int i = 0; i < 10; i++) {
        m2.add(i);
      }
      region.removeAll(m2);
    });
    return async;
  }

  private void testBulkOpFromNonDataStore(final DataPolicy dataPolicy) throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm2.invoke(() -> PutAllMultiVmDUnitTest.createCache(dataPolicy));
    Random rand = new Random();
    for (int k = 0; k < 100; k++) {
      int shuffle = rand.nextInt(2);
      AsyncInvocation a1 = null;
      AsyncInvocation a2 = null;
      if (shuffle == 1) {
        a1 = invokeClear(vm1);
        a2 = invokeBulkOp(vm2);
      } else {
        a2 = invokeBulkOp(vm2);
        a1 = invokeClear(vm1);
      }
      a1.await();
      a2.await();

      // verify vm0 and vm1 has the same keys
      await().untilAsserted(() -> {
        Set vm0Contents = vm0.invoke(() -> {
          final HashSet<Object> keys = new HashSet<>();
          for (Object o : region.keySet()) {
            keys.add(o);
          }
          return keys;
        }); // replicated
        Set vm1Contents = vm1.invoke(() -> {
          final HashSet<Object> keys = new HashSet<>();
          for (Object o : region.keySet()) {
            keys.add(o);
          }
          return keys;
        }); // replicated
        assertThat(vm0Contents).isEqualTo(vm1Contents);
      });
    }
  }

  // tests methods
  @Test
  public void putAllAndPutDistributionToNormalMemberShouldBeSame() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    vm2.invoke(() -> PutAllMultiVmDUnitTest.createCache(DataPolicy.NORMAL));
    vm3.invoke(() -> PutAllMultiVmDUnitTest.createCache(DataPolicy.NORMAL));

    vm2.invoke(() -> {
      // create from NORMAL
      region.put("putKeyFromNormal", "value1");
    });

    vm3.invoke(() -> {
      // verify create is not distributed from NORMAL to NORMAL
      Region.Entry entry = region.getEntry("putKeyFromNormal");
      assertThat(entry).isEqualTo(null);

      // add putKeyFromNormal to local cache
      region.put("putKeyFromNormal", "vm3value1");
    });

    vm2.invoke(() -> {
      // update from NORMAL
      region.put("putKeyFromNormal", "value2");
    });

    vm3.invoke(() -> {
      // verify update is distributed from NORMAL to NORMAL
      Object value = region.getEntry("putKeyFromNormal").getValue();
      assertThat(value).isEqualTo("value2");
    });

    vm1.invoke(() -> {
      // verify update is distributed from NORMAL to REPLICATE
      Object value = region.getEntry("putKeyFromNormal").getValue();
      assertThat(value).isEqualTo("value2");
    });

    vm2.invoke(() -> {
      // create some PUTALL_CREATE events
      Map m = new HashMap();
      for (int i = 0; i < 20; i++) {
        m.put("putAllKeysFromNormal" + i, "map" + i);
      }
      region.putAll(m);

    });

    vm1.invoke(() -> {
      // let REPLICATE member to create some keys that NORMAL member does not have
      region.put("putKeyFromReplicate", "value1");
    });

    vm3.invoke(() -> {
      // verify NORMAL member received no events
      for (int i = 0; i < 20; i++) {
        Region.Entry entry = region.getEntry("putAllKeysFromNormal" + i);
        assertThat(entry).isNull();
      }

      // verify NORMAL member will not receive update for keys not in its local cache
      Region.Entry entry = region.getEntry("putKeyFromReplicate");
      assertThat(entry).isNull();
      Object value = region.getEntry("putKeyFromNormal").getValue();
      assertThat(value).isEqualTo("value2");

      // create keys in local cache
      Map m = new HashMap();
      for (int i = 0; i < 20; i++) {
        m.put("putAllKeysFromNormal" + i, "v3map" + i);
      }
      region.putAll(m);
    });

    vm2.invoke(() -> {
      // create some PUTALL_UPDATE events
      HashMap m = new HashMap();
      for (int i = 0; i < 20; i++) {
        m.put("putAllKeysFromNormal" + i, "newmap" + i);
      }
      region.putAll(m);

      // removeAll to remove a few existing entries
      HashSet m2 = new HashSet();
      for (int i = 0; i < 10; i++) {
        m2.add("putAllKeysFromNormal" + i);
      }
      region.removeAll(m2);
      for (int i = 0; i < 10; i++) {
        assertThat(region.getEntry("putAllKeysFromNormal" + i)).isNull();
      }

      // NORMAL starts a REMOVEALL with one key in its local cache, one key is not
      Object value = region.getEntry("putKeyFromNormal").getValue();
      assertThat(value).isEqualTo("value2");
      Region.Entry entry = region.getEntry("putKeyFromReplicate");
      assertThat(entry).isNull();

      m2 = new HashSet();
      m2.add("putKeyFromNormal");
      m2.add("putKeyFromReplicate");

      region.removeAll(m2);
      entry = region.getEntry("putKeyFromNormal");
      assertThat(entry).isNull();
      entry = region.getEntry("putKeyFromReplicate");
      assertThat(entry).isNull();
    });

    vm1.invoke(() -> {
      // verify REPLICATE member received ReomeAll event for both keys
      Region.Entry entry = region.getEntry("putKeyFromNormal");
      assertThat(entry).isNull();
      entry = region.getEntry("putKeyFromReplicate");
      assertThat(entry).isNull();
    });

    vm3.invoke(() -> {
      // Note: removeAll will not distribute to NORMAL member
      for (int i = 0; i < 20; i++) {
        Object value = region.getEntry("putAllKeysFromNormal" + i).getValue();
        assertThat(value).isEqualTo("newmap" + i);
      }

      // verify NORMAL member received PUTALL_UPDATE only
      // ReomeAll event will not distribute to NORMAL member
      Object value = region.getEntry("putKeyFromNormal").getValue();
      assertThat(value).isEqualTo("value2");
      Region.Entry entry = region.getEntry("putKeyFromReplicate");
      assertThat(entry).isNull();
    });
  }

  @Test
  public void testPutAllFromAccessor() throws InterruptedException {
    testBulkOpFromNonDataStore(DataPolicy.EMPTY);
  }

  @Test
  public void testPutAllFromNormal() throws InterruptedException {
    testBulkOpFromNonDataStore(DataPolicy.NORMAL);
  }

  @Test
  public void testPutAllFromPreload() throws InterruptedException {
    testBulkOpFromNonDataStore(DataPolicy.PRELOADED);
  }

  @Test
  public void testSimplePutAll() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable clear = new CacheSerializableRunnable("clear") {
      @Override
      public void run2() throws CacheException {
        try {
          region.clear();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    };// end of clear

    vm0.invoke(new CacheSerializableRunnable("testSimplePutAll1") {
      @Override
      public void run2() throws CacheException {
        int cntr = 0, cntr1 = 0;
        for (int i = 1; i < 6; i++) {
          region.put(i, "testSimplePutAll" + i);
          cntr++;
        }

        int size1 = region.size();
        Map m = new HashMap();
        for (int i = 6; i < 27; i++) {
          m.put(i, "map" + i);
          cntr++;
          cntr1++;
        }

        region.putAll(m);
        int size2 = region.size();

        assertEquals(cntr, region.size());
        assertEquals(cntr1, (size2 - size1));
        assertEquals(true, region.containsKey(10));
        assertEquals(true, region.containsValue("map12"));
      }
    });

    vm0.invoke(clear);

    vm1.invoke(new CacheSerializableRunnable("create mirrored region") {
      @Override
      public void run2() throws CacheException {
        createMirroredRegion();
      }
    });

    vm0.invoke(new CacheSerializableRunnable("testSimplePutAll2") {
      @Override
      public void run2() throws CacheException {
        // assertIndexDetailsEquals(0, region.size());
        createMirroredRegion();
        cacheTxnMgr = cache.getCacheTransactionManager();
        int cntr = 0;
        for (int i = 1; i < 6; i++) {
          mirroredRegion.put(i, "testSimplePutAll" + i);
          cntr++;
        }

        int size1 = mirroredRegion.size();
        Map m = new HashMap();
        for (int i = 6; i < 27; i++) {
          m.put(i, "map" + i);
          cntr++;
        }

        // Disabled until putAll works in tx
        // cacheTxnMgr.begin();
        // mirroredRegion.putAll(m);
        // cacheTxnMgr.rollback();

        assertEquals(size1, mirroredRegion.size());
        assertEquals(false, mirroredRegion.containsKey(10));
        assertEquals(false, mirroredRegion.containsValue("map12"));

        // cacheTxnMgr.begin();
        mirroredRegion.putAll(m);
        // cacheTxnMgr.commit();

        // int size2 = mirroredRegion.size();

        assertEquals(cntr, mirroredRegion.size());
        assertEquals(true, mirroredRegion.containsKey(10));
        assertEquals(true, mirroredRegion.containsValue("map12"));

        // sharing the size of region of vm0 in vm1
        mirroredRegion.put("size", mirroredRegion.size());
      }
    });

    vm1.invoke(new CacheSerializableRunnable("testSimplePutAll3") {
      @Override
      public void run2() throws CacheException {
        Integer i = (Integer) mirroredRegion.get("size");
        int cntr = i;
        assertEquals(cntr, (mirroredRegion.size() - 1));
        assertEquals(true, mirroredRegion.containsKey(10));
        assertEquals(true, mirroredRegion.containsValue("map12"));
      }
    });

  }// end of testSimplePutAll

  @Test
  public void testPutAllExceptions() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(new CacheSerializableRunnable("testPutAllExceptions1") {
      @Override
      public void run2() throws CacheException {
        int cntr = 0;
        // int cntr1 = 0;
        for (int i = 1; i < 6; i++) {
          region.put(i, "testSimplePutAll" + i);
          cntr++;
        }

        Map m = new TreeMap();// to verify the assertions
        for (int i = 6; i < 27; i++) {
          if (i == 16) {
            m.put(i, null);
          } else {
            m.put(i, "map" + i);
          }
        }

        try {
          region.putAll(m);
          fail("Expect NullPointerException");
        } catch (NullPointerException ex) {
          // do nothing
        }

        assertEquals(5, region.size());
        assertEquals(false, region.containsKey(10));
        assertEquals(false, region.containsValue("map12"));
        assertEquals(false, region.containsKey(20));
        assertEquals(false, region.containsValue("map21"));
      }
    });


    vm1.invoke(new CacheSerializableRunnable("create mirrored region") {
      @Override
      public void run2() throws CacheException {
        createMirroredRegion();
      }
    });


    vm0.invoke(new CacheSerializableRunnable("testPutAllExceptions2") {
      @Override
      public void run2() throws CacheException {
        // assertIndexDetailsEquals(0, region.size());
        createMirroredRegion();

        for (int i = 1; i < 6; i++) {
          mirroredRegion.put(i, "testSimplePutAll" + i);
        }

        Map m = new TreeMap();// to verify the assertions
        for (int i = 6; i < 27; i++) {
          if (i == 16) {
            m.put(i, null);
          } else {
            m.put(i, "map" + i);
          }
        }

        try {
          mirroredRegion.putAll(m);
          fail("Expect NullPointerException");
        } catch (NullPointerException ex) {
          // do nothing
        }

        assertEquals(5, mirroredRegion.size());
        assertEquals(false, mirroredRegion.containsKey(10));
        assertEquals(false, mirroredRegion.containsValue("map12"));
        assertEquals(false, region.containsKey(20));
        assertEquals(false, region.containsValue("map21"));

        // sharing the size of region of vm0 in vm1
        mirroredRegion.put("size", mirroredRegion.size());
      }
    });

    vm1.invoke(new CacheSerializableRunnable("testPutAllExceptions3") {
      @Override
      public void run2() throws CacheException {
        Integer i = (Integer) mirroredRegion.get("size");
        int cntr = i;
        assertEquals(cntr, (mirroredRegion.size() - 1));
        assertEquals(false, mirroredRegion.containsKey(10));
        assertEquals(false, mirroredRegion.containsValue("map12"));
        assertEquals(false, mirroredRegion.containsKey(20));
        assertEquals(false, mirroredRegion.containsValue("map21"));
      }
    });


  }// end of testPutAllExceptions

  @Test
  public void testPutAllExceptionHandling() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    // VM vm1 = host.getVM(1);

    vm0.invoke(new CacheSerializableRunnable("testPutAllExceptionHandling1") {
      @Override
      public void run2() throws CacheException {
        Map m = new HashMap();
        m = null;
        try {
          region.putAll(m);
          fail("Should have thrown NullPointerException");
        } catch (NullPointerException ex) {
          // pass
        }

        region.localDestroyRegion();
        try {
          Map m1 = new HashMap();
          for (int i = 1; i < 21; i++) {
            m1.put(i, Integer.toString(i));
          }

          region.putAll(m1);
          fail("Should have thrown RegionDestroyedException");
        } catch (RegionDestroyedException ex) {
          // pass
        }

      }
    });

  }// testPutAllExceptionHandling


}// end of PutAllMultiVmDUnitTest
