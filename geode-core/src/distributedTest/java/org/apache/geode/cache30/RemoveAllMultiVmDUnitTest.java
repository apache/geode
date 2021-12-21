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
 * RemoveAllMultiVmDUnitTest.java
 *
 * Adapted from PutAllMultiVmDUnitTest
 */
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
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
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class RemoveAllMultiVmDUnitTest extends JUnit4DistributedTestCase { // TODO: reformat

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
    vm0.invoke(() -> RemoveAllMultiVmDUnitTest.createCache());
    vm1.invoke(() -> RemoveAllMultiVmDUnitTest.createCache());
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(() -> RemoveAllMultiVmDUnitTest.closeCache());
    vm1.invoke(() -> RemoveAllMultiVmDUnitTest.closeCache());
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
      ds = (new RemoveAllMultiVmDUnitTest()).getSystem(props);
      cache = CacheFactory.create(ds);
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
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


  // tests methods

  @Test
  public void testLocalRemoveAll() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    vm0.invoke(new CacheSerializableRunnable("testLocalRemoveAll") {
      @Override
      public void run2() throws CacheException {
        int cntr = 0, cntr1 = 0;
        for (int i = 1; i < 6; i++) {
          region.put(Integer.valueOf(i), "testLocalRemoveAll" + i);
          cntr++;
        }

        int size1 = region.size();
        assertEquals(5, size1);

        region.removeAll(Collections.EMPTY_SET);
        assertEquals(size1, region.size());
        region.removeAll(Collections.singleton(Integer.valueOf(666)));
        assertEquals(size1, region.size());
        assertEquals(true, region.containsKey(Integer.valueOf(1)));
        region.removeAll(Collections.singleton(Integer.valueOf(1)));
        assertEquals(false, region.containsKey(Integer.valueOf(1)));
        assertEquals(size1 - 1, region.size());
        size1--;
        region.removeAll(Arrays.asList(Integer.valueOf(2), Integer.valueOf(3)));
        assertEquals(size1 - 2, region.size());
        size1 -= 2;
      }
    });
  }

  @Test
  public void testLocalTxRemoveAll() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    vm0.invoke(new CacheSerializableRunnable("testSimpleRemoveAllTx") {
      @Override
      public void run2() throws CacheException {
        cacheTxnMgr = cache.getCacheTransactionManager();
        int cntr = 0;
        for (int i = 1; i < 6; i++) {
          region.put(Integer.valueOf(i), "testLocalTxRemoveAll" + i);
          cntr++;
        }

        int size1 = region.size();
        assertEquals(5, size1);

        cacheTxnMgr.begin();
        region.removeAll(Arrays.asList(Integer.valueOf(1), Integer.valueOf(2)));
        cacheTxnMgr.rollback();

        assertEquals(size1, region.size());

        cacheTxnMgr.begin();
        region
            .removeAll(Arrays.asList(Integer.valueOf(666), Integer.valueOf(1), Integer.valueOf(2)));
        cacheTxnMgr.commit();

        int size2 = region.size();

        assertEquals(size1 - 2, size2);
        assertEquals(true, region.containsKey(Integer.valueOf(3)));
        assertEquals(false, region.containsKey(Integer.valueOf(2)));
        assertEquals(false, region.containsKey(Integer.valueOf(1)));
      }
    });
  }

  @Test
  public void testDistributedRemoveAll() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm1.invoke(new CacheSerializableRunnable("create mirrored region") {
      @Override
      public void run2() throws CacheException {
        createMirroredRegion();
      }
    });

    vm0.invoke(new CacheSerializableRunnable("testDistributedRemoveAll1") {
      @Override
      public void run2() throws CacheException {
        createMirroredRegion();
        int cntr = 0, cntr1 = 0;
        for (int i = 1; i < 6; i++) {
          mirroredRegion.put(Integer.valueOf(i), "testDistributedRemoveAll" + i);
          cntr++;
        }

        int size1 = mirroredRegion.size();
        assertEquals(5, size1);

        mirroredRegion.removeAll(Collections.EMPTY_SET);
        assertEquals(size1, mirroredRegion.size());
        mirroredRegion.removeAll(Collections.singleton(Integer.valueOf(666)));
        assertEquals(size1, mirroredRegion.size());
        assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(1)));
        mirroredRegion.removeAll(Collections.singleton(Integer.valueOf(1)));
        assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(1)));
        assertEquals(size1 - 1, mirroredRegion.size());
        size1--;
        mirroredRegion.removeAll(Arrays.asList(Integer.valueOf(2), Integer.valueOf(3)));
        assertEquals(size1 - 2, mirroredRegion.size());
        size1 -= 2;
      }
    });

    vm1.invoke(new CacheSerializableRunnable("testDistributedRemoveAllVerifyRemote") {
      @Override
      public void run2() throws CacheException {
        assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(5)));
        assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(4)));
        assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(3)));
        assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(2)));
        assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(1)));
        assertEquals(2, mirroredRegion.size());
      }
    });
  }

  @Test
  public void testDistributedTxRemoveAll() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm1.invoke(new CacheSerializableRunnable("create mirrored region") {
      @Override
      public void run2() throws CacheException {
        createMirroredRegion();
      }
    });

    vm0.invoke(new CacheSerializableRunnable("testDistributedTxRemoveAll1") {
      @Override
      public void run2() throws CacheException {
        createMirroredRegion();
        int cntr = 0, cntr1 = 0;
        for (int i = 1; i < 6; i++) {
          mirroredRegion.put(Integer.valueOf(i), "testDistributedTxRemoveAll" + i);
          cntr++;
        }

        int size1 = mirroredRegion.size();
        assertEquals(5, size1);
        cacheTxnMgr = cache.getCacheTransactionManager();

        cacheTxnMgr.begin();
        mirroredRegion.removeAll(Collections.EMPTY_SET);
        cacheTxnMgr.commit();
        assertEquals(size1, mirroredRegion.size());
        cacheTxnMgr.begin();
        mirroredRegion.removeAll(Collections.singleton(Integer.valueOf(666)));
        cacheTxnMgr.commit();
        assertEquals(size1, mirroredRegion.size());
        assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(1)));
        cacheTxnMgr.begin();
        mirroredRegion.removeAll(Collections.singleton(Integer.valueOf(1)));
        cacheTxnMgr.commit();
        assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(1)));
        assertEquals(size1 - 1, mirroredRegion.size());
        size1--;
        cacheTxnMgr.begin();
        mirroredRegion.removeAll(Arrays.asList(Integer.valueOf(2), Integer.valueOf(3)));
        cacheTxnMgr.commit();
        assertEquals(size1 - 2, mirroredRegion.size());
        size1 -= 2;
      }
    });

    vm1.invoke(new CacheSerializableRunnable("testDistributedTxRemoveAllVerifyRemote") {
      @Override
      public void run2() throws CacheException {
        assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(5)));
        assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(4)));
        assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(3)));
        assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(2)));
        assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(1)));
        assertEquals(2, mirroredRegion.size());
      }
    });
  }

}// end of RemoveAllMultiVmDUnitTest
