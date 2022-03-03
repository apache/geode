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
package org.apache.geode.cache30;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.naming.Context;
import javax.transaction.UserTransaction;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.CopyHelper;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.transaction.Person;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.TransactionListenerAdapter;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Test the order of operations done on the farside of a tx.
 *
 * @since GemFire 5.0
 */

public class TXOrderDUnitTest extends JUnit4CacheTestCase {

  private final int TEST_PUT = 0;
  private final int TEST_INVALIDATE = 1;
  private final int TEST_DESTROY = 2;

  private transient Region r;
  protected transient int invokeCount;

  List expectedKeys;
  int clCount = 0;

  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }

  private void doCommitOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        Region r1 = createRootRegion("r1", af.create());
        Region r2 = r1.createSubregion("r2", af.create());
        Region r3 = r2.createSubregion("r3", af.create());
        CacheTransactionManager ctm = getCache().getCacheTransactionManager();
        ctm.begin();
        r2.put("b", "value1");
        r3.put("c", "value2");
        r1.put("a", "value3");
        r1.put("a2", "value4");
        r3.put("c2", "value5");
        r2.put("b2", "value6");
        ctm.commit();
      }
    });
  }

  Object getCurrentExpectedKey() {
    Object result = expectedKeys.get(clCount);
    clCount += 1;
    return result;
  }

  /**
   * make sure listeners get invoked in correct order on far side of tx
   */
  @Test
  public void testFarSideOrder() throws CacheException {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setScope(Scope.DISTRIBUTED_ACK);
    CacheListener cl1 = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent e) {
        assertEquals(getCurrentExpectedKey(), e.getKey());
      }
    };
    af.addCacheListener(cl1);
    Region r1 = createRootRegion("r1", af.create());
    Region r2 = r1.createSubregion("r2", af.create());
    r2.createSubregion("r3", af.create());

    TransactionListener tl1 = new TransactionListenerAdapter() {
      @Override
      public void afterCommit(TransactionEvent e) {
        assertEquals(6, e.getEvents().size());
        ArrayList keys = new ArrayList();
        for (final CacheEvent<?, ?> cacheEvent : e.getEvents()) {
          EntryEvent ee = (EntryEvent) cacheEvent;
          keys.add(ee.getKey());
          assertEquals(null, ee.getCallbackArgument());
          assertEquals(true, ee.isCallbackArgumentAvailable());
        }
        assertEquals(expectedKeys, keys);
        invokeCount = 1;
      }
    };
    CacheTransactionManager ctm = getCache().getCacheTransactionManager();
    ctm.addListener(tl1);

    invokeCount = 0;
    clCount = 0;
    expectedKeys = Arrays.asList("b", "c", "a", "a2", "c2", "b2");
    doCommitOtherVm();
    assertEquals(1, invokeCount);
    assertEquals(6, clCount);
  }

  /**
   * Tests fix for #40870 Remote CacheListeners invoke afterCreate with Operation.LOCAL_LOAD_CREATE
   * when create executed transactionally"
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testFarSideOpForLoad() throws Exception {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        CacheListener cl1 = new CacheListenerAdapter() {
          @Override
          public void afterCreate(EntryEvent e) {
            assertTrue(e.getOperation().isLocalLoad());
          }
        };
        af.addCacheListener(cl1);
        CacheLoader cl = new CacheLoader() {
          @Override
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            LogWriterUtils.getLogWriter().info("Loading value:" + helper.getKey() + "_value");
            return helper.getKey() + "_value";
          }

          @Override
          public void close() {}
        };
        af.setCacheLoader(cl);
        createRootRegion("r1", af.create());
        return null;
      }
    });

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        CacheListener cl1 = new CacheListenerAdapter() {
          @Override
          public void afterCreate(EntryEvent e) {
            LogWriterUtils.getLogWriter().info("op:" + e.getOperation().toString());
            assertTrue(!e.getOperation().isLocalLoad());
          }
        };
        af.addCacheListener(cl1);
        createRootRegion("r1", af.create());
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getRootRegion("r1");
        getCache().getCacheTransactionManager().begin();
        r.get("obj_2");
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });
  }

  @Test
  public void testInternalRegionNotExposed() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    SerializableCallable createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ExposedRegionTransactionListener tl = new ExposedRegionTransactionListener();
        CacheTransactionManager ctm = getCache().getCacheTransactionManager();
        ctm.addListener(tl);
        ExposedRegionCacheListener cl = new ExposedRegionCacheListener();
        AttributesFactory af = new AttributesFactory();
        PartitionAttributes pa =
            new PartitionAttributesFactory().setRedundantCopies(1).setTotalNumBuckets(1).create();
        af.setPartitionAttributes(pa);
        af.addCacheListener(cl);
        Region pr = createRootRegion("testTxEventForRegion", af.create());
        return null;
      }
    };
    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region pr = getRootRegion("testTxEventForRegion");
        CacheTransactionManager ctm = getCache().getCacheTransactionManager();
        pr.put(2, "tw");
        pr.put(3, "three");
        pr.put(4, "four");
        ctm.begin();
        pr.put(1, "one");
        pr.put(2, "two");
        pr.invalidate(3);
        pr.destroy(4);
        ctm.commit();
        return null;
      }
    });
    SerializableCallable verifyListener = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region pr = getRootRegion("testTxEventForRegion");
        CacheTransactionManager ctm = getCache().getCacheTransactionManager();
        ExposedRegionTransactionListener tl =
            (ExposedRegionTransactionListener) ctm.getListeners()[0];
        ExposedRegionCacheListener cl =
            (ExposedRegionCacheListener) pr.getAttributes().getCacheListeners()[0];
        assertFalse(tl.exceptionOccurred);
        assertFalse(cl.exceptionOccurred);
        return null;
      }
    };
    vm1.invoke(verifyListener);
    vm2.invoke(verifyListener);
  }

  private static class ExposedRegionTransactionListener extends TransactionListenerAdapter {

    private boolean exceptionOccurred = false;

    @Override
    public void afterCommit(TransactionEvent event) {
      List<CacheEvent<?, ?>> events = event.getEvents();
      for (CacheEvent<?, ?> e : events) {
        if (!(SEPARATOR + "testTxEventForRegion").equals(e.getRegion().getFullPath())) {
          exceptionOccurred = true;
        }
      }
    }
  }

  private static class ExposedRegionCacheListener extends CacheListenerAdapter {

    private boolean exceptionOccurred = false;

    @Override
    public void afterCreate(EntryEvent event) {
      verifyRegion(event);
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      verifyRegion(event);
    }

    private void verifyRegion(EntryEvent event) {
      if (!(SEPARATOR + "testTxEventForRegion").equals(event.getRegion().getFullPath())) {
        exceptionOccurred = true;
      }
    }
  }

  /**
   * verify that queries on indexes work with transaction
   */
  @Test
  public void testFarSideIndexOnPut() throws Exception {
    doTest(TEST_PUT);
  }

  @Test
  public void testFarSideIndexOnInvalidate() throws Exception {
    doTest(TEST_INVALIDATE);
  }

  @Test
  public void testFarSideIndexOnDestroy() throws Exception {
    doTest(TEST_DESTROY);
  }

  private void doTest(final int op) throws Exception {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    SerializableCallable createRegionAndIndex = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        Region region = createRootRegion("sample", af.create());
        QueryService qs = getCache().getQueryService();
        qs.createIndex("foo", IndexType.FUNCTIONAL, "age", SEPARATOR + "sample");
        return null;
      }
    };
    vm1.invoke(createRegionAndIndex);
    vm2.invoke(createRegionAndIndex);

    // do transactional puts in vm1
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Context ctx = getCache().getJNDIContext();
        UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
        Region region = getRootRegion("sample");
        Integer x = 0;
        utx.begin();
        region.create(x, new Person("xyz", 45));
        utx.commit();
        QueryService qs = getCache().getQueryService();
        Query q = qs.newQuery("select * from " + SEPARATOR + "sample where age < 50");
        assertEquals(1, ((SelectResults) q.execute()).size());
        Person dsample = (Person) CopyHelper.copy(region.get(x));
        dsample.setAge(55);
        utx.begin();
        switch (op) {
          case TEST_PUT:
            region.put(x, dsample);
            break;
          case TEST_INVALIDATE:
            region.invalidate(x);
            break;
          case TEST_DESTROY:
            region.destroy(x);
            break;
          default:
            fail("unknown op");
        }
        utx.commit();
        assertEquals(0, ((SelectResults) q.execute()).size());
        return null;
      }
    });

    // run query and verify results in other vm
    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        QueryService qs = getCache().getQueryService();
        Query q = qs.newQuery("select * from " + SEPARATOR + "sample where age < 50");
        assertEquals(0, ((SelectResults) q.execute()).size());
        return null;
      }
    });
  }

  @Test
  public void testBug43353() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);

    SerializableCallable createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        getCache().createRegionFactory(RegionShortcut.REPLICATE).create(getTestMethodName());
        return null;
      }
    };

    vm1.invoke(createRegion);
    vm2.invoke(createRegion);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(getTestMethodName());
        r.put("ikey", "value");
        getCache().getCacheTransactionManager().begin();
        r.put("key1", new byte[20]);
        r.invalidate("ikey");
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(getTestMethodName());
        Object v = r.get("key1");
        assertNotNull(v);
        assertTrue(v instanceof byte[]);
        assertNull(r.get("ikey"));
        return null;
      }
    });
  }
}
