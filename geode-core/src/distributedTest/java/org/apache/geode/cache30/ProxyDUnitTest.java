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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Make sure that operations are distributed and done in regions remote from a PROXY
 *
 * @since GemFire 5.0
 */

public class ProxyDUnitTest extends JUnit4CacheTestCase {

  private transient Region r;
  private transient DistributedMember otherId;
  protected transient int clInvokeCount;
  protected transient CacheEvent clLastEvent;

  public ProxyDUnitTest() {
    super();
  }

  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }

  private void initOtherId() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("Connect") {
      @Override
      public void run2() throws CacheException {
        getCache();
      }
    });
    otherId = vm.invoke(() -> getSystem().getDistributedMember());
  }

  private void doCreateOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        createRootRegion("ProxyDUnitTest", af.create());
      }
    });
  }

  ////////////////////// Test Methods //////////////////////

  /**
   * check distributed ops that originate in a PROXY are correctly distributed to non-proxy regions.
   */
  private void distributedOps(DataPolicy dp, InterestPolicy ip) throws CacheException {
    initOtherId();
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(dp);
    af.setSubscriptionAttributes(new SubscriptionAttributes(ip));
    af.setScope(Scope.DISTRIBUTED_ACK);
    Region r = createRootRegion("ProxyDUnitTest", af.create());

    doCreateOtherVm();

    r.put("putkey", "putvalue1");

    getOtherVm().invoke(new CacheSerializableRunnable("check put") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        assertEquals(true, r.containsKey("putkey"));
        assertEquals("putvalue1", r.getEntry("putkey").getValue());
        r.put("putkey", "putvalue2");
      }
    });

    assertEquals(false, r.containsKey("putkey"));
    assertEquals("putvalue2", r.get("putkey")); // netsearch

    r.invalidate("putkey");

    getOtherVm().invoke(new CacheSerializableRunnable("check invalidate") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        assertEquals(true, r.containsKey("putkey"));
        assertEquals(null, r.getEntry("putkey").getValue());
      }
    });
    assertEquals(null, r.get("putkey")); // invalid so total miss

    r.destroy("putkey");

    getOtherVm().invoke(new CacheSerializableRunnable("check destroy") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        assertEquals(false, r.containsKey("putkey"));
      }
    });

    assertEquals(null, r.get("putkey")); // total miss

    r.create("createKey", "createValue1");
    getOtherVm().invoke(new CacheSerializableRunnable("check create") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        assertEquals(true, r.containsKey("createKey"));
        assertEquals("createValue1", r.getEntry("createKey").getValue());
      }
    });
    {
      Map m = new HashMap();
      m.put("putAllKey1", "putAllValue1");
      m.put("putAllKey2", "putAllValue2");
      r.putAll(m, "putAllCallback");
    }
    getOtherVm().invoke(new CacheSerializableRunnable("check putAll") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        assertEquals(true, r.containsKey("putAllKey1"));
        assertEquals("putAllValue1", r.getEntry("putAllKey1").getValue());
        assertEquals(true, r.containsKey("putAllKey2"));
        assertEquals("putAllValue2", r.getEntry("putAllKey2").getValue());
      }
    });
    r.clear();
    getOtherVm().invoke(new CacheSerializableRunnable("check clear") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        assertEquals(0, r.size());
      }
    });

    getOtherVm().invoke(new CacheSerializableRunnable("install CacheWriter") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        AttributesMutator am = r.getAttributesMutator();
        CacheWriter cw = new CacheWriterAdapter() {
          @Override
          public void beforeCreate(EntryEvent event) throws CacheWriterException {
            throw new CacheWriterException("expected");
          }
        };
        am.setCacheWriter(cw);
      }
    });
    try {
      r.put("putkey", "putvalue");
      fail("expected CacheWriterException");
    } catch (CacheWriterException ignored) {
    }
    getOtherVm().invoke(new CacheSerializableRunnable("check clear") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        assertEquals(0, r.size());
      }
    });

    assertEquals(null, r.get("loadkey")); // total miss
    getOtherVm().invoke(new CacheSerializableRunnable("install CacheLoader") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        AttributesMutator am = r.getAttributesMutator();
        am.setCacheWriter(null); // clear csche writer
        CacheLoader cl = new CacheLoader() {
          @Override
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            if (helper.getKey().equals("loadkey")) {
              return "loadvalue";
            } else if (helper.getKey().equals("loadexception")) {
              throw new CacheLoaderException("expected");
            } else {
              return null;
            }
          }

          @Override
          public void close() {}
        };
        am.setCacheLoader(cl);
      }
    });
    assertEquals("loadvalue", r.get("loadkey")); // net load
    assertEquals(null, r.get("foobar")); // total miss
    try {
      r.get("loadexception");
      fail("expected CacheLoaderException");
    } catch (CacheLoaderException ignored) {
    }
    r.destroyRegion();
    getOtherVm().invoke(new CacheSerializableRunnable("check clear") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        assertEquals(null, r);
      }
    });
  }

  /**
   * Gets the DMStats for the vm's DM
   */
  private DMStats getDMStats() {
    return getCache().getDistributionManager().getStats();
  }

  /**
   * check remote ops done in a normal vm are correctly distributed to PROXY regions
   */
  private void remoteOriginOps(DataPolicy dp, InterestPolicy ip) throws CacheException {
    initOtherId();
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(dp);
    af.setSubscriptionAttributes(new SubscriptionAttributes(ip));
    af.setScope(Scope.DISTRIBUTED_ACK);
    CacheListener cl1 = new CacheListener() {
      @Override
      public void afterUpdate(EntryEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      @Override
      public void afterCreate(EntryEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      @Override
      public void afterInvalidate(EntryEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      @Override
      public void afterDestroy(EntryEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      @Override
      public void afterRegionInvalidate(RegionEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      @Override
      public void afterRegionDestroy(RegionEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      @Override
      public void afterRegionClear(RegionEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      @Override
      public void afterRegionCreate(RegionEvent e) {}

      @Override
      public void afterRegionLive(RegionEvent e) {}

      @Override
      public void close() {}
    };
    af.addCacheListener(cl1);
    Region r = createRootRegion("ProxyDUnitTest", af.create());
    clInvokeCount = 0;

    doCreateOtherVm();

    DMStats stats = getDMStats();
    long receivedMsgs = stats.getReceivedMessages();

    if (ip.isAll()) {
      getOtherVm().invoke(new CacheSerializableRunnable("do put") {
        @Override
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          r.put("p", "v");
        }
      });
      assertEquals(1, clInvokeCount);
      assertEquals(Operation.CREATE, clLastEvent.getOperation());
      assertEquals(true, clLastEvent.isOriginRemote());
      assertEquals(otherId, clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent) clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent) clLastEvent).isOldValueAvailable()); // failure
      assertEquals("v", ((EntryEvent) clLastEvent).getNewValue());
      assertEquals("p", ((EntryEvent) clLastEvent).getKey());
      clInvokeCount = 0;

      getOtherVm().invoke(new CacheSerializableRunnable("do create") {
        @Override
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          r.create("c", "v");
        }
      });
      assertEquals(1, clInvokeCount);
      assertEquals(Operation.CREATE, clLastEvent.getOperation());
      assertEquals(true, clLastEvent.isOriginRemote());
      assertEquals(otherId, clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent) clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent) clLastEvent).isOldValueAvailable());
      assertEquals("v", ((EntryEvent) clLastEvent).getNewValue());
      assertEquals("c", ((EntryEvent) clLastEvent).getKey());
      clInvokeCount = 0;

      getOtherVm().invoke(new CacheSerializableRunnable("do update") {
        @Override
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          r.put("c", "v2");
        }
      });
      assertEquals(1, clInvokeCount);
      assertEquals(Operation.UPDATE, clLastEvent.getOperation());
      assertEquals(true, clLastEvent.isOriginRemote());
      assertEquals(otherId, clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent) clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent) clLastEvent).isOldValueAvailable());
      assertEquals("v2", ((EntryEvent) clLastEvent).getNewValue());
      assertEquals("c", ((EntryEvent) clLastEvent).getKey());
      clInvokeCount = 0;

      getOtherVm().invoke(new CacheSerializableRunnable("do invalidate") {
        @Override
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          r.invalidate("c");
        }
      });
      assertEquals(1, clInvokeCount);
      assertEquals(Operation.INVALIDATE, clLastEvent.getOperation());
      assertEquals(true, clLastEvent.isOriginRemote());
      assertEquals(otherId, clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent) clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent) clLastEvent).isOldValueAvailable());
      assertEquals(null, ((EntryEvent) clLastEvent).getNewValue());
      assertEquals("c", ((EntryEvent) clLastEvent).getKey());
      clInvokeCount = 0;

      getOtherVm().invoke(new CacheSerializableRunnable("do destroy") {
        @Override
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          r.destroy("c");
        }
      });
      assertEquals(1, clInvokeCount);
      assertEquals(Operation.DESTROY, clLastEvent.getOperation());
      assertEquals(true, clLastEvent.isOriginRemote());
      assertEquals(otherId, clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent) clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent) clLastEvent).isOldValueAvailable());
      assertEquals(null, ((EntryEvent) clLastEvent).getNewValue());
      assertEquals("c", ((EntryEvent) clLastEvent).getKey());
      clInvokeCount = 0;

      getOtherVm().invoke(new CacheSerializableRunnable("do putAll") {
        @Override
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          Map m = new HashMap();
          m.put("putAllKey1", "putAllValue1");
          m.put("putAllKey2", "putAllValue2");
          r.putAll(m);
        }
      });
      assertEquals(2, clInvokeCount);
      // @todo darrel; check putAll events
      clInvokeCount = 0;

      getOtherVm().invoke(new CacheSerializableRunnable("do netsearch") {
        @Override
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          assertEquals(null, r.get("loadkey")); // total miss
        }
      });
      assertEquals(0, clInvokeCount);

    } else {
      getOtherVm().invoke(new CacheSerializableRunnable("do entry ops") {
        @Override
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          r.put("p", "v");
          r.create("c", "v");
          r.put("c", "v"); // update
          r.invalidate("c");
          r.destroy("c");
          {
            Map m = new HashMap();
            m.put("putAllKey1", "putAllValue1");
            m.put("putAllKey2", "putAllValue2");
            r.putAll(m);
          }
          assertEquals(null, r.get("loadkey")); // total miss
        }
      });

      assertEquals(0, clInvokeCount);
      assertEquals(0, r.size());
      // check the stats to make sure none of the above sent up messages
      assertEquals(receivedMsgs, stats.getReceivedMessages());
    }

    {
      AttributesMutator am = r.getAttributesMutator();
      CacheLoader cl = new CacheLoader() {
        @Override
        public Object load(LoaderHelper helper) throws CacheLoaderException {
          if (helper.getKey().equals("loadkey")) {
            return "loadvalue";
          } else if (helper.getKey().equals("loadexception")) {
            throw new CacheLoaderException("expected");
          } else {
            return null;
          }
        }

        @Override
        public void close() {}
      };
      am.setCacheLoader(cl);
    }

    receivedMsgs = stats.getReceivedMessages();
    getOtherVm().invoke(new CacheSerializableRunnable("check net loader") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        assertEquals("loadvalue", r.get("loadkey")); // net load
        assertEquals(null, r.get("foobar")); // total miss
        try {
          r.get("loadexception");
          fail("expected CacheLoaderException");
        } catch (CacheLoaderException ignored) {
        }
      }
    });
    assertTrue(stats.getReceivedMessages() > receivedMsgs);
    if (ip.isAll()) {
      assertEquals(1, clInvokeCount);
      assertEquals(Operation.NET_LOAD_CREATE, clLastEvent.getOperation());
      assertEquals(true, clLastEvent.isOriginRemote());
      assertEquals(otherId, clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent) clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent) clLastEvent).isOldValueAvailable());
      clInvokeCount = 0;
    } else {
      assertEquals(0, clInvokeCount);
    }

    {
      AttributesMutator am = r.getAttributesMutator();
      am.setCacheLoader(null);
      CacheWriter cw = new CacheWriterAdapter() {
        @Override
        public void beforeCreate(EntryEvent event) throws CacheWriterException {
          throw new CacheWriterException("expected");
        }
      };
      am.setCacheWriter(cw);
    }
    receivedMsgs = stats.getReceivedMessages();
    getOtherVm().invoke(new CacheSerializableRunnable("check net write") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        try {
          r.put("putkey", "putvalue");
          fail("expected CacheWriterException");
        } catch (CacheWriterException ignored) {
        }
      }
    });
    assertTrue(stats.getReceivedMessages() > receivedMsgs);
    {
      AttributesMutator am = r.getAttributesMutator();
      am.setCacheWriter(null);
    }
    assertEquals(0, clInvokeCount);
    clLastEvent = null;
    getOtherVm().invoke(new CacheSerializableRunnable("check region invalidate") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        r.invalidateRegion();
      }
    });
    assertEquals(1, clInvokeCount);
    assertEquals(Operation.REGION_INVALIDATE, clLastEvent.getOperation());
    assertEquals(true, clLastEvent.isOriginRemote());
    assertEquals(otherId, clLastEvent.getDistributedMember());

    clLastEvent = null;
    getOtherVm().invoke(new CacheSerializableRunnable("check region clear") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        r.clear();
      }
    });
    assertEquals(2, clInvokeCount);
    assertEquals(Operation.REGION_CLEAR, clLastEvent.getOperation());
    assertEquals(true, clLastEvent.isOriginRemote());
    assertEquals(otherId, clLastEvent.getDistributedMember());

    clLastEvent = null;
    getOtherVm().invoke(new CacheSerializableRunnable("check region destroy") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion("ProxyDUnitTest");
        r.destroyRegion();
      }
    });
    assertEquals(3, clInvokeCount);
    assertEquals(Operation.REGION_DESTROY, clLastEvent.getOperation());
    assertEquals(true, clLastEvent.isOriginRemote());
    assertEquals(otherId, clLastEvent.getDistributedMember());
    assertTrue(r.isDestroyed());
  }

  @Test
  public void testDistributedOpsPROXY() throws CacheException {
    distributedOps(DataPolicy.EMPTY, InterestPolicy.CACHE_CONTENT);
  }

  @Test
  public void testRemoteOriginOpsPROXY() throws CacheException {
    remoteOriginOps(DataPolicy.EMPTY, InterestPolicy.CACHE_CONTENT);
  }

  @Test
  public void testRemoteOriginOpsPROXY_ALL() throws CacheException {
    remoteOriginOps(DataPolicy.EMPTY, InterestPolicy.ALL);
  }
}
