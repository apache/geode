/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache30;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Make sure that operations are distributed and done in
 * regions remote from a PROXY
 *
 * @author darrel
 * @since 5.0
 */
public class ProxyDUnitTest extends CacheTestCase {

  private transient Region r;
  private transient DistributedMember otherId;
  protected transient int clInvokeCount;
  protected transient CacheEvent clLastEvent;
  
  public ProxyDUnitTest(String name) {
    super(name);
  }

  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }
    
  private void initOtherId() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("Connect") {
        public void run2() throws CacheException {
          getCache();
        }
      });
    this.otherId = (DistributedMember)vm.invoke(ProxyDUnitTest.class, "getVMDistributedMember");
  }
  private void doCreateOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
        public void run2() throws CacheException {
          AttributesFactory af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.REPLICATE);
          af.setScope(Scope.DISTRIBUTED_ACK);
          createRootRegion("ProxyDUnitTest", af.create());
        }
      });
  }

  public static DistributedMember getVMDistributedMember() {
    return InternalDistributedSystem.getAnyInstance().getDistributedMember();
  }
  
  //////////////////////  Test Methods  //////////////////////

  /**
   * check distributed ops that originate in a PROXY are correctly distributed
   * to non-proxy regions.
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
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          assertEquals(true, r.containsKey("putkey"));
          assertEquals(null, r.getEntry("putkey").getValue());
        }
      });
    assertEquals(null, r.get("putkey")); // invalid so total miss

    r.destroy("putkey");

    getOtherVm().invoke(new CacheSerializableRunnable("check destroy") {
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          assertEquals(false, r.containsKey("putkey"));
        }
      });
    
    assertEquals(null, r.get("putkey")); // total miss

    r.create("createKey", "createValue1");
    getOtherVm().invoke(new CacheSerializableRunnable("check create") {
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
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          assertEquals(0, r.size());
        }
      });

    getOtherVm().invoke(new CacheSerializableRunnable("install CacheWriter") {
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          AttributesMutator am = r.getAttributesMutator();
          CacheWriter cw = new CacheWriterAdapter() {
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
    } catch (CacheWriterException expected) {
    }
    getOtherVm().invoke(new CacheSerializableRunnable("check clear") {
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          assertEquals(0, r.size());
        }
      });

    assertEquals(null, r.get("loadkey")); // total miss
    getOtherVm().invoke(new CacheSerializableRunnable("install CacheLoader") {
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          AttributesMutator am = r.getAttributesMutator();
          am.setCacheWriter(null); // clear csche writer
          CacheLoader cl = new CacheLoader() {
              public Object load(LoaderHelper helper) throws CacheLoaderException {
                if (helper.getKey().equals("loadkey")) {
                  return "loadvalue";
                } else if (helper.getKey().equals("loadexception")) {
                  throw new CacheLoaderException("expected");
                } else {
                  return null;
                }
              }
              public void close() {
              }
            };
          am.setCacheLoader(cl);
        }
      });
    assertEquals("loadvalue", r.get("loadkey")); // net load
    assertEquals(null, r.get("foobar")); // total miss
    try {
      r.get("loadexception");
      fail("expected CacheLoaderException");
    } catch (CacheLoaderException expected) {
    }
    r.destroyRegion();
    getOtherVm().invoke(new CacheSerializableRunnable("check clear") {
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
    return ((InternalDistributedSystem)getCache().getDistributedSystem())
      .getDistributionManager().getStats();
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
        public void afterUpdate(EntryEvent e) {
          clLastEvent = e;
          clInvokeCount++;
        }
        public void afterCreate(EntryEvent e) {
          clLastEvent = e;
          clInvokeCount++;
        }
        public void afterInvalidate(EntryEvent e) {
          clLastEvent = e;
          clInvokeCount++;
        }
        public void afterDestroy(EntryEvent e) {
          clLastEvent = e;
          clInvokeCount++;
        }
        public void afterRegionInvalidate(RegionEvent e) {
          clLastEvent = e;
          clInvokeCount++;
        }
        public void afterRegionDestroy(RegionEvent e) {
          clLastEvent = e;
          clInvokeCount++;
        }
        public void afterRegionClear(RegionEvent e) {
          clLastEvent = e;
          clInvokeCount++;
        }
        public void afterRegionCreate(RegionEvent e) {
        }
        public void afterRegionLive(RegionEvent e) {
        }
        public void close() {
        }
      };
    af.addCacheListener(cl1);
    Region r = createRootRegion("ProxyDUnitTest", af.create());
    this.clInvokeCount = 0;

    doCreateOtherVm();

    DMStats stats = getDMStats();
    long receivedMsgs = stats.getReceivedMessages();

    if (ip.isAll()) {
      getOtherVm().invoke(new CacheSerializableRunnable("do put") {
          public void run2() throws CacheException {
            Region r = getRootRegion("ProxyDUnitTest");
            r.put("p", "v");
          }
        });
      assertEquals(1, this.clInvokeCount);
      assertEquals(Operation.CREATE, this.clLastEvent.getOperation());
      assertEquals(true, this.clLastEvent.isOriginRemote());
      assertEquals(this.otherId, this.clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent)this.clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent)this.clLastEvent).isOldValueAvailable()); // failure
      assertEquals("v", ((EntryEvent)this.clLastEvent).getNewValue());
      assertEquals("p", ((EntryEvent)this.clLastEvent).getKey());
      this.clInvokeCount = 0;
      
      getOtherVm().invoke(new CacheSerializableRunnable("do create") {
          public void run2() throws CacheException {
            Region r = getRootRegion("ProxyDUnitTest");
            r.create("c", "v");
          }
        });
      assertEquals(1, this.clInvokeCount);
      assertEquals(Operation.CREATE, this.clLastEvent.getOperation());
      assertEquals(true, this.clLastEvent.isOriginRemote());
      assertEquals(this.otherId, this.clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent)this.clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent)this.clLastEvent).isOldValueAvailable());
      assertEquals("v", ((EntryEvent)this.clLastEvent).getNewValue());
      assertEquals("c", ((EntryEvent)this.clLastEvent).getKey());
      this.clInvokeCount = 0;
      
      getOtherVm().invoke(new CacheSerializableRunnable("do update") {
          public void run2() throws CacheException {
            Region r = getRootRegion("ProxyDUnitTest");
            r.put("c", "v2");
          }
        });
      assertEquals(1, this.clInvokeCount);
      assertEquals(Operation.UPDATE, this.clLastEvent.getOperation());
      assertEquals(true, this.clLastEvent.isOriginRemote());
      assertEquals(this.otherId, this.clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent)this.clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent)this.clLastEvent).isOldValueAvailable());
      assertEquals("v2", ((EntryEvent)this.clLastEvent).getNewValue());
      assertEquals("c", ((EntryEvent)this.clLastEvent).getKey());
      this.clInvokeCount = 0;

      getOtherVm().invoke(new CacheSerializableRunnable("do invalidate") {
          public void run2() throws CacheException {
            Region r = getRootRegion("ProxyDUnitTest");
            r.invalidate("c");
          }
        });
      assertEquals(1, this.clInvokeCount);
      assertEquals(Operation.INVALIDATE, this.clLastEvent.getOperation());
      assertEquals(true, this.clLastEvent.isOriginRemote());
      assertEquals(this.otherId, this.clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent)this.clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent)this.clLastEvent).isOldValueAvailable());
      assertEquals(null, ((EntryEvent)this.clLastEvent).getNewValue());
      assertEquals("c", ((EntryEvent)this.clLastEvent).getKey());
      this.clInvokeCount = 0;
      
      getOtherVm().invoke(new CacheSerializableRunnable("do destroy") {
          public void run2() throws CacheException {
            Region r = getRootRegion("ProxyDUnitTest");
            r.destroy("c");
          }
        });
      assertEquals(1, this.clInvokeCount);
      assertEquals(Operation.DESTROY, this.clLastEvent.getOperation());
      assertEquals(true, this.clLastEvent.isOriginRemote());
      assertEquals(this.otherId, this.clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent)this.clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent)this.clLastEvent).isOldValueAvailable());
      assertEquals(null, ((EntryEvent)this.clLastEvent).getNewValue());
      assertEquals("c", ((EntryEvent)this.clLastEvent).getKey());
      this.clInvokeCount = 0;
      
      getOtherVm().invoke(new CacheSerializableRunnable("do putAll") {
          public void run2() throws CacheException {
            Region r = getRootRegion("ProxyDUnitTest");
            Map m = new HashMap();
            m.put("putAllKey1", "putAllValue1");
            m.put("putAllKey2", "putAllValue2");
            r.putAll(m);
          }
        });
      assertEquals(2, this.clInvokeCount);
      // @todo darrel; check putAll events
      this.clInvokeCount = 0;

      getOtherVm().invoke(new CacheSerializableRunnable("do netsearch") {
          public void run2() throws CacheException {
            Region r = getRootRegion("ProxyDUnitTest");
            assertEquals(null, r.get("loadkey")); // total miss
          }
        });
      assertEquals(0, this.clInvokeCount);
      
    } else {
      getOtherVm().invoke(new CacheSerializableRunnable("do entry ops") {
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

      assertEquals(0, this.clInvokeCount);
      assertEquals(0, r.size());
      // check the stats to make sure none of the above sent up messages
      assertEquals(receivedMsgs, stats.getReceivedMessages());
    }

    {
      AttributesMutator am = r.getAttributesMutator();
      CacheLoader cl = new CacheLoader() {
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            if (helper.getKey().equals("loadkey")) {
              return "loadvalue";
            } else if (helper.getKey().equals("loadexception")) {
              throw new CacheLoaderException("expected");
            } else {
              return null;
            }
          }
          public void close() {
          }
        };
      am.setCacheLoader(cl);
    }

    receivedMsgs = stats.getReceivedMessages();
    getOtherVm().invoke(new CacheSerializableRunnable("check net loader") {
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          assertEquals("loadvalue", r.get("loadkey")); // net load
          assertEquals(null, r.get("foobar")); // total miss
          try {
            r.get("loadexception");
            fail("expected CacheLoaderException");
          } catch (CacheLoaderException expected) {
          }
        }
      });
    assertTrue(stats.getReceivedMessages() > receivedMsgs);
    if (ip.isAll()) {
      assertEquals(1, this.clInvokeCount);
      assertEquals(Operation.NET_LOAD_CREATE, this.clLastEvent.getOperation());
      assertEquals(true, this.clLastEvent.isOriginRemote());
      assertEquals(this.otherId, this.clLastEvent.getDistributedMember());
      assertEquals(null, ((EntryEvent)this.clLastEvent).getOldValue());
      assertEquals(false, ((EntryEvent)this.clLastEvent).isOldValueAvailable());
      this.clInvokeCount = 0;
    } else {
      assertEquals(0, this.clInvokeCount);
    }

    {
      AttributesMutator am = r.getAttributesMutator();
      am.setCacheLoader(null);
      CacheWriter cw = new CacheWriterAdapter() {
          public void beforeCreate(EntryEvent event) throws CacheWriterException {
            throw new CacheWriterException("expected");
          }
        };
      am.setCacheWriter(cw);
    }
    receivedMsgs = stats.getReceivedMessages();
    getOtherVm().invoke(new CacheSerializableRunnable("check net write") {
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          try {
            r.put("putkey", "putvalue");
            fail("expected CacheWriterException");
          } catch (CacheWriterException expected) {
          }
        }
      });
    assertTrue(stats.getReceivedMessages() > receivedMsgs);
    {
      AttributesMutator am = r.getAttributesMutator();
      am.setCacheWriter(null);
    }
    assertEquals(0, this.clInvokeCount);
    this.clLastEvent = null;
    getOtherVm().invoke(new CacheSerializableRunnable("check region invalidate") {
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          r.invalidateRegion();
        }
      });
    assertEquals(1, this.clInvokeCount);
    assertEquals(Operation.REGION_INVALIDATE, this.clLastEvent.getOperation());
    assertEquals(true, this.clLastEvent.isOriginRemote());
    assertEquals(this.otherId, this.clLastEvent.getDistributedMember());

    this.clLastEvent = null;
    getOtherVm().invoke(new CacheSerializableRunnable("check region clear") {
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          r.clear();
        }
      });
    assertEquals(2, this.clInvokeCount);
    assertEquals(Operation.REGION_CLEAR, this.clLastEvent.getOperation());
    assertEquals(true, this.clLastEvent.isOriginRemote());
    assertEquals(this.otherId, this.clLastEvent.getDistributedMember());

    this.clLastEvent = null;
    getOtherVm().invoke(new CacheSerializableRunnable("check region destroy") {
        public void run2() throws CacheException {
          Region r = getRootRegion("ProxyDUnitTest");
          r.destroyRegion();
        }
      });
    assertEquals(3, this.clInvokeCount);
    assertEquals(Operation.REGION_DESTROY, this.clLastEvent.getOperation());
    assertEquals(true, this.clLastEvent.isOriginRemote());
    assertEquals(this.otherId, this.clLastEvent.getDistributedMember());
    assertTrue(r.isDestroyed());
  }
  
  public void testDistributedOpsPROXY() throws CacheException {
    distributedOps(DataPolicy.EMPTY, InterestPolicy.CACHE_CONTENT);
  }

  public void testRemoteOriginOpsPROXY() throws CacheException {
    remoteOriginOps(DataPolicy.EMPTY, InterestPolicy.CACHE_CONTENT);
  }
  public void testRemoteOriginOpsPROXY_ALL() throws CacheException {
    remoteOriginOps(DataPolicy.EMPTY, InterestPolicy.ALL);
  }
}
