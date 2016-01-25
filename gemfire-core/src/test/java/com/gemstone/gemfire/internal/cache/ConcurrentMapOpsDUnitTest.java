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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.AssertionFailedError;

import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.internal.DestroyOp;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.gms.MembershipManagerHelper;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * tests for the concurrentMapOperations. there are more tests in ClientServerMiscDUnitTest
 * @author sbawaska
 *
 */
public class ConcurrentMapOpsDUnitTest extends CacheTestCase {

  protected static final String REP_REG_NAME = "repRegion";
  protected static final String PR_REG_NAME = "prRegion";
  private static final int MAX_ENTRIES = 113;
  
  enum OP {PUTIFABSENT, REPLACE, REMOVE}
  
  private void createRegions(VM vm) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        createReplicateRegion();
        createPartitionedRegion();
        return null;
      }
    });
  }
  
  private void createRedundantRegions(VM vm) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getCache().createRegionFactory(RegionShortcut.REPLICATE).setConcurrencyChecksEnabled(true).create(REP_REG_NAME);
        getCache().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).setConcurrencyChecksEnabled(true).create(PR_REG_NAME);
        return null;
      }
    });
  }

  protected Region createReplicateRegion() {
    return getCache().createRegionFactory(RegionShortcut.REPLICATE).setConcurrencyChecksEnabled(true).create(REP_REG_NAME);
  }

  protected Region createPartitionedRegion() {
    return getCache().createRegionFactory(RegionShortcut.PARTITION).setConcurrencyChecksEnabled(true).create(PR_REG_NAME);
  }

  private Integer createRegionsAndStartServer(VM vm) {
    return createRegionsAndStartServer(vm, false);
  }
  
  private Integer createRegionsAndStartServer(VM vm, final boolean withRedundancy) {
    return (Integer)vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getCache().createRegionFactory(RegionShortcut.REPLICATE).create(REP_REG_NAME);
        if (withRedundancy) {
          getCache().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).create(PR_REG_NAME);
        } else {
          getCache().createRegionFactory(RegionShortcut.PARTITION).create(PR_REG_NAME);
        }
        int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
        CacheServer s = getCache().addCacheServer();
        s.setPort(port);
        s.start();
        return port;
      }
    });
  }
  
  private void createEmptyRegion(VM vm) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getCache().createRegionFactory(RegionShortcut.REPLICATE_PROXY).setConcurrencyChecksEnabled(true).create(REP_REG_NAME);
        getCache().createRegionFactory(RegionShortcut.PARTITION_PROXY).setConcurrencyChecksEnabled(true).create(PR_REG_NAME);
        return null;
      }
    });
  }
  
  private void createClientRegionWithRI(VM vm, final int port, final boolean isEmpty) {
    createClientRegion(vm, port, isEmpty, true, -1);
  }
  
  private void createClientRegion(VM vm, final int port1, final boolean isEmpty, int port2) {
    createClientRegion(vm, port1, isEmpty, false, port2);
  }
  
  private void createClientRegion(VM vm, final int port1, final boolean isEmpty, final boolean ri, final int port2) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/*getServerHostName(Host.getHost(0))*/, port1);
        if (port2 > 0) {
          ccf.addPoolServer("localhost", port2);
        }
        ccf.setPoolSubscriptionEnabled(true);
        ccf.set("log-level", getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<Integer, String> crf = cCache
            .createClientRegionFactory(isEmpty ? ClientRegionShortcut.PROXY
                : ClientRegionShortcut.CACHING_PROXY);
        Region<Integer, String> r = crf.create(REP_REG_NAME);
        Region<Integer, String> pr = crf.create(PR_REG_NAME);
        if (ri) {
          r.registerInterestRegex(".*");
          pr.registerInterestRegex(".*");
        }
        return null;
      }
    });
  }
  
   static abstract class AbstractConcMapOpsListener implements CacheListener<Integer, String> {
    public void afterCreate(EntryEvent<Integer, String> event) {
      validate(event);
    }
    public void afterDestroy(EntryEvent<Integer, String> event) {
      validate(event);
    }
    public void afterInvalidate(EntryEvent<Integer, String> event) {
      validate(event);
    }
    public void afterRegionClear(RegionEvent<Integer, String> event) {
    }
    public void afterRegionCreate(RegionEvent<Integer, String> event) {
    }
    public void afterRegionDestroy(RegionEvent<Integer, String> event) {
    }
    public void afterRegionInvalidate(RegionEvent<Integer, String> event) {
    }
    public void afterRegionLive(RegionEvent<Integer, String> event) {
    }
    public void afterUpdate(EntryEvent<Integer, String> event) {
      validate(event);
    }
    public void close() {
    }
    abstract void validate(EntryEvent event);
  }
  
  static class NotInvokedListener extends AbstractConcMapOpsListener {
    @Override
    void validate(EntryEvent event) {
      fail("should not be called.  Event="+event);
    }
  }

  static class InitialCreatesListener extends AbstractConcMapOpsListener {
    AtomicInteger numCreates = new AtomicInteger();
    @Override
    void validate(EntryEvent event) {
      if (!event.getOperation().isCreate()) {
        fail("expected only create events");
      }
      numCreates.incrementAndGet();
    }
  }
  /**
   * @param name
   */
  public ConcurrentMapOpsDUnitTest(String name) {
    super(name);
  }

  // test for bug #42164
  public void testListenerNotInvokedForRejectedOperation() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    int port1 = createRegionsAndStartServer(vm1);
    int port2 = createRegionsAndStartServer(vm2);
    createClientRegionWithRI(client1, port1, true);
    createClientRegionWithRI(client2, port2, true);

    SerializableCallable addListenerToClientForInitialCreates = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(REP_REG_NAME);
        r.getAttributesMutator().addCacheListener(new InitialCreatesListener());
        Region pr = getCache().getRegion(PR_REG_NAME);
        pr.getAttributesMutator().addCacheListener(new InitialCreatesListener());
        return null;
      }
    };
    client1.invoke(addListenerToClientForInitialCreates);
    client2.invoke(addListenerToClientForInitialCreates);

    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<Integer, String> r = getGemfireCache().getRegion(REP_REG_NAME);
        Region<Integer, String> pr = getGemfireCache().getRegion(PR_REG_NAME);
        for (int i = 0; i < MAX_ENTRIES; i++) {
          r.put(i, "value" + i);
          pr.put(i, "value" + i);
        }
        return null;
      }
    });

    SerializableCallable waitForInitialCreates = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<Integer, String> r = getGemfireCache().getRegion(REP_REG_NAME);
        Region<Integer, String> pr = getGemfireCache().getRegion(PR_REG_NAME);
        waitForCreates(r);
        waitForCreates(pr);
        return null;
      }
      private void waitForCreates(Region region) {
        CacheListener[] listeners = region.getAttributes().getCacheListeners();
        boolean listenerFound = false;
        for (CacheListener listener : listeners) {
          if (listener instanceof InitialCreatesListener) {
            listenerFound = true;
            final InitialCreatesListener initialCreatesListener = (InitialCreatesListener) listener;
            WaitCriterion wc = new WaitCriterion() {
              @Override
              public boolean done() {
                return initialCreatesListener.numCreates.get() == MAX_ENTRIES;
              }
              @Override
              public String description() {
                return "Client expected to get "+MAX_ENTRIES+" creates, but got "+initialCreatesListener.numCreates.get();
              }
            };
            DistributedTestCase.waitForCriterion(wc, 30*1000, 500, true);
          }
        }
        if (!listenerFound) {
          fail("Client listener should have been found");
        }
      }
    };
    client1.invoke(waitForInitialCreates);
    client2.invoke(waitForInitialCreates);

    SerializableCallable addListener = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(REP_REG_NAME);
        r.getAttributesMutator().addCacheListener(new NotInvokedListener());
        Region pr = getCache().getRegion(PR_REG_NAME);
        pr.getAttributesMutator().addCacheListener(new NotInvokedListener());
        return null;
      }
    };
    vm1.invoke(addListener);
    vm2.invoke(addListener);
    SerializableCallable addListenerToClient = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(REP_REG_NAME);
        r.getAttributesMutator().addCacheListener(new NotInvokedListener());
        Region pr = getCache().getRegion(PR_REG_NAME);
        pr.getAttributesMutator().addCacheListener(new NotInvokedListener());
        return null;
      }
    };
    client1.invoke(addListenerToClient);
    client2.invoke(addListenerToClient);
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(REP_REG_NAME);
        Region pr = getCache().getRegion(PR_REG_NAME);
        for (int i=0; i<MAX_ENTRIES; i++) {
          assertEquals("value"+i, r.putIfAbsent(i, "piavalue"));
          assertEquals("value"+i, pr.putIfAbsent(i, "piavalue"));
        }
        for (int i=0; i<MAX_ENTRIES; i++) {
          assertFalse(r.replace(i, "value", "replace1Value"));
          assertFalse(pr.replace(i, "value", "replace1Value"));
        }
        for (int i=MAX_ENTRIES+1; i<MAX_ENTRIES*2; i++) {
          assertNull(r.replace(i, "replace2value"+i));
          assertNull(pr.replace(i, "replace2value"+i));
        }
        for (int i=MAX_ENTRIES+1; i<MAX_ENTRIES*2; i++) {
          assertFalse(r.remove(i, "removeValue"+i));
          assertFalse(pr.remove(i, "removeValue"+i));
        }
        return null;
      }
    });
  }
  
  public void testBug42162() {
    dotestConcOps(false);
  }
  
  public void testBug42162EmptyClient() {
    dotestConcOps(true);
  }
  
  private void dotestConcOps(final boolean emptyClient) {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(2);
    int port1 = createRegionsAndStartServer(server);
    
    createClientRegion(client, port1, emptyClient, -1);
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        r.registerInterestRegex(".*");
        pr.registerInterestRegex(".*");
        return null;
      }
    });
    server.invoke(new SerializableCallable() {
      public Object call() {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        r.put("key0", "value");
        pr.put("key0", "value");
        assertNull(r.putIfAbsent("keyForNull", null));
        assertNull(pr.putIfAbsent("keyForNull", null));
        assertEquals("value", r.putIfAbsent("key0", null));
        assertEquals("value", pr.putIfAbsent("key0", null));
        assertTrue(r.containsKey("keyForNull"));
        assertTrue(pr.containsKey("keyForNull"));
        assertFalse(r.containsValueForKey("keyForNull"));
        assertFalse(pr.containsValueForKey("keyForNull"));
        r.put("key0", "value");
        pr.put("key0", "value");
        return null;
      }
    });
    client.invoke(new SerializableCallable() {
      public Object call() {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        WaitCriterion wc = new WaitCriterion() {
          AssertionFailedError e = null;
          public boolean done() {
            try {
              if (!emptyClient) {
                assertTrue(r.containsKey("key0"));
                assertTrue(pr.containsKey("key0"));
                assertTrue(r.containsKey("keyForNull"));
                assertTrue(pr.containsKey("keyForNull"));
                assertFalse(r.containsValueForKey("keyForNull"));
                assertFalse(pr.containsValueForKey("keyForNull"));
              }
              assertEquals("value", r.putIfAbsent("key0", null));
              assertEquals("value", pr.putIfAbsent("key0", null));
              assertNull(r.putIfAbsent("keyForNull", null));
              assertNull(pr.putIfAbsent("keyForNull", null));
              assertNull(r.putIfAbsent("clientNullKey", null));
              assertNull(pr.putIfAbsent("clientNullKey", null));
            } catch (AssertionFailedError ex) {
              r.getCache().getLoggerI18n().fine("SWAP:caught ", ex);
              e = ex;
              return false;
            }
            return true;
          }
          public String description() {
            return "timeout "+e;
          }
        };
        DistributedTestCase.waitForCriterion(wc, 30000, 1000, true);
        return null;
      }
    });
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertTrue(r.containsKey("clientNullKey"));
        assertTrue(pr.containsKey("clientNullKey"));
        assertFalse(r.containsValueForKey("clientNullKey"));
        assertFalse(pr.containsValueForKey("clientNullKey"));
        assertNotNull(r.replace("key0", "value2"));
        assertNotNull(pr.replace("key0", "value2"));
        assertTrue(r.replace("keyForNull", null, "newValue"));
        assertTrue(pr.replace("keyForNull", null, "newValue"));
        return null;
      }
    });
    client.invoke(new SerializableCallable() {
      public Object call() {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        WaitCriterion wc = new WaitCriterion() {
          AssertionFailedError e = null;
          public boolean done() {
            try {
              assertEquals("value2", r.putIfAbsent("key0", null));
              assertEquals("value2", pr.putIfAbsent("key0", null));
              assertEquals("newValue", r.putIfAbsent("keyForNull", null));
              assertEquals("newValue", pr.putIfAbsent("keyForNull", null));
              //replace from client
              assertEquals("value2", r.replace("key0", "value"));
              assertEquals("value2", pr.replace("key0", "value"));
              assertNull(r.replace("NoKeyOnServer", "value"));
              assertNull(r.replace("NoKeyOnServer", "value"));
              assertTrue(r.replace("clientNullKey", null, "newValue"));
              assertTrue(pr.replace("clientNullKey", null, "newValue"));
            } catch (AssertionFailedError ex) {
              e = ex;
              return false;
            }
            return true;
          }
          public String description() {
            return "timeout "+e.getMessage();
          }
        };
        DistributedTestCase.waitForCriterion(wc, 30000, 1000, true);
        return null;
      }
    });
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertEquals("newValue", r.get("clientNullKey"));
        assertEquals("newValue", pr.get("clientNullKey"));
        return null;
      }
    });
  }
  
  public void testNullValueFromNonEmptyClients() {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(2);
    int port1 = createRegionsAndStartServer(server);
    
    createClientRegion(client, port1, true, -1);
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        r.create("createKey", null);
        pr.create("createKey", null);
        assertNull(r.putIfAbsent("putAbsentKey", null));
        assertNull(pr.putIfAbsent("putAbsentKey", null));
        return null;
      }
    });
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertEquals(r.get("createKey"), r.get("putAbsentKey"));
        assertEquals(pr.get("createKey"), pr.get("putAbsentKey"));
        assertFalse(r.containsKey("createKey"));
        assertFalse(pr.containsKey("createKey"));
        assertEquals(r.containsKey("createKey"), r.containsKey("putAbsentKey"));
        assertEquals(pr.containsKey("createKey"), pr.containsKey("putAbsentKey"));
        return null;
      }
    });
  }

  public void testPutIfAbsent() {
    doPutIfAbsentWork(false);
  }
  
  public void testPutIfAbsentCS() {
    doPutIfAbsentWork(true);
  }
  
  private void doPutIfAbsentWork(final boolean cs) {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(2);
    if (cs) {
      int port1 = createRegionsAndStartServer(vm1);
      createClientRegion(vm2, port1, false, -1);
    } else {
      createRegions(vm1);
      createRegions(vm2);
    }
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertNull(r.putIfAbsent("key0", "value"));
        assertNull(pr.putIfAbsent("key0", "value"));
        assertNull(r.putIfAbsent("keyForClient", "value"));
        assertNull(pr.putIfAbsent("keyForClient", "value"));
        assertEquals("value", r.putIfAbsent("key0", "value2"));
        assertEquals("value", pr.putIfAbsent("key0", "value2"));
        return null;
      }
    });
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertEquals("value", r.putIfAbsent("key0", "value2"));
        assertEquals("value", pr.putIfAbsent("key0", "value2"));
        if (cs) {
          r.get("key0");
          pr.get("key0");
        }
        assertTrue(r.containsKey("key0"));
        assertTrue(pr.containsKey("key0"));
        assertTrue(r.containsValueForKey("key0"));
        assertTrue(pr.containsValueForKey("key0"));
        return null;
      }
    });
  }
  
  public void testRemove() {
    doRemoveWork(false);
  }
  
  public void testRemoveCS() {
    doRemoveWork(true);
  }
  
  private void doRemoveWork(final boolean cs) {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(2);
    if (cs) {
      int port1 = createRegionsAndStartServer(vm1);
      createClientRegion(vm2, port1, true, -1);
    } else {
      createRegions(vm1);
      createRegions(vm2);
    }
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertNull(r.putIfAbsent("key0", "value"));
        assertNull(pr.putIfAbsent("key0", "value"));
        assertNull(r.putIfAbsent("keyForClient", "value"));
        assertNull(pr.putIfAbsent("keyForClient", "value"));
        assertFalse(r.remove("nonExistentkey", "value"));
        assertFalse(pr.remove("nonExistentkey", "value"));
        assertFalse(r.remove("key0", "newValue"));
        assertFalse(pr.remove("key0", "newValue"));
        assertTrue(r.remove("key0", "value"));
        assertTrue(pr.remove("key0", "value"));
        assertFalse(r.containsKey("key0"));
        assertFalse(pr.containsKey("key0"));
        return null;
      }
    });
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertFalse(r.remove("nonExistentkey", "value"));
        assertFalse(pr.remove("nonExistentkey", "value"));
        assertFalse(r.remove("keyForClient", "newValue"));
        assertFalse(pr.remove("keyForClient", "newValue"));
        assertTrue(r.remove("keyForClient", "value"));
        assertTrue(pr.remove("keyForClient", "value"));
        return null;
      }
    });
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertFalse(r.containsKey("keyForClient"));
        assertFalse(pr.containsKey("keyForClient"));
        return null;
      }
    });
  }
  
  public void testReplaceCS() {
    doReplaceWork(true);
  }
  
  public void testReplace() {
    doReplaceWork(false);
  }
  
  private void doReplaceWork(final boolean cs) {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(2);
    if (cs) {
      int port1 = createRegionsAndStartServer(vm1);
      createClientRegion(vm2, port1, true, -1);
    } else {
      createRegions(vm1);
      createRegions(vm2);
    }
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertNull(r.putIfAbsent("key0", "value"));
        assertNull(pr.putIfAbsent("key0", "value"));
        assertNull(r.putIfAbsent("keyForClient", "value"));
        assertNull(pr.putIfAbsent("keyForClient", "value"));
        assertNull(r.replace("nonExistentkey", "value"));
        assertNull(pr.replace("nonExistentkey", "value"));
        assertEquals("value", r.replace("key0", "value2"));
        assertEquals("value", pr.replace("key0", "value2"));
        assertFalse(r.replace("key0", "value", "newValue"));
        assertFalse(pr.replace("key0", "value", "newValue"));
        assertTrue(r.replace("key0", "value2", "newValue"));
        assertTrue(pr.replace("key0", "value2", "newValue"));
        return null;
      }
    });
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertNull(r.replace("nonExistentkey", "value"));
        assertNull(pr.replace("nonExistentkey", "value"));
        assertEquals("value", r.replace("keyForClient", "value2"));
        assertEquals("value", pr.replace("keyForClient", "value2"));
        assertFalse(r.replace("keyForClient", "value", "newValue"));
        assertFalse(pr.replace("keyForClient", "value", "newValue"));
        assertTrue(r.replace("keyForClient", "value2", "newValue"));
        assertTrue(pr.replace("keyForClient", "value2", "newValue"));
        return null;
      }
    });
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        final Region r = getCache().getRegion(REP_REG_NAME);
        final Region pr = getCache().getRegion(PR_REG_NAME);
        assertFalse(r.containsKey("nonExistentkey"));
        assertFalse(pr.containsKey("nonExistentkey"));
        return null;
      }
    });
  }
  
  public void testBug42167() {
    do42167Work(false, REP_REG_NAME);
  }
  public void testBug42167PR() {
    do42167Work(false, PR_REG_NAME);
  }
  public void testBug42167Empty() {
    do42167Work(true, REP_REG_NAME);
  }
  public void testBug42167EmptyPR() {
    do42167Work(true, PR_REG_NAME);
  }
  private void do42167Work(final boolean emptyClient, final String regionName) {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(2);
    int port1 = createRegionsAndStartServer(server);
    
    createClientRegion(client, port1, emptyClient, -1);
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        r.put("key0", "value");
        r.put("key2", "value2");
        return null;
      }
    });
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        assertEquals("value", r.get("key0"));
        if (!emptyClient) {
          r.localDestroy("key0");
          assertFalse(r.containsKey("key0"));
        }
        getCache().getLogger().fine("SWAP:doingRemove");
        assertTrue(r.remove("key0", "value"));
        
        getCache().getLogger().fine("Bruce:doingExtraRemoves.  Bug #47010");
        DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
        assertTrue(r.remove("key0") == null);
        assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

        DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
        assertFalse(r.remove("key0", "value"));
        assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

        DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
        assertTrue(r.destroy("key0") == null);
        assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

        DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
        assertTrue(r.remove("nonExistentKey1") == null);
        assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

        DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
        assertFalse(r.remove("nonExistentKey2", "value"));
        assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

        DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
        assertTrue(r.destroy("nonExistentKey3") == null);
        assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);
        
        getCache().getLogger().fine("SWAP:doingReplace");
        assertEquals("value2", r.replace("key2", "newValue2"));
        getCache().getLogger().fine("SWAP:doingReplace2");
        assertEquals(null, r.replace("key0", "newValue"));
        assertNull(r.putIfAbsent("key4", "value4"));
        return null;
      }
    });
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        assertFalse(r.containsKey("key0"));
        assertFalse(r.containsValueForKey("key0"));
        assertTrue(r.containsKey("key2"));
        assertEquals("newValue2", r.get("key2"));
        r.getCache().getLogger().fine("SWAP:doingGet");
        assertEquals("value4", r.get("key4"));
        return null;
      }
    });
  }
  
  public void testBug42189() {
    doBug42189Work(REP_REG_NAME);
  }
  public void testBug42189PR() {
    doBug42189Work(PR_REG_NAME);
  }
  private void doBug42189Work(final String regionName) {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(2);
    int port1 = createRegionsAndStartServer(server);
    
    createClientRegion(client, port1, false, -1);
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(regionName);
        r.create("key0", null);
        assertNull(r.putIfAbsent("key0", "value"));
        assertTrue(r.containsKey("key0"));
        Object v = r.get("key0");
        assertNull("expected null but was "+v, v);
        return null;
      }
    });
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(regionName);
        assertNull(r.putIfAbsent("key0", "value"));
        assertTrue(r.containsKeyOnServer("key0"));
        Object v = r.get("key0");
        assertNull("expected null but was "+v, v);
        return null;
      }
    });
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(regionName);
        assertTrue(r.containsKey("key0"));
        assertFalse(r.containsValueForKey("key0"));
        return null;
      }
    });
  }
  
  public void _testBug42195() {
    doPutIfAbsentPutsKeyInLocalClientCacheWork(REP_REG_NAME);
  }
  public void _testBug42195PR() {
    doPutIfAbsentPutsKeyInLocalClientCacheWork(PR_REG_NAME);
  }
  private void doPutIfAbsentPutsKeyInLocalClientCacheWork(final String regionName) {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(2);
    int port1 = createRegionsAndStartServer(server);
    
    createClientRegion(client, port1, false, -1);
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(regionName);
        assertNull(r.putIfAbsent("key0", "value"));
        assertTrue(r.containsKey("key0"));
        return null;
      }
    });
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(regionName);
        assertEquals("value", r.putIfAbsent("key0", "newValue"));
        assertTrue(r.containsKeyOnServer("key0"));
        assertTrue(r.containsKey("key0"));
        assertTrue(r.containsValueForKey("key0"));
        return null;
      }
    });
  }
  
  public void testReplacePutsKeyInLocalClientCache() {
    doReplacePutsKeyInLocalClientCacheWork(REP_REG_NAME);
  }
  public void testReplacePutsKeyInLocalClientCachePR() {
    doReplacePutsKeyInLocalClientCacheWork(PR_REG_NAME);
  }
  private void doReplacePutsKeyInLocalClientCacheWork(final String regionName) {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(2);
    int port1 = createRegionsAndStartServer(server);
    
    createClientRegion(client, port1, false, -1);
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(regionName);
        assertNull(r.putIfAbsent("key0", "value"));
        assertTrue(r.containsKey("key0"));
        assertNull(r.putIfAbsent("key2", "value2"));
        assertTrue(r.containsKey("key2"));
        return null;
      }
    });
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(regionName);
        assertEquals("value", r.replace("key0", "newValue"));
        assertTrue(r.containsKeyOnServer("key0"));
        assertTrue(r.containsKey("key0"));
        assertTrue(r.containsValueForKey("key0"));
        
        assertFalse(r.replace("key2", "DontReplace", "newValue"));
        assertTrue(r.replace("key2", "value2", "newValu2"));
        assertTrue(r.containsKeyOnServer("key2"));
        assertTrue(r.containsKey("key2"));
        assertTrue(r.containsValueForKey("key2"));
        return null;
      }
    });
    // bug #42221 - replace does not put entry on client when server has invalid value
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        final String key = "bug42221";
        r.putIfAbsent(key, null);
        assertTrue(r.containsKey(key));
        Object result = r.replace(key, "not null");
        assertEquals(null, result);
        assertTrue(r.containsKey(key));
        assertEquals(r.get(key), "not null");
        r.remove(key); // cleanup
        return null;
      }
    });
    // bug #42242 - remove(K,null) doesn't work
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        final String key = "bug42242";
        r.putIfAbsent(key, null);
        assertTrue(r.containsKey(key));
        assertTrue(r.containsKeyOnServer(key));
        boolean result = r.remove(key, null);
        assertTrue(result);
        assertFalse(r.containsKey(key));
        assertFalse(r.containsKeyOnServer(key));
        return null;
      }
    });
    // bug #42242b - second scenario with a replace(K,V,V) that didn't work
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        final String key = "bug42242b";
        r.putIfAbsent(key, null);
        assertTrue(r.containsKey(key));
        assertTrue(r.containsKeyOnServer(key));
        boolean result = r.replace(key, null, "new value");
        assertTrue(result);
        result = r.remove(key, "new value");
        assertTrue(result);
        assertFalse(r.containsKey(key));
        assertFalse(r.containsKeyOnServer(key));
        return null;
      }
    });
    // bug #42242c - remove does not work for entry that's on the server but not on the client
    final String key = "bug42242c";
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        r.registerInterest("ALL_KEYS");
        return null;
      }});
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        r.putIfAbsent(key, null);
        assertTrue(r.containsKey(key));
        return null;
      }});
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(regionName);
        WaitCriterion w = new WaitCriterion() {
          public String description() {
            return "waiting for server operation to reach client";
          }
          public boolean done() {
            return r.containsKey(key);
          }
        };
        waitForCriterion(w, 10000, 200, true);
        assertTrue(r.containsKeyOnServer(key));
        boolean result = r.remove(key, null);
//        if (!result) {
//          ((LocalRegion)r).dumpBackingMap();
//        }
        assertTrue(result);
        assertFalse(r.containsKey(key));
        assertFalse(r.containsKeyOnServer(key));
        return null;
      }
    });
  }
  public void testWithDelta() {
    doTestWithDeltaWork(false, REP_REG_NAME);
  }
  public void testWithDeltaPR() {
    doTestWithDeltaWork(false, PR_REG_NAME);
  }  
  public void testWithDeltaCS() {
    doTestWithDeltaWork(true, REP_REG_NAME);
  }
  public void testWithDeltaPRCS() {
    doTestWithDeltaWork(true, PR_REG_NAME);
  }
  private void doTestWithDeltaWork(final boolean clientServer, final String regName) {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    
    if (clientServer) {
      int port = createRegionsAndStartServer(vm1);
      createClientRegion(vm2, port, false, -1);
    } else {
      createRegions(vm1);
      createRegions(vm2);
    }
    
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(regName);
        CustomerDelta c = new CustomerDelta("cust1", "addr1");
        assertNull(r.putIfAbsent("k1", c));
        CustomerDelta newc = new CustomerDelta(c);
        newc.setAddress("updatedAddress");
        assertEquals(c, r.putIfAbsent("k1", c));
        assertEquals(c, r.replace("k1", newc));
        assertFalse(r.replace("k1", c, newc));
        assertTrue(r.replace("k1", newc, c));
        assertFalse(r.remove("k1", newc));
        assertTrue(r.remove("k1", c));
        return null;
      }
    });
  }
  
  /** test putIfAbsent with failover & retry.  This is bugs 42559 and 43640 */
  public void testRetriedPutIfAbsent() throws Exception {
    doRetriedOperation(Operation.PUT_IF_ABSENT, false);
  }
  
  public void testRetriedReplace() throws Exception {
    doRetriedOperation(Operation.REPLACE, false);
  }

  public void testRetriedRemove() throws Exception {
    doRetriedOperation(Operation.REMOVE, false);
  }

  public void testRetriedPutIfAbsentPR() throws Exception {
    doRetriedOperation(Operation.PUT_IF_ABSENT, false);
  }
  
  public void testRetriedReplacePR() throws Exception {
    doRetriedOperation(Operation.REPLACE, false);
  }

  public void testRetriedRemovePR() throws Exception {
    doRetriedOperation(Operation.REMOVE, false);
  }
  private void doRetriedOperation(final Operation op, boolean usePR) {
    Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client = host.getVM(2);
    final int port1 = createRegionsAndStartServer(server1, true);
    final int port2 = createRegionsAndStartServer(server2, true);
    final String regionName = usePR? PR_REG_NAME : REP_REG_NAME;

    addExpectedException("java.net.SocketException");
    
    createClientRegion(client, port1, false, port2);
    
    SerializableCallable getID = new SerializableCallable("get DM ID") {
      public Object call() {
        return getSystem().getDistributedMember();
      }
    };

    final DistributedMember server1ID = (DistributedMember)server1.invoke(getID);
    final DistributedMember server2ID = (DistributedMember)server2.invoke(getID);
    
    Set<ExpectedException> exceptions = new HashSet<ExpectedException>();
    exceptions.add(addExpectedException("Membership: requesting removal", server1));
    exceptions.add(addExpectedException("Membership: requesting removal", server2));
    exceptions.add(addExpectedException("ForcedDisconnect", server1));
    exceptions.add(addExpectedException("ForcedDisconnect", server2));
    
    try {

      server1.invoke(new SerializableCallable("install crasher in server1") {
        public Object call() throws Exception {
          Region r = getCache().getRegion(regionName);
          r.put("key0", "value");
          if (op == Operation.PUT_IF_ABSENT) {
            r.destroy("key0");
          }
          // force client to use server1 for now
//          getCache().getCacheServers().get(0).stop();
          r.getAttributesMutator().addCacheListener(new CacheListenerAdapter() {
            private void killSender(EntryEvent event) {
              if (event.isOriginRemote()) {
                MembershipManager mgr = MembershipManagerHelper.getMembershipManager(getSystem());
                mgr.requestMemberRemoval(server2ID, "removing for test");
                try {
                  mgr.waitForDeparture(server2ID);
                } catch (Exception e) {
                  fail("failed to stop the other server for this test:" + e.getMessage());
                }
              }
            }
            @Override
            public void afterCreate(EntryEvent event) {
              getCache().getLogger().info("afterCreate invoked with " + event);
              killSender(event);
            }
            @Override
            public void afterUpdate(EntryEvent event) {
              getCache().getLogger().info("afterUpdate invoked with " + event);
              killSender(event);
            }
            @Override
            public void afterDestroy(EntryEvent event) {
              getCache().getLogger().info("afterDestroy invoked with " + event);
              killSender(event);
            }
          });
          return null;
        }
      });

      server2.invoke(new SerializableCallable("install crasher in server2") {
        public Object call() throws Exception {
          Region r = getCache().getRegion(regionName);
          // force client to use server1 for now
//          getCache().getCacheServers().get(0).stop();
          r.getAttributesMutator().addCacheListener(new CacheListenerAdapter() {
            private void killSender(EntryEvent event) {
              if (event.isOriginRemote()) {
                MembershipManager mgr = MembershipManagerHelper.getMembershipManager(getSystem());
                mgr.requestMemberRemoval(server1ID, "removing for test");
                try {
                  mgr.waitForDeparture(server1ID);
                } catch (Exception e) {
                  fail("failed to stop the other server for this test:" + e.getMessage());
                }
              }
            }
            @Override
            public void afterCreate(EntryEvent event) {
              getCache().getLogger().info("afterCreate invoked with " + event);
              killSender(event);
            }
            @Override
            public void afterUpdate(EntryEvent event) {
              getCache().getLogger().info("afterUpdate invoked with " + event);
              killSender(event);
            }
            @Override
            public void afterDestroy(EntryEvent event) {
              getCache().getLogger().info("afterDestroy invoked with " + event);
              killSender(event);
            }
          });
          return null;
        }
      });
      

      client.invoke(new SerializableRunnable() {
        public void run() {
          GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
          Region r = cache.getRegion(regionName);
          if (op == Operation.PUT_IF_ABSENT) {
            assertTrue("expected putIfAbsent to succeed and return null",
              r.putIfAbsent("key0", "newvalue") == null);
          } else if (op == Operation.REMOVE) {
            assertTrue("expected remove operation to succeed and return true",
                r.remove("key0", "value"));
          } else if (op == Operation.REPLACE) {
            assertTrue("expected replace operation to succeed and return true",
                r.replace("key0", "value", "newvalue"));
          }
        }
      });
    } finally {
      disconnectAllFromDS();
      for (ExpectedException ex: exceptions) {
        ex.remove();
      }
    }
  }
  
  private static class CustomerDelta implements Serializable, Delta {
    private String name;
    private String address;
    private boolean nameChanged;
    private boolean addressChanged;
    public CustomerDelta(CustomerDelta o) {
      this.address = o.address;
      this.name = o.name;
    }
    public CustomerDelta(String name, String address) {
      this.name = name;
      this.address = address;
    }
    public void fromDelta(DataInput in) throws IOException,
        InvalidDeltaException {
      boolean nameC = in.readBoolean();
      if (nameC) {
        this.name = in.readUTF();
      }
      boolean addressC = in.readBoolean();
      if (addressC) {
        this.address = in.readUTF();
      }
    }
    public boolean hasDelta() {
      return nameChanged || addressChanged;
    }
    public void toDelta(DataOutput out) throws IOException {
      if (this.nameChanged) {
        out.writeBoolean(nameChanged);
        out.writeUTF(name);
      }
      if (this.addressChanged) {
        out.writeBoolean(addressChanged);
        out.writeUTF(address);
      }
    }
    public void setName(String name) {
      this.nameChanged = true;
      this.name = name;
    }
    public String getName() {
      return name;
    }
    public void setAddress(String address) {
      this.addressChanged = true;
      this.address = address;
    }
    public String getAddress() {
      return address;
    }
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof CustomerDelta)) return false;
      CustomerDelta other = (CustomerDelta)obj;
      return this.name.equals(other.name) && this.address.equals(other.address);
    }
    @Override
    public int hashCode() {
      return this.address.hashCode() + this.name.hashCode();
    }
  }
}
