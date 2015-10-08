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

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;

import dunit.*;
//import hydra.ClientMgr;

/**
 * This class tests various search load and write scenarios for distributed regions
 * @author Sudhir Menon
 *
 */
public class SearchAndLoadDUnitTest extends CacheTestCase {

  static boolean loaderInvoked;
  static boolean  remoteLoaderInvoked;
  static int remoteLoaderInvokedCount;
  static boolean netSearchCalled;
  static boolean netSearchHit;
  static boolean netWriteInvoked;
  static boolean operationWasCreate;
  static boolean originWasRemote;
  static int writerInvocationCount;

  /** A <code>CacheListener</code> used by a test */
  protected static TestCacheListener listener;

  /** A <code>CacheLoader</code> used by a test */
  protected static TestCacheLoader loader;

  /** A <code>CacheWriter</code> used by a test */
  protected static TestCacheWriter writer;

  public SearchAndLoadDUnitTest(String name) {
    super(name);
  }

  public void tearDown2() throws Exception {
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      for (int v = 0; v < host.getVMCount(); v++) {
        host.getVM(v).invoke(new SerializableRunnable("Clean up") {
            public void run() {
              cleanup();
            }
          });
          // already called in every VM in super.tearDown
//        host.getVM(v).invoke(this.getClass(), "remoteTearDown");
      }
    }
    cleanup();
    super.tearDown2();
  }

  /**
   * Clears fields used by a test
   */
  protected static void cleanup() {
    listener = null;
    loader = null;
    writer = null;
  }

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEarlyAck(false);
    return factory.create();
  }

  public void testNetSearch()
  throws CacheException, InterruptedException {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String name = this.getUniqueName() + "-ACK";
    final String objectName = "NetSearchKey";
    final Integer value = new Integer(440);
    vm0.invoke(new SerializableRunnable("Create ACK Region") {
      public void run() {
        try {

          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setEarlyAck(false);
          factory.setStatisticsEnabled(true);
          Region region = createRegion(name,factory.create());
          region.create(objectName,null);
        }
        catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });

    vm1.invoke(new SerializableRunnable("Create ACK Region") {
      public void run() {
        try {

          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setEarlyAck(false);
          factory.setStatisticsEnabled(true);
          Region region = createRegion(name,factory.create());
          region.put(objectName,value);

        }
        catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });

    vm2.invoke(new SerializableRunnable("Create ACK Region") {
      public void run() {
        try {

          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setEarlyAck(false);
          factory.setStatisticsEnabled(true);
          Region region = createRegion(name,factory.create());
          region.create(objectName,null);

        }
        catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });

    vm0.invoke(new SerializableRunnable("Get a value") {
      public void run() {
        try {
          Object result = null;
          result = getRootRegion().getSubregion(name).get(objectName);
          assertEquals(value,result);

//         System.err.println("Results is " + result.toString() + " Key is " + objectName.toString());
        }
        catch(CacheLoaderException cle) {
          fail("While Get a value", cle);
        }
        catch(TimeoutException te) {
          fail("While Get a value", te);
        }
      }

    });
  }

  public void testNetLoadNoLoaders()
  throws CacheException, InterruptedException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName() + "-ACK";
    final String objectName = "B";
    SerializableRunnable create =
          new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        AttributesFactory factory =
          new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setEarlyAck(false);
        createRegion(name,factory.create());

      }
    };

    vm0.invoke(create);
    vm1.invoke(create);

    vm0.invoke(new SerializableRunnable("Get with No Loaders defined") {
      public void run() {
        try {
          Object result = getRootRegion().getSubregion(name).get(objectName);
          assertNull(result);
        }
        catch(CacheLoaderException cle) {
          fail("While getting value for ACK region", cle);
        }
        catch(TimeoutException te) {
          fail("While getting value for ACK region", te);
        }

      }
    });


  }

  public void testNetLoad()
  throws CacheException, InterruptedException {
    invokeInEveryVM(DistributedTestCase.class,
        "disconnectFromDS");

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName() + "-ACK";
    final String objectName = "B";
    final Integer value = new Integer(43);
    loaderInvoked = false;
    remoteLoaderInvoked = false;
    vm0.invoke(new SerializableRunnable("Create ACK Region") {
      public void run() {
        try {
          loaderInvoked = false;
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setEarlyAck(false);
      //  factory.setCacheLoader(new CacheLoader() {
      //      public Object load(LoaderHelper helper) {
      ///        loaderInvoked = true;
      //        return value;
      //      }
//
      //      public void close() {
//
      //      }
      //    });

          Region region = createRegion(name,factory.create());
          region.create(objectName,null);

        }
        catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });

    vm1.invoke(new SerializableRunnable("Create ACK Region") {
      public void run() {
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setEarlyAck(false);
          factory.setCacheLoader(new CacheLoader() {
            public Object load(LoaderHelper helper) {
              remoteLoaderInvoked = true;
              return value;
            }
            public void close() {

            }
          });
          createRegion(name,factory.create());
        }
        catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });
    vm0.invoke(new SerializableRunnable("Get a value from remote loader") {
      public void run() {
        for (int i=0;i< 1;i++) {
          try {
            Object result = getRootRegion().getSubregion(name).get(objectName);
            assertEquals(value,result);
            assertEquals(new Boolean(loaderInvoked),Boolean.FALSE);
           // getRootRegion().getSubregion(name).invalidate(objectName);

          }
          catch(CacheLoaderException cle) {
            fail("While getting value for ACK region", cle);

          }
/*        catch(EntryNotFoundException enfe) {
            fail("While getting value for ACK region", enfe);

          }*/
          catch(TimeoutException te) {
            fail("While getting value for ACK region", te);

          }
        }
      }

    });
  }

  /**
   * Confirm that a netLoad that returns null will NOT allow other netLoad methods
   * to be called.
   */
  public void testEmptyNetLoad()
  throws CacheException, InterruptedException {
    invokeInEveryVM(DistributedTestCase.class,
        "disconnectFromDS");

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String name = this.getUniqueName() + "-ACK";
    final String objectName = "B";
    final Integer value = new Integer(43);
    loaderInvoked = false;
    remoteLoaderInvoked = false;
    remoteLoaderInvokedCount = 0;
    vm0.invoke(new SerializableRunnable("Create ACK Region") {
      public void run() {
        loaderInvoked = false;
        remoteLoaderInvoked = false;
        remoteLoaderInvokedCount = 0;
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setEarlyAck(false);
      //  factory.setCacheLoader(new CacheLoader() {
      //      public Object load(LoaderHelper helper) {
      ///        loaderInvoked = true;
      //        return value;
      //      }
//
      //      public void close() {
//
      //      }
      //    });
          Region region = createRegion(name,factory.create());
          region.create(objectName,null);
        }
        catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });

    SerializableRunnable installLoader = new SerializableRunnable("Create ACK Region") {
        public void run() {
          loaderInvoked = false;
          remoteLoaderInvoked = false;
          remoteLoaderInvokedCount = 0;
          try {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setEarlyAck(false);
            factory.setCacheLoader(new CacheLoader() {
                public Object load(LoaderHelper helper) {
                  remoteLoaderInvoked = true;
                  remoteLoaderInvokedCount++;
                  return null;
                }
                public void close() {

                }
              });
            Region region = createRegion(name,factory.create());
          }
          catch (CacheException ex) {
            fail("While creating ACK region", ex);
          }
        }
      };
    vm1.invoke(installLoader);
    vm2.invoke(installLoader);
    vm0.invoke(new SerializableRunnable("Get a value from remote loader") {
      public void run() {
        for (int i=0;i< 1;i++) {
          try {
            Object result = getRootRegion().getSubregion(name).get(objectName);
            assertEquals(null,result);
            assertEquals(false, loaderInvoked);
           // getRootRegion().getSubregion(name).invalidate(objectName);

          }
          catch(CacheLoaderException cle) {
            fail("While getting value for ACK region", cle);

          }
/*        catch(EntryNotFoundException enfe) {
            fail("While getting value for ACK region", enfe);

          }*/
          catch(TimeoutException te) {
            fail("While getting value for ACK region", te);

          }
        }
      }

    });
    // we only invoke one netLoad loader even when they return null.
    boolean xor = vmRemoteLoaderInvoked(vm1) ^ vmRemoteLoaderInvoked(vm2);
    assertEquals("vm1=" + vmRemoteLoaderInvoked(vm1) + " vm2=" + vmRemoteLoaderInvoked(vm2)
                 + " vm1Count=" + vmRemoteLoaderInvokedCount(vm1) + " vm2Count=" + vmRemoteLoaderInvokedCount(vm2), true, xor);
    int total = vmRemoteLoaderInvokedCount(vm1) + vmRemoteLoaderInvokedCount(vm2);
    assertEquals("vm1=" + vmRemoteLoaderInvokedCount(vm1) + " vm2=" + vmRemoteLoaderInvokedCount(vm2), 1, total);
  }

  public static boolean vmRemoteLoaderInvoked(VM vm) {
    Boolean v = (Boolean)vm.invoke(SearchAndLoadDUnitTest.class, "fetchRemoteLoaderInvoked");
    return v.booleanValue();
  }
  public static int vmRemoteLoaderInvokedCount(VM vm) {
    Integer v = (Integer)vm.invoke(SearchAndLoadDUnitTest.class, "fetchRemoteLoaderInvokedCount");
    return v.intValue();
  }

  public static Boolean fetchRemoteLoaderInvoked() {
    return Boolean.valueOf(remoteLoaderInvoked);
  }
  public static Integer fetchRemoteLoaderInvokedCount() {
    return new Integer(remoteLoaderInvokedCount);
  }
  
  public void testLocalLoad()
  throws CacheException, InterruptedException {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName() + "-ACK";
    final String objectName = "C";
    final Integer value = new Integer(44);
    remoteLoaderInvoked = false;
    loaderInvoked = false;
    vm0.invoke(new SerializableRunnable("Create ACK Region") {
      public void run() {
        remoteLoaderInvoked = false;
        loaderInvoked = false;
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setEarlyAck(false);
          factory.setCacheLoader(new CacheLoader() {
            public Object load(LoaderHelper helper) {
              loaderInvoked = true;
              return value;
            }

            public void close() {

            }
          });

          Region region = createRegion(name,factory.create());
          region.create(objectName,null);

        }
        catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });

    vm1.invoke(new SerializableRunnable("Create ACK Region") {
      public void run() {
        remoteLoaderInvoked = false;
        loaderInvoked = false;
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setEarlyAck(false);
          factory.setCacheLoader(new CacheLoader() {
            public Object load(LoaderHelper helper) {
              remoteLoaderInvoked = true;
              return value;
            }
            public void close() {

            }
          });
          createRegion(name,factory.create());
        }
        catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });
    vm0.invoke(new SerializableRunnable("Get a value from local loader") {
      public void run() {
        try {
          Object result = getRootRegion().getSubregion(name).get(objectName);
          assertEquals(value,result);
          assertEquals(new Boolean(loaderInvoked),Boolean.TRUE);
          assertEquals(new Boolean(remoteLoaderInvoked),Boolean.FALSE);

        }
        catch(CacheLoaderException cle) {

        }
        catch(TimeoutException te) {
        }
      }

    });
  }


  public void testNetWrite()
  throws CacheException, InterruptedException {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName() + "-ACK";
    final String objectName = "Gemfire7";
    final Integer value = new Integer(483);

    vm0.invoke(new SerializableRunnable("Create ACK Region with cacheWriter") {
      public void run() {
        netWriteInvoked = false;
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setCacheWriter(new CacheWriter() {
            public void beforeCreate(EntryEvent e) throws CacheWriterException {
              netWriteInvoked = true;
              return;
            }
            public void beforeUpdate(EntryEvent e) throws CacheWriterException {
              netWriteInvoked = true;
              return;
            }
            public void beforeDestroy(EntryEvent e) throws CacheWriterException {
              return;
            }
            public void beforeRegionDestroy(RegionEvent e) throws CacheWriterException {
              return;
            }
            public void beforeRegionClear(RegionEvent e) throws CacheWriterException {
              return;
            }
            public void close() {
            }
          });

          createRegion(name,factory.create());

        }
        catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });
    vm1.invoke(new SerializableRunnable("Create ACK Region") {
        public void run() {
          loaderInvoked = false;
          remoteLoaderInvoked = false;
          netWriteInvoked = false;
          try {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            createRegion(name,factory.create());
          }
          catch (CacheException ex) {
            fail("While creating ACK region", ex);
          }
        }
    });

    vm1.invoke(new SerializableRunnable("Do a put operation resulting in cache writer notification in other vm") {
      public void run() {
        try {
          getRootRegion().getSubregion(name).put(objectName,value);
          try {
            Object result = getRootRegion().getSubregion(name).get(objectName);
            assertEquals(result,value);
           }
           catch(CacheLoaderException cle) {
           }
           catch(TimeoutException te) {
           }
        }
        catch(CacheWriterException cwe) {

        }
        catch(TimeoutException te) {
        }
      }

    });

    vm0.invoke(new SerializableRunnable("ensure that cache writer was invoked") {
      public void run() {
        assertTrue("expected cache writer to be invoked", netWriteInvoked);
      }
    });
  }

  
  public void testOneHopNetWrite() throws CacheException, InterruptedException {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName() + "Region";
    final String objectName = "Object7";
    final Integer value = new Integer(483);
    final Integer updateValue = new Integer(484);

    vm0.invoke(new SerializableRunnable("Create replicated region with cacheWriter") {
      public void run() {
        netWriteInvoked = false;
        operationWasCreate = false;
        originWasRemote = false;
        writerInvocationCount = 0;
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          factory.setCacheWriter(new CacheWriter() {
            public void beforeCreate(EntryEvent e) throws CacheWriterException {
              e.getRegion().getCache().getLogger().info("cache writer beforeCreate invoked for " + e);
              netWriteInvoked = true;
              operationWasCreate = true;
              originWasRemote = e.isOriginRemote();
              writerInvocationCount++;
              return;
            }
            public void beforeUpdate(EntryEvent e) throws CacheWriterException {
              e.getRegion().getCache().getLogger().info("cache writer beforeUpdate invoked for " + e);
              netWriteInvoked = true;
              operationWasCreate = false;
              originWasRemote = e.isOriginRemote();
              writerInvocationCount++;
              return;
            }
            public void beforeDestroy(EntryEvent e) throws CacheWriterException {  }
            public void beforeRegionDestroy(RegionEvent e) throws CacheWriterException {   }
            public void beforeRegionClear(RegionEvent e) throws CacheWriterException {   }
            public void close() { }
          });

          createRegion(name,factory.create());

        }
        catch (CacheException ex) {
          fail("While creating replicated region", ex);
        }
      }
    });
    vm1.invoke(new SerializableRunnable("Create empty Region") {
        public void run() {
          try {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.EMPTY);
            createRegion(name,factory.create());
          }
          catch (CacheException ex) {
            fail("While creating empty region", ex);
          }
        }
    });

    vm1.invoke(new SerializableRunnable("do a put that should be proxied in the other vm and invoke its cache writer") {
      public void run() {
        try {
          getRootRegion().getSubregion(name).put(objectName,value);
        } catch(CacheWriterException cwe) {
        } catch(TimeoutException te) {
        }
      }
    });

    vm0.invoke(new SerializableRunnable("ensure that cache writer was invoked with correct settings in event") {
      public void run() {
        assertTrue("expected cache writer to be invoked", netWriteInvoked);
        assertTrue("expected originRemote to be true", originWasRemote);
        assertTrue("expected event to be create", operationWasCreate);
        assertEquals("expected only one cache writer invocation", 1, writerInvocationCount);
        // set flags for the next test - updating the same key
        netWriteInvoked = false;
        writerInvocationCount = 0;
      }
    });

    vm1.invoke(new SerializableRunnable("do an update that should be proxied in the other vm and invoke its cache writer") {
      public void run() {
        try {
          getRootRegion().getSubregion(name).put(objectName,updateValue);
        } catch(CacheWriterException cwe) {
        } catch(TimeoutException te) {
        }
      }
    });

    vm0.invoke(new SerializableRunnable("ensure that cache writer was invoked with correct settings in event") {
      public void run() {
        assertTrue("expected cache writer to be invoked", netWriteInvoked);
        assertTrue("expected originRemote to be true", originWasRemote);
        assertTrue("expected event to be create", operationWasCreate);
        assertEquals("expected only one cache writer invocation", 1, writerInvocationCount);
      }
    });
  }


  /** same as the previous test but the cache writer is in a third, non-replicated, vm */
  public void testOneHopNetWriteRemoteWriter() throws CacheException, InterruptedException {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String name = this.getUniqueName() + "Region";
    final String objectName = "Object7";
    final Integer value = new Integer(483);
    final Integer updateValue = new Integer(484);

    vm0.invoke(new SerializableRunnable("Create replicate Region") {
      public void run() {
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          createRegion(name,factory.create());
        }
        catch (CacheException ex) {
          fail("While creating empty region", ex);
        }
      }
    });
    vm1.invoke(new SerializableRunnable("Create empty Region") {
        public void run() {
          try {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.EMPTY);
            createRegion(name,factory.create());
          }
          catch (CacheException ex) {
            fail("While creating empty region", ex);
          }
        }
    });
    vm2.invoke(new SerializableRunnable("Create replicated region with cacheWriter") {
      public void run() {
        netWriteInvoked = false;
        operationWasCreate = false;
        originWasRemote = false;
        writerInvocationCount = 0;
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.EMPTY);
          factory.setCacheWriter(new CacheWriter() {
            public void beforeCreate(EntryEvent e) throws CacheWriterException {
              e.getRegion().getCache().getLogger().info("cache writer beforeCreate invoked for " + e);
              netWriteInvoked = true;
              operationWasCreate = true;
              originWasRemote = e.isOriginRemote();
              writerInvocationCount++;
              return;
            }
            public void beforeUpdate(EntryEvent e) throws CacheWriterException {
              e.getRegion().getCache().getLogger().info("cache writer beforeUpdate invoked for " + e);
              netWriteInvoked = true;
              operationWasCreate = false;
              originWasRemote = e.isOriginRemote();
              writerInvocationCount++;
              return;
            }
            public void beforeDestroy(EntryEvent e) throws CacheWriterException {  }
            public void beforeRegionDestroy(RegionEvent e) throws CacheWriterException {   }
            public void beforeRegionClear(RegionEvent e) throws CacheWriterException {   }
            public void close() { }
          });

          createRegion(name,factory.create());

        }
        catch (CacheException ex) {
          fail("While creating replicated region", ex);
        }
      }
    });

    vm1.invoke(new SerializableRunnable("do a put that should be proxied in the other vm and invoke its cache writer") {
      public void run() {
        try {
          getRootRegion().getSubregion(name).put(objectName,value);
        } catch(CacheWriterException cwe) {
        } catch(TimeoutException te) {
        }
      }
    });

    vm2.invoke(new SerializableRunnable("ensure that cache writer was invoked with correct settings in event") {
      public void run() {
        assertTrue("expected cache writer to be invoked", netWriteInvoked);
        assertTrue("expected originRemote to be true", originWasRemote);
        assertTrue("expected event to be create", operationWasCreate);
        assertEquals("expected only one cache writer invocation", 1, writerInvocationCount);
        // set flags for the next test - updating the same key
        netWriteInvoked = false;
        writerInvocationCount = 0;
      }
    });

    vm1.invoke(new SerializableRunnable("do an update that should be proxied in the other vm and invoke its cache writer") {
      public void run() {
        try {
          getRootRegion().getSubregion(name).put(objectName,updateValue);
        } catch(CacheWriterException cwe) {
        } catch(TimeoutException te) {
        }
      }
    });

    vm2.invoke(new SerializableRunnable("ensure that cache writer was invoked with correct settings in event") {
      public void run() {
        assertTrue("expected cache writer to be invoked", netWriteInvoked);
        assertTrue("expected originRemote to be true", originWasRemote);
        assertTrue("expected event to be create", operationWasCreate);
        assertEquals("expected only one cache writer invocation", 1, writerInvocationCount);
      }
    });
  }
}


