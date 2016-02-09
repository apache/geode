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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This class tests various search load and write scenarios for distributed regions
 */
@SuppressWarnings({"deprecation", "unchecked", "rawtypes", "serial"})
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

  static boolean exceptionThrown;
  static final CountDownLatch readyForExceptionLatch = new CountDownLatch(1);
  static final CountDownLatch loaderInvokedLatch = new CountDownLatch(1);

  public SearchAndLoadDUnitTest(String name) {
    super(name);
  }

  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      for (int v = 0; v < host.getVMCount(); v++) {
        host.getVM(v).invoke(new SerializableRunnable("Clean up") {
            public void run() {
              cleanup();
            }
          });
      }
    }
    cleanup();
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
          Assert.fail("While creating ACK region", ex);
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
          Assert.fail("While creating ACK region", ex);
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
          Assert.fail("While creating ACK region", ex);
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
          Assert.fail("While Get a value", cle);
        }
        catch(TimeoutException te) {
          Assert.fail("While Get a value", te);
        }
      }

    });
  }


  /**
   * This test is for a bug in which a cache loader threw an exception
   * that caused the wrong value to be put in a Future in nonTxnFindObject.  This
   * in turn caused a concurrent search for the object to not invoke the loader a
   * second time.
   * 
   * VM0 is used to create a cache and a region having a loader that simulates the
   * conditions that caused the bug.  One async thread then does a get() which invokes
   * the loader.  Another async thread does a get() which reaches nonTxnFindObject
   * and blocks waiting for the first thread's load to complete.  The loader then
   * throws an exception that is sent back to the first thread.  The second thread
   * should then cause the loader to be invoked again, and this time the loader will
   * return a value.  Both threads then validate that they received the expected
   * result.
   */
  public void testConcurrentLoad() throws Throwable {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    final String name = this.getUniqueName() + "Region";
    final String objectName = "theKey";
    final Integer value = new Integer(44);
    final String exceptionString = "causing first cache-load to fail";

    remoteLoaderInvoked = false;
    loaderInvoked = false;
    
    vm0.invoke(new CacheSerializableRunnable("create region " + name + " in vm0") {
      public void run2() {
        remoteLoaderInvoked = false;
        loaderInvoked = false;
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setConcurrencyChecksEnabled(true);
        factory.setCacheLoader(new CacheLoader() {
          boolean firstInvocation = true;
          public synchronized Object load(LoaderHelper helper) {
            System.out.println("invoked cache loader for " + helper.getKey());
            loaderInvoked = true;
            loaderInvokedLatch.countDown();
            if (firstInvocation) {
              firstInvocation = false;
              try { 
                // wait for both threads to be ready for the exception to be thrown
                System.out.println("waiting for vm0t2 to be ready before throwing exception");
                readyForExceptionLatch.await(30, TimeUnit.SECONDS);
                // give the second thread time to get into loader code
                Thread.sleep(5000);
              } catch (InterruptedException e) {
                fail("interrupted");
              }
              System.out.println("throwing exception");
              exceptionThrown = true;
              throw new RuntimeException(exceptionString);
            }
            System.out.println("returning value="+value);
            return value;
          }

          public void close() {

          }
        });

        Region region = createRegion(name,factory.create());
        region.create(objectName, null);
        IgnoredException.addIgnoredException(exceptionString);
      }
    });

    AsyncInvocation async1 = null;
    try {
      async1 = vm0.invokeAsync(new CacheSerializableRunnable("Concurrently invoke the remote loader on the same key - t1") {
        public void run2() {
          Region region = getCache().getRegion("root/"+name);
  
          LogWriterUtils.getLogWriter().info("t1 is invoking get("+objectName+")");
          try {
            LogWriterUtils.getLogWriter().info("t1 retrieved value " + region.get(objectName));
            fail("first load should have triggered an exception");
          } catch (RuntimeException e) {
            if (!e.getMessage().contains(exceptionString)) {
              throw e;
            }
          }
        }
      });
      vm0.invoke(new CacheSerializableRunnable("Concurrently invoke the loader on the same key - t2") {
        public void run2() {
          final Region region = getCache().getRegion("root/"+name);
          final Object[] valueHolder = new Object[1];
  
          // wait for vm1 to cause the loader to be invoked
          LogWriterUtils.getLogWriter().info("t2 is waiting for loader to be invoked by t1");
          try {
            loaderInvokedLatch.await(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          assertTrue(loaderInvoked);
          
          Thread t = new Thread("invoke get()") {
            public void run() {
              try {
                valueHolder[0] = region.get(objectName);
              } catch (RuntimeException e) {
                valueHolder[0] = e;
              }
            }
          };
          
          t.setDaemon(true);
          t.start();
          try {
            // let the thread get to the point of blocking on vm1's Future
            // in LocalRegion.nonTxnFindObject()
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          
          readyForExceptionLatch.countDown();
          try {
            t.join(30000);
          } catch (InterruptedException e) {
            fail("interrupted");
          }
          if (t.isAlive()) {
            t.interrupt();
            fail("get() operation blocked for too long - test needs some work");
          }
          
          LogWriterUtils.getLogWriter().info("t2 is invoking get("+objectName+")");
          Object value = valueHolder[0];
          if (value instanceof RuntimeException) {
            if ( ((Exception)value).getMessage().contains(exceptionString) ) {
              fail("second load should not have thrown an exception");
            } else {
              throw (RuntimeException)value;
            }
          } else {
            LogWriterUtils.getLogWriter().info("t2 retrieved value " + value);
            assertNotNull(value);
          }
        }
      });
    } finally {
      if (async1 != null) {
        async1.join();
        if (async1.exceptionOccurred()) {
          throw async1.getException();
        }
      }
    }
  }
  
  
  public void testNetLoadNoLoaders() throws CacheException, InterruptedException {
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
          Assert.fail("While getting value for ACK region", cle);
        }
        catch(TimeoutException te) {
          Assert.fail("While getting value for ACK region", te);
        }

      }
    });


  }

  public void testNetLoad()
  throws CacheException, InterruptedException {
    Invoke.invokeInEveryVM(DistributedTestCase.class,
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
          Assert.fail("While creating ACK region", ex);
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
          Assert.fail("While creating ACK region", ex);
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
            Assert.fail("While getting value for ACK region", cle);

          }
/*        catch(EntryNotFoundException enfe) {
            fail("While getting value for ACK region", enfe);

          }*/
          catch(TimeoutException te) {
            Assert.fail("While getting value for ACK region", te);

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
    Invoke.invokeInEveryVM(DistributedTestCase.class,
        "disconnectFromDS");

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String name = this.getUniqueName() + "-ACK";
    final String objectName = "B";
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
          Assert.fail("While creating ACK region", ex);
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
            createRegion(name,factory.create());
          }
          catch (CacheException ex) {
            Assert.fail("While creating ACK region", ex);
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
            Assert.fail("While getting value for ACK region", cle);

          }
/*        catch(EntryNotFoundException enfe) {
            fail("While getting value for ACK region", enfe);

          }*/
          catch(TimeoutException te) {
            Assert.fail("While getting value for ACK region", te);

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
          Assert.fail("While creating ACK region", ex);
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
          Assert.fail("While creating ACK region", ex);
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
          Assert.fail("While creating ACK region", ex);
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
            Assert.fail("While creating ACK region", ex);
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
          Assert.fail("While creating replicated region", ex);
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
            Assert.fail("While creating empty region", ex);
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
          Assert.fail("While creating empty region", ex);
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
            Assert.fail("While creating empty region", ex);
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
          Assert.fail("While creating replicated region", ex);
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


