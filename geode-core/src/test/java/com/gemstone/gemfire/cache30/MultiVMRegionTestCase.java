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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.apache.logging.log4j.Logger;
import org.junit.Ignore;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.TransactionEvent;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.TransactionListener;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.EntryExpiryTask;
import com.gemstone.gemfire.internal.cache.ExpiryTask;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.TombstoneService;
import com.gemstone.gemfire.internal.cache.delta.Delta;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionHolder;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VMRegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.offheap.MemoryChunkWithRefCount;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;


/**
 * Abstract superclass of {@link Region} tests that involve more than
 * one VM.
 */
public abstract class MultiVMRegionTestCase extends RegionTestCase {

  private static final Logger logger = LogService.getLogger();
  
  Properties props = new Properties();

  final int putRange_1Start = 1;

  final int putRange_1End = 5;

  final int putRange_2Start = 6;

  final int putRange_2End = 10;

  final int putRange_3Start = 11;

  final int putRange_3End = 15;

  final int putRange_4Start = 16;

  final int putRange_4End = 20;

  final int removeRange_1Start = 2;

  final int removeRange_1End = 4;

  final int removeRange_2Start = 7;

  final int removeRange_2End = 9;

  public MultiVMRegionTestCase(String name) {
    super(name);
  }
  
  public static void caseTearDown() throws Exception {
    disconnectAllFromDS();
  }
  
  @Override
  protected final void postTearDownRegionTestCase() throws Exception {
    DistributedTestCase.cleanupAllVms();
    CCRegion = null;
  }

  // @todo can be used in tests
//  protected CacheSerializableRunnable createRegionTask(final String name) {
//    return new CacheSerializableRunnable("Create Region") {
//      public void run2() throws CacheException {
//        assertNotNull(createRegion(name));
//      }
//    };
//  }


  ////////  Test Methods

  /**
   * This is a for the ConcurrentMap operations.
   * 4 VMs are used to
   * create the region and operations are performed on one of the nodes
   */
  public void testConcurrentOperations() throws Exception {
    SerializableRunnable createRegion = new CacheSerializableRunnable(
    "createRegion") {

      public void run2() throws CacheException {
        Cache cache = getCache();
        RegionAttributes regionAttribs = getRegionAttributes();
        cache.createRegion("R1",
            regionAttribs);
      }
    };

    Host host = Host.getHost(0);
    // create the VM(0 - 4)
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    vm0.invoke(createRegion);
    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    vm3.invoke(createRegion);
    concurrentMapTest("/R1");
  }

  /**
   * Do putIfAbsent(), replace(Object, Object),
   * replace(Object, Object, Object), remove(Object, Object) operations
   */
  public void concurrentMapTest(final String rName) {
    
    //String exceptionStr = "";
    VM vm0 = Host.getHost(0).getVM(0);
    vm0.invoke(new CacheSerializableRunnable("doConcurrentMapOperations") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        final Region pr = cache.getRegion(rName);
        assertNotNull(rName + " not created", pr);
               
        // test successful putIfAbsent
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object putResult = pr.putIfAbsent(Integer.toString(i),
                                            Integer.toString(i));
          assertNull("Expected null, but got " + putResult + " for key " + i,
                     putResult);
        }
        int size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the region", 
                    pr.isEmpty());
        
        // test unsuccessful putIfAbsent
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object putResult = pr.putIfAbsent(Integer.toString(i),
                                            Integer.toString(i + 1));
          assertEquals("for i=" + i, Integer.toString(i), putResult);
          assertEquals("for i=" + i, Integer.toString(i), pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the region", 
                    pr.isEmpty());
               
        // test successful replace(key, oldValue, newValue)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
         boolean replaceSucceeded = pr.replace(Integer.toString(i),
                                               Integer.toString(i),
                                               "replaced" + i);
          assertTrue("for i=" + i, replaceSucceeded);
          assertEquals("for i=" + i, "replaced" + i, pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the region", 
                   pr.isEmpty());
               
        // test unsuccessful replace(key, oldValue, newValue)
        for (int i = putRange_1Start; i <= putRange_2End; i++) {
         boolean replaceSucceeded = pr.replace(Integer.toString(i),
                                               Integer.toString(i), // wrong expected old value
                                               "not" + i);
         assertFalse("for i=" + i, replaceSucceeded);
         assertEquals("for i=" + i,
                      i <= putRange_1End ? "replaced" + i : null,
                      pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the region", 
                   pr.isEmpty());
                                    
        // test successful replace(key, value)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object replaceResult = pr.replace(Integer.toString(i),
                                            "twice replaced" + i);
          assertEquals("for i=" + i, "replaced" + i, replaceResult);
          assertEquals("for i=" + i,
                       "twice replaced" + i,
                       pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the region", 
                    pr.isEmpty());
                                    
        // test unsuccessful replace(key, value)
        for (int i = putRange_2Start; i <= putRange_2End; i++) {
          Object replaceResult = pr.replace(Integer.toString(i),
                                           "thrice replaced" + i);
          assertNull("for i=" + i, replaceResult);
          assertNull("for i=" + i, pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the region", 
                    pr.isEmpty());
                                    
        // test unsuccessful remove(key, value)
        for (int i = putRange_1Start; i <= putRange_2End; i++) {
          boolean removeResult = pr.remove(Integer.toString(i),
                                           Integer.toString(-i));
          assertFalse("for i=" + i, removeResult);
          assertEquals("for i=" + i,
                       i <= putRange_1End ? "twice replaced" + i : null,
                       pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the region", 
                    pr.isEmpty());

        // test successful remove(key, value)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          boolean removeResult = pr.remove(Integer.toString(i),
                                           "twice replaced" + i);
          assertTrue("for i=" + i, removeResult);
          assertEquals("for i=" + i, null, pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", 0, size);
        assertTrue("isEmpty doesnt return proper state of the region", 
                 pr.isEmpty());
        }
    });
    
    
    /*
     * destroy the Region.
     */
    vm0.invoke(new CacheSerializableRunnable("destroyRegionOp") {
               
       public void run2() {
         Cache cache = getCache();
         Region pr = cache.getRegion(rName);
         assertNotNull("Region already destroyed.", pr);
         pr.destroyRegion();
         assertTrue("Region isDestroyed false", pr.isDestroyed());
         assertNull("Region not destroyed.", cache.getRegion(rName));
       }
     });
  }

  /**
   * Tests that doing a {@link Region#put put} in a distributed region
   * one VM updates the value in another VM.
   */
  public void testDistributedUpdate() {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";

    SerializableRunnable put =
      new CacheSerializableRunnable("Put key/value") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            region.put(key, oldValue);
            flushIfNecessary(region);
          }
      };

    vm0.invoke(put);
    vm1.invoke(put);

    vm0.invoke(new CacheSerializableRunnable("Update") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.put(key, newValue);
          flushIfNecessary(region);
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Validate update") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          Region.Entry entry = region.getEntry(key);
          assertNotNull(entry);
          assertEquals(newValue, entry.getValue());
        }
      });
  }

  /**
   * Tests that distributed updates are delivered in order
   *
   * <P>
   *
   * Note that this test does not make sense for regions that are
   * {@link Scope#DISTRIBUTED_NO_ACK} for which we do not guarantee
   * the ordering of updates for a single producer/single consumer.
   *
   * DISABLED 4-16-04 - the current implementation assumes events
   * are processed synchronously, which is no longer true.
   */
  public void _ttestOrderedUpdates() throws Throwable {
    if (getRegionAttributes().getScope() ==
        Scope.DISTRIBUTED_NO_ACK) {
      return;
    }

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final int lastNumber = 10;

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region entry") {
          public void run2() throws CacheException {
            Region region = createRegion(name);
            region.create(key, null);
          }
        };

    vm0.invoke(create);
    vm1.invoke(create);

    vm1.invoke(new CacheSerializableRunnable("Set listener") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.setUserAttribute(new LinkedBlockingQueue());
          region.getAttributesMutator().addCacheListener(new
            CacheListenerAdapter() {
              public void afterUpdate(EntryEvent e) {
                Region region2 = e.getRegion();
                LinkedBlockingQueue queue =
                  (LinkedBlockingQueue) region2.getUserAttribute();
                Object value = e.getNewValue();
                assertNotNull(value);
                try {
                  com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("++ Adding " + value);
                  queue.put(value);

                } catch (InterruptedException ex) {
                  com.gemstone.gemfire.test.dunit.Assert.fail("Why was I interrupted?", ex);
                }
              }
            });
          flushIfNecessary(region);
        }
      });
    AsyncInvocation ai1 =
      vm1.invokeAsync(new CacheSerializableRunnable("Verify") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            LinkedBlockingQueue queue =
              (LinkedBlockingQueue) region.getUserAttribute();
            for (int i = 0; i <= lastNumber; i++) {
              try {
                com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("++ Waiting for " + i);
                Integer value = (Integer) queue.take();
                com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("++ Got " + value);
                assertEquals(i, value.intValue());

              } catch (InterruptedException ex) {
                com.gemstone.gemfire.test.dunit.Assert.fail("Why was I interrupted?", ex);
              }
            }
          }
        });

    AsyncInvocation ai0 =
      vm0.invokeAsync(new CacheSerializableRunnable("Populate") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            for (int i = 0; i <= lastNumber; i++) {
//              com.gemstone.gemfire.internal.GemFireVersion.waitForJavaDebugger(getLogWriter());
              region.put(key, new Integer(i));
            }
          }
        });

    ThreadUtils.join(ai0, 30 * 1000);
    ThreadUtils.join(ai1, 30 * 1000);

    if (ai0.exceptionOccurred()) {
      com.gemstone.gemfire.test.dunit.Assert.fail("ai0 failed", ai0.getException());

    } else if (ai1.exceptionOccurred()) {
      com.gemstone.gemfire.test.dunit.Assert.fail("ai1 failed", ai1.getException());
    }
  }

  /**
   * Tests that doing a distributed get results in a
   * <code>netSearch</code>.
   */
  public void testDistributedGet() {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(new CacheSerializableRunnable("Populate region") {
        public void run2() throws CacheException {
          Region region = createRegion(name);
          region.put(key, value);
        }
      });

    SerializableRunnable get = new CacheSerializableRunnable("Distributed get") {
        public void run2() throws CacheException {
          Region region = createRegion(name);
          assertEquals(value, region.get(key));
        }
    };


    vm1.invoke(get);
  }

  /**
   * Tests that doing a {@link Region#put put} on a distributed region
   * in one VM does not effect a region in a different VM that does
   * not have that key defined.
   */
  public void testDistributedPutNoUpdate()
    throws InterruptedException {

    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    Thread.sleep(250);

    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke(new CacheSerializableRunnable("Put key/value") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key, value);
        }
      });

    Thread.sleep(250);

    vm1.invoke(new CacheSerializableRunnable("Verify no update") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          Region.Entry entry = region.getEntry(key);
          if (getRegionAttributes().getDataPolicy().withReplication() ||
              getRegionAttributes().getPartitionAttributes() != null) {
            assertEquals(value, region.get(key));
          }
          else {
            assertNull(entry);
          }
        }
      });
  }

  /**
   * Two VMs create a region. One populates a region entry.  The other
   * VM defines that entry.  The first VM updates the entry.  The
   * second VM should see the updated value.
   */
  public void testDefinedEntryUpdated() {
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };

    vm0.invoke(create);
    vm1.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Create and populate") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key, oldValue);
        }
      });
    vm1.invoke(new CacheSerializableRunnable("Define entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          if (!getRegionAttributes().getDataPolicy().withReplication())
            region.create(key, null);
        }
      });
    vm0.invoke(new CacheSerializableRunnable("Update entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key, newValue);
        }
      });
    Invoke.invokeRepeatingIfNecessary(vm1, new CacheSerializableRunnable("Get entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          assertEquals(newValue, region.get(key));
        }
      }, getRepeatTimeoutMs());
  }


  /**
   * Tests that {@linkplain Region#destroy destroying} an entry is
   * propagated to all VMs that define that entry.
   */
  public void testDistributedDestroy() throws InterruptedException {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
//DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
            Region region = createRegion(name);
            assertTrue(!region.isDestroyed());
            Region root = region.getParentRegion();
            assertTrue(!root.isDestroyed());
          }
        };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm0.invoke(create);
    vm1.invoke(create);
    vm2.invoke(create);

    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable put =
      new CacheSerializableRunnable("Put key/value") {
          public void run2() throws CacheException {
//DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to put");
            Region region =
              getRootRegion().getSubregion(name);
            region.put(key, value);
            assertTrue(!region.isDestroyed());
            assertTrue(!region.getParentRegion().isDestroyed());
            flushIfNecessary(region);
          }
        };

    vm0.invoke(put);
    vm1.invoke(put);
    vm2.invoke(put);

    SerializableRunnable verifyPut =
      new CacheSerializableRunnable("Verify Put") {
          public void run2() throws CacheException {
            Region root = getRootRegion();
            assertTrue(!root.isDestroyed());
            Region region = root.getSubregion(name);
            assertTrue(!region.isDestroyed());
//DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to get");
            assertEquals(value, region.getEntry(key).getValue());
          }
        };

    vm0.invoke(verifyPut);
    vm1.invoke(verifyPut);
    vm2.invoke(verifyPut);

    vm0.invoke(new CacheSerializableRunnable("Destroy Entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.destroy(key);
          flushIfNecessary(region);
        }
      });

    CacheSerializableRunnable verifyDestroy =
      new CacheSerializableRunnable("Verify entry destruction") {
          public void run2() throws CacheException {
            Region root = getRootRegion();
            assertTrue(!root.isDestroyed());
            Region region = root.getSubregion(name);
            assertTrue(!region.isDestroyed());
            assertNull(region.getEntry(key));
          }
        };
    vm0.invoke(verifyDestroy);
    vm1.invoke(verifyDestroy);
    vm2.invoke(verifyDestroy);
  }

  /**
   * Tests that {@linkplain Region#destroy destroying} a region is
   * propagated to all VMs that define that region.
   */
  public void testDistributedRegionDestroy()
    throws InterruptedException {

    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };

    Invoke.invokeInEveryVM(create);

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    vm0.invoke(new CacheSerializableRunnable("Destroy Region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.destroyRegion();
          flushIfNecessary(region);
        }
      });

    Invoke.invokeInEveryVM(new CacheSerializableRunnable("Verify region destruction") {
      public void run2() throws CacheException {
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return getRootRegion().getSubregion(name) == null;
          }
          public String description() {
            return "Waiting for region " + name + " to be destroyed";
          }
        };
        Wait.waitForCriterion(ev, 60 * 1000, 10, true);
        Region region = getRootRegion().getSubregion(name);
        assertNull(region);
      }
    });
  }

  /**
   * Tests that a {@linkplain Region#localDestroy} does not effect
   * other VMs that define that entry.
   */
  public void testLocalDestroy() throws InterruptedException {
    if (!supportsLocalDestroyAndLocalInvalidate()) {
      return;
    }
    // test not valid for persistBackup region since they have to be
    // mirrored KEYS_VALUES
    if (getRegionAttributes().getDataPolicy().withPersistence()) return;

    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    Thread.sleep(250);

    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable put =
      new CacheSerializableRunnable("Put key/value") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            region.put(key, value);
          }
        };

    vm0.invoke(put);
    vm1.invoke(put);

    Thread.sleep(250);

    vm0.invoke(new CacheSerializableRunnable("Local Destroy Entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.localDestroy(key);
        }
      });

    Thread.sleep(250);

    SerializableRunnable verify =
      new CacheSerializableRunnable("Verify entry existence") {
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            assertNotNull(region.getEntry(key));
          }
        };
    vm1.invoke(verify);
  }

  /**
   * Tests that a {@link Region#localDestroyRegion} is not propagated
   * to other VMs that define that region.
   */
  public void testLocalRegionDestroy()
    throws InterruptedException {

    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    Thread.sleep(250);

    vm0.invoke(new CacheSerializableRunnable("Local Destroy Region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      });

    Thread.sleep(250);

    SerializableRunnable verify =
      new CacheSerializableRunnable("Verify region existence") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            assertNotNull(region);
          }
        };
    vm1.invoke(verify);
  }

  /**
   * Tests that {@linkplain Region#invalidate invalidating} an entry is
   * propagated to all VMs that define that entry.
   */
  public void testDistributedInvalidate() {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm0.invoke(create);
    vm1.invoke(create);

    // vm2 is on a different gemfire system
    vm2.invoke(create);

    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable put =
      new CacheSerializableRunnable("Put key/value") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            region.put(key, value);
            flushIfNecessary(region);
          }
        };

    vm0.invoke(put);
    vm1.invoke(put);
    vm2.invoke(put);

    vm0.invoke(new CacheSerializableRunnable("Invalidate Entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.invalidate(key);
          flushIfNecessary(region);
        }
      });

    CacheSerializableRunnable verify =
      new CacheSerializableRunnable("Verify entry invalidation") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            Region.Entry entry = region.getEntry(key);
            assertNotNull(entry);
            if (entry.getValue() != null) {
              // changed from severe to fine because it is possible
              // for this to return non-null on d-no-ack
              // that is was invokeRepeatingIfNecessary is called
              com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().fine("invalidated entry has value of " + entry.getValue());
            }
            assertNull(entry.getValue());
          }
        };


    vm1.invoke(verify);
    vm2.invoke(verify);
  }

  /**
   * Tests that {@linkplain Region#invalidate invalidating} an entry
   * in multiple VMs does not cause any problems.
   */
  public void testDistributedInvalidate4() throws InterruptedException {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };

    Host host = Host.getHost(0);
    int vmCount = host.getVMCount();
    for (int i = 0; i < vmCount; i++) {
      VM vm = host.getVM(i);
      vm.invoke(create);
    }

    SerializableRunnable put =
        new CacheSerializableRunnable("put entry") {
            public void run2() throws CacheException {
              Region region =
                  getRootRegion().getSubregion(name);
              region.put(key, value);
              flushIfNecessary(region);
            }
          };

      for (int i = 0; i < vmCount; i++) {
        VM vm = host.getVM(i);
        vm.invoke(put);
      }

      SerializableRunnable invalidate =
      new CacheSerializableRunnable("Invalidate Entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.invalidate(key);
          flushIfNecessary(region);
        }
      };

    for (int i = 0; i < vmCount; i++) {
      VM vm = host.getVM(i);
      vm.invoke(invalidate);
    }

    SerializableRunnable verify =
      new CacheSerializableRunnable("Verify entry invalidation") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            Region.Entry entry = region.getEntry(key);
            assertNotNull(entry);
            assertNull(entry.getValue());
          }
        };

    for (int i = 0; i < vmCount; i++) {
      VM vm = host.getVM(i);
      vm.invoke(verify);
    }
  }

  /**
   * Tests that {@linkplain Region#invalidateRegion invalidating} a
   * region is propagated to all VMs that define that entry.
   */
  public void testDistributedRegionInvalidate()
    throws InterruptedException {
    if (!supportsSubregions()) {
      return;
    }
    final String name = this.getUniqueName();
    final String subname = "sub";
    final boolean useSubs = getRegionAttributes().getPartitionAttributes() == null;

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            Region region;
            region = createRegion(name);
            if (useSubs) {
              region.createSubregion(subname, region.getAttributes());
            }
          }
        };

    Invoke.invokeInEveryVM(create);

    final Object key = "KEY";
    final Object value = "VALUE";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";

    SerializableRunnable put =
      new CacheSerializableRunnable("Put key/value") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            region.put(key, value);
            region.put(key2, value2);
            flushIfNecessary(region);

            if (useSubs) {
              Region subregion = region.getSubregion(subname);
              subregion.put(key, value);
              subregion.put(key2, value2);
              flushIfNecessary(subregion);
            }
          }
        };

    Invoke.invokeInEveryVM(put);

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    vm0.invoke(new CacheSerializableRunnable("Invalidate Region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.invalidateRegion();
        }
      });

    CacheSerializableRunnable verify =
      new CacheSerializableRunnable("Verify region invalidation") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            {
              Region.Entry entry = region.getEntry(key);
              assertNotNull(entry);
              Object v = entry.getValue();
              assertNull("Expected null but was " + v, v);

              entry = region.getEntry(key2);
              assertNotNull(entry);
              assertNull(entry.getValue());
            }

            if (useSubs) {
              Region subregion = region.getSubregion(subname);
              Region.Entry entry = subregion.getEntry(key);
              assertNotNull(entry);
              assertNull(entry.getValue());

              entry = subregion.getEntry(key2);
              assertNotNull(entry);
              assertNull(entry.getValue());
            }
          }
        };

    Invoke.invokeInEveryVMRepeatingIfNecessary(verify, getRepeatTimeoutMs());
  }

  /**
   * Tests that a {@link CacheListener} is invoked in a remote VM.
   */
  public void testRemoteCacheListener() throws InterruptedException {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";
//    final Object key2 = "KEY2";
//    final Object value2 = "VALUE2";

    SerializableRunnable populate =
      new CacheSerializableRunnable("Create Region and Put") {
          public void run2() throws CacheException {
            Region region = createRegion(name);
            region.put(key, oldValue);
          }
        };

    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);

    vm0.invoke(populate);
    vm1.invoke(populate);

    vm1.invoke(new CacheSerializableRunnable("Set listener") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          listener = new TestCacheListener() {
              public void afterUpdate2(EntryEvent event) {
                assertEquals(Operation.UPDATE, event.getOperation());
                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(event.getCallbackArgument(), event.getDistributedMember());
                assertEquals(key, event.getKey());
                assertEquals(oldValue, event.getOldValue());
                assertEquals(newValue, event.getNewValue());
                assertFalse(event.getOperation().isLoad());
                assertFalse(event.getOperation().isLocalLoad());
                assertFalse(event.getOperation().isNetLoad());
                assertFalse(event.getOperation().isNetSearch());
                if (event.getRegion().getAttributes().getOffHeap()) {
                  // since off heap always serializes the old value is serialized and available
                  assertEquals(oldValue, event.getSerializedOldValue().getDeserializedValue());
                } else {
                  assertEquals(null, event.getSerializedOldValue()); // since it was put originally in this VM
                }
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(event.getSerializedNewValue().getSerializedValue()));
                try {
                  assertEquals(newValue, DataSerializer.readObject(dis));
                } catch (Exception e) {
                  com.gemstone.gemfire.test.dunit.Assert.fail("Unexpected Exception", e);
                }
              }
            };
          region.getAttributesMutator().addCacheListener(listener);
        }
      });

    // I see no reason to pause here.
    // The test used to pause here but only if no-ack.
    // But we have no operations to wait for.
    // The last thing we did was install a listener in vm1
    // and it is possible that vm0 does not yet know we have
    // a listener but for this test it does not matter.
    // So I'm commenting out the following pause:
    //pauseIfNecessary();
    // If needed then do a flushIfNecessary(region) after adding the cache listener

    vm0.invoke(new CacheSerializableRunnable("Update") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key, newValue, getSystem().getDistributedMember());
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify Update") {
        public void run2() throws CacheException {
          listener.waitForInvocation(3000, 10);

          // Setup listener for next test
          final Region region =
            getRootRegion().getSubregion(name);
          listener = new TestCacheListener() {
              public void afterInvalidate2(EntryEvent event) {
                assertEquals(Operation.INVALIDATE, event.getOperation());
                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(event.getCallbackArgument(), event.getDistributedMember());
                assertEquals(key, event.getKey());
                assertEquals(newValue, event.getOldValue());
                assertNull(event.getNewValue());
                assertFalse(event.getOperation().isLoad());
                assertFalse(event.getOperation().isLocalLoad());
                assertFalse(event.getOperation().isNetLoad());
                assertFalse(event.getOperation().isNetSearch());
                assertNull(event.getSerializedNewValue());
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(event.getSerializedOldValue().getSerializedValue()));
                try {
                  assertEquals(newValue, DataSerializer.readObject(dis));
                } catch (Exception e) {
                  com.gemstone.gemfire.test.dunit.Assert.fail("Unexpected Exception", e);
                }
              }
            };
          region.getAttributesMutator().addCacheListener(listener);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Invalidate") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.invalidate(key, getSystem().getDistributedMember());
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify Invalidate") {
        public void run2() throws CacheException {
          listener.waitForInvocation(3000, 10);

          // Setup listener for next test
          final Region region =
            getRootRegion().getSubregion(name);
          listener = new TestCacheListener() {
              public void afterDestroy2(EntryEvent event) {
                assertTrue(event.getOperation().isDestroy());
                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(event.getCallbackArgument(), event.getDistributedMember());
                assertEquals(key, event.getKey());
                assertNull(event.getOldValue());
                assertNull(event.getNewValue());
                assertFalse(event.getOperation().isLoad());
                assertFalse(event.getOperation().isLocalLoad());
                assertFalse(event.getOperation().isNetLoad());
                assertFalse(event.getOperation().isNetSearch());
                assertNull(event.getSerializedOldValue());
                assertNull(event.getSerializedNewValue());
              }
            };
          region.getAttributesMutator().addCacheListener(listener);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Destroy") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.destroy(key, getSystem().getDistributedMember());
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify Destroy") {
        public void run2() throws CacheException {
          listener.waitForInvocation(3000, 10);

          // Setup listener for next test
          final Region region =
            getRootRegion().getSubregion(name);
          listener = new TestCacheListener() {
              public void afterRegionInvalidate2(RegionEvent event) {
                assertEquals(Operation.REGION_INVALIDATE, event.getOperation());
                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(event.getCallbackArgument(), event.getDistributedMember());
              }
            };
          region.getAttributesMutator().addCacheListener(listener);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Invalidate Region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.invalidateRegion(getSystem().getDistributedMember());
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify Invalidate Region") {
        public void run2() throws CacheException {
          listener.waitForInvocation(3000, 10);

          // Setup listener for next test
          final Region region =
            getRootRegion().getSubregion(name);
          listener = new TestCacheListener() {
              public void afterRegionDestroy2(RegionEvent event) {
                assertEquals(Operation.REGION_DESTROY, event.getOperation());
                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(event.getCallbackArgument(), event.getDistributedMember());
              }
            };
          region.getAttributesMutator().addCacheListener(listener);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Destroy Region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.destroyRegion(getSystem().getDistributedMember());
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify Destroy Region") {
        public void run2() throws CacheException {
          listener.waitForInvocation(3000, 10);
        }
      });
  }


  /**
   * Tests that a {@link CacheListener} is invoked in a remote VM.
   */
  public void testRemoteCacheListenerInSubregion() throws InterruptedException {
    if (!supportsSubregions()) {
      return;
    }
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };

    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);

    vm0.invoke(new CacheSerializableRunnable("Create Root") {
      public void run2() throws CacheException {
        createRootRegion();
      }
    });

    vm1.invoke(create);

    vm1.invoke(new CacheSerializableRunnable("Set listener") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          listener = new TestCacheListener() {
              public void afterRegionInvalidate2(RegionEvent event) {
                assertEquals(Operation.REGION_INVALIDATE, event.getOperation());
                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(event.getCallbackArgument(), event.getDistributedMember());
              }
            };
          region.getAttributesMutator().addCacheListener(listener);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Invalidate Root Region") {
        public void run2() throws CacheException {
          getRootRegion().invalidateRegion(getSystem().getDistributedMember());
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify Invalidate Region") {
        public void run2() throws CacheException {
          listener.waitForInvocation(3000, 10);

          // Setup listener for next test
          final Region region =
            getRootRegion().getSubregion(name);
          listener = new TestCacheListener() {
              public void afterRegionDestroy2(RegionEvent event) {
                assertEquals(Operation.REGION_DESTROY, event.getOperation());
                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(event.getCallbackArgument(), event.getDistributedMember());
              }
            };
          region.getAttributesMutator().addCacheListener(listener);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Destroy Root Region") {
        public void run2() throws CacheException {
          getRootRegion().destroyRegion(getSystem().getDistributedMember());
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify Destroy Region") {
        public void run2() throws CacheException {
          listener.waitForInvocation(3000, 10);
        }
      });
  }


  /**
   * Indicate whether this region supports netload
   * @return true if it supports netload
   */
  protected boolean supportsNetLoad() {
    return true;
  }
  
  /**
   * Tests that a {@link CacheLoader} is invoked in a remote VM.  This
   * essentially tests <code>netLoad</code>.
   */
  public void testRemoteCacheLoader() throws InterruptedException {
    if (!supportsNetLoad()) {
      return;
    }
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);


    vm1.invoke(new CacheSerializableRunnable("Set CacheLoader") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          loader = new TestCacheLoader() {
              public Object load2(LoaderHelper helper)
                throws CacheLoaderException {
                assertEquals(region, helper.getRegion());
                assertEquals(key, helper.getKey());
                assertNull(helper.getArgument());

                return value;
              }
            };
          region.getAttributesMutator().setCacheLoader(loader);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Remote load") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          assertEquals(value, region.get(key));
        }
      });

    vm1.invoke(new SerializableRunnable("Verify loader") {
        public void run() {
          assertTrue(loader.wasInvoked());
        }
      });
  }

  /**
   * Tests that the parameter passed to a remote {@link CacheLoader}
   * is actually passed.
   */
  public void testRemoteCacheLoaderArg() throws InterruptedException {
    if (!supportsNetLoad()) {
      return;
    }
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";
    final String arg = "ARG";

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
            // Can't test non-Serializable callback argument here
            // because netLoad will not occur because there are no
            // other members with the region defined when it is
            // created.  Hooray for intelligent messaging.
          }
        };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    vm1.invoke(new CacheSerializableRunnable("Set CacheLoader") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          loader = new TestCacheLoader() {
              public Object load2(LoaderHelper helper)
                throws CacheLoaderException {
                assertEquals(region, helper.getRegion());
                assertEquals(key, helper.getKey());
                assertEquals(arg, helper.getArgument());

                return value;
              }
            };
          region.getAttributesMutator().setCacheLoader(loader);
          flushIfNecessary(region);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Remote load") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);

          try {
            // Use a non-serializable arg object
            region.get(key, new Object() { });
            fail("Should have thrown an IllegalArgumentException");

          } catch (IllegalArgumentException ex) {
            // pass...
          }
          assertNull(region.getEntry(key));
          try {
           assertEquals(value, region.get(key, arg));
          }
          catch(IllegalArgumentException e) {}
       }
      });

    vm1.invoke(new SerializableRunnable("Verify loader") {
        public void run() {
          assertTrue(loader.wasInvoked());
        }
      });
  }

  /**
   * Tests that a remote {@link CacheLoader} that throws a {@link
   * CacheLoaderException} results is propagated back to the caller.
   */
  public void testRemoteCacheLoaderException() throws InterruptedException {
    if (!supportsNetLoad()) {
      return;
    }
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
//    final Object value = "VALUE";

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    vm1.invoke(new CacheSerializableRunnable("Set CacheLoader") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          loader = new TestCacheLoader() {
              public Object load2(LoaderHelper helper)
                throws CacheLoaderException {
                assertEquals(region, helper.getRegion());
                assertEquals(key, helper.getKey());
                assertNull(helper.getArgument());

                String s = "Test Exception";
                throw new CacheLoaderException(s);
              }
            };
          region.getAttributesMutator().setCacheLoader(loader);
          flushIfNecessary(region);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Remote load") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          try {
            region.get(key);
            fail("Should have thrown a CacheLoaderException");

          } catch (CacheLoaderException ex) {
            // pass...
          }
        }
      });

    vm1.invoke(new SerializableRunnable("Verify loader") {
        public void run() {
          assertTrue(loader.wasInvoked());
        }
      });
  }


  public void testCacheLoaderWithNetSearch() throws CacheException {
    if (!supportsNetLoad()) {
      return;
    }
    // some tests use mirroring by default (e.g. persistBackup regions)
    // if so, then this test won't work right
    if (getRegionAttributes().getDataPolicy().withReplication()
        || getRegionAttributes().getDataPolicy().isPreloaded()) {
      return;
    }

    final String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object value = new Integer(42);

    Host host = Host.getHost(0);
    // use vm on other gemfire system
    VM vm1 = host.getVM(1);
    vm1.invoke(new CacheSerializableRunnable("set remote value") {
      public void run2() throws CacheException {
//        final TestCacheLoader remoteloader = new TestCacheLoader() {
//            public Object load2(LoaderHelper helper)
//              throws CacheLoaderException {
//
//              assertEquals(key, helper.getKey());
//              assertEquals(name, helper.getRegion().getName());
//              return value;
//            }
//          };
//
//        AttributesFactory factory =
//          new AttributesFactory(getRegionAttributes());
//        factory.setCacheLoader(remoteloader);
        Region rgn = createRegion(name);
        rgn.put(key, value);
        flushIfNecessary(rgn);
      }
    });

    final TestCacheLoader loader1 = new TestCacheLoader() {
        public Object load2(LoaderHelper helper)
          throws CacheLoaderException {

          assertEquals(key, helper.getKey());
          assertEquals(name, helper.getRegion().getName());

          try {
            helper.getRegion().getAttributes();
            Object result = helper.netSearch(false);
            assertEquals(value, result);
            return result;
          } catch (TimeoutException ex) {
            com.gemstone.gemfire.test.dunit.Assert.fail("Why did I time out?", ex);
          }
          return null;
        }
      };

    AttributesFactory f = new AttributesFactory(getRegionAttributes());
    f.setCacheLoader(loader1);
    Region region =
      createRegion(name, f.create());

    loader1.wasInvoked();

    Region.Entry entry = region.getEntry(key);
    assertNull(entry);
    region.create(key, null);

    entry = region.getEntry(key);
    assertNotNull(entry);
    assertNull(entry.getValue());

    // make sure value is still there in vm1
    vm1.invoke(new CacheSerializableRunnable("verify remote value") {
      public void run2() throws CacheException {
        Region rgn = getRootRegion().getSubregion(name);
        assertEquals(value, rgn.getEntry(key).getValue());
      }
    });

//    com.gemstone.gemfire.internal.util.DebuggerSupport.waitForJavaDebugger(getLogWriter());
    assertEquals(value, region.get(key));
    // if global scope, then a netSearch is done BEFORE the loader is invoked,
    // so we get the value but the loader is never invoked.
    if (region.getAttributes().getScope().isGlobal()) {
      assertTrue(!loader1.wasInvoked());
    }
    else {
      assertTrue(loader1.wasInvoked());
    }
    assertEquals(value, region.getEntry(key).getValue());
  }


  public void testCacheLoaderWithNetLoad() throws CacheException {


    // replicated regions and partitioned regions make no sense for this
    // test
    if (getRegionAttributes().getDataPolicy().withReplication() ||
        getRegionAttributes().getDataPolicy().isPreloaded() ||
        getRegionAttributes().getPartitionAttributes() != null)
    {
      return;
    }

    final String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object value = new Integer(42);

    Host host = Host.getHost(0);
    // use vm on other gemfire system
    VM vm1 = host.getVM(1);
    vm1.invoke(new CacheSerializableRunnable("set up remote loader") {
      public void run2() throws CacheException {
        final TestCacheLoader remoteloader = new TestCacheLoader() {
            public Object load2(LoaderHelper helper)
              throws CacheLoaderException {

              assertEquals(key, helper.getKey());
              assertEquals(name, helper.getRegion().getName());
              return value;
            }
          };

        AttributesFactory factory =
          new AttributesFactory(getRegionAttributes());
        factory.setCacheLoader(remoteloader);
        createRegion(name, factory.create());
      }
    });

    final TestCacheLoader loader1 = new TestCacheLoader() {
        public Object load2(LoaderHelper helper)
          throws CacheLoaderException {

          assertEquals(key, helper.getKey());
          assertEquals(name, helper.getRegion().getName());

          try {
            helper.getRegion().getAttributes();
            Object result = helper.netSearch(true);
            assertEquals(value, result);
            return result;
          } catch (TimeoutException ex) {
            com.gemstone.gemfire.test.dunit.Assert.fail("Why did I time out?", ex);
          }
          return null;
        }
      };

    AttributesFactory f = new AttributesFactory(getRegionAttributes());
    f.setCacheLoader(loader1);
    Region region = createRegion(name, f.create());

    loader1.wasInvoked();

    Region.Entry entry = region.getEntry(key);
    assertNull(entry);

    region.create(key, null);

    entry = region.getEntry(key);
    assertNotNull(entry);
    assertNull(entry.getValue());

//    com.gemstone.gemfire.internal.util.DebuggerSupport.waitForJavaDebugger(getLogWriter());
    assertEquals(value, region.get(key));

    assertTrue(loader1.wasInvoked());
    assertEquals(value, region.getEntry(key).getValue());
  }


  /**
   * Tests that {@link Region#get} returns <code>null</code> when
   * there is no remote loader.
   */
  public void testNoRemoteCacheLoader() throws InterruptedException {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
//    final Object value = "VALUE";

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Remote load") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          assertNull(region.get(key));
        }
      });
  }

  /**
   * Tests that a remote <code>CacheLoader</code> is not invoked if
   * the remote region has an invalid entry (that is, a key, but no
   * value).
   */
  public void testNoLoaderWithInvalidEntry() {
    if (!supportsNetLoad()) {
      return;
    }
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            Region region = createRegion(name);
            loader = new TestCacheLoader() {
                public Object load2(LoaderHelper helper)
                  throws CacheLoaderException {

                  return value;
                }
              };
            region.getAttributesMutator().setCacheLoader(loader);
          }
        };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    vm1.invoke(new CacheSerializableRunnable("Create invalid entry") {
      public void run2() throws CacheException {
        Region region =
          getRootRegion().getSubregion(name);
          region.create(key, null);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Remote get") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
// DebuggerSupport.waitForJavaDebugger(getLogWriter());
          assertEquals(value, region.get(key));
          assertTrue(loader.wasInvoked());
        }
      });

    vm1.invoke(new SerializableRunnable("Verify loader") {
        public void run() {
          assertFalse(loader.wasInvoked());
        }
      });
  }

  /**
   * Tests that a remote {@link CacheWriter} is invoked and that
   * <code>CacheWriter</code> arguments and {@link
   * CacheWriterException}s are propagated appropriately.
   */
  public void testRemoteCacheWriter() throws InterruptedException {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";
    final Object arg = "ARG";
    final Object exception = "EXCEPTION";

    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            Region region = createRegion(name);

            // Put key2 in the region before any callbacks are
            // registered, so it can be destroyed later
            region.put(key2, value2);
            assertEquals(1, region.size());
            if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
              GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
              SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
              LocalRegion reRegion;
              reRegion = (LocalRegion) region;
              RegionEntry re = reRegion.getRegionEntry(key2);
              MemoryChunkWithRefCount mc = (MemoryChunkWithRefCount) re._getValue();
              assertEquals(1, mc.getRefCount());
              assertEquals(1, ma.getStats().getObjects());
            }
          }
        };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    ////////  Create

    vm1.invoke(new CacheSerializableRunnable("Set Writer") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          writer = new TestCacheWriter() {
              public void beforeCreate2(EntryEvent event)
                throws CacheWriterException {

                if (exception.equals(event.getCallbackArgument())) {
                  String s = "Test Exception";
                  throw new CacheWriterException(s);
                }

                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isCreate());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(key, event.getKey());
                assertEquals(null, event.getOldValue());
                assertEquals(oldValue, event.getNewValue());
                assertFalse(event.getOperation().isLoad());
                assertFalse(event.getOperation().isLocalLoad());
                assertFalse(event.getOperation().isNetLoad());
                assertFalse(event.getOperation().isNetSearch());

              }
            };
          region.getAttributesMutator().setCacheWriter(writer);
          flushIfNecessary(region);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Create with Exception") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          try {
            region.put(key, oldValue, exception);
            fail("Should have thrown a CacheWriterException");

          } catch (CacheWriterException ex) {
            assertNull(region.getEntry(key));
            assertEquals(1, region.size());
            if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
              GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
              SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
              assertEquals(1, ma.getStats().getObjects());
            }
          }
        }
      });

    vm1.invoke(new SerializableRunnable("Verify callback") {
        public void run() {
          assertTrue(writer.wasInvoked());
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Create with Argument") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key, oldValue, arg);
          assertEquals(2, region.size());
          if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
            GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
            SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
            assertEquals(2, ma.getStats().getObjects());
            LocalRegion reRegion;
            reRegion = (LocalRegion) region;
            MemoryChunkWithRefCount mc = (MemoryChunkWithRefCount) reRegion.getRegionEntry(key)._getValue();
            assertEquals(1, mc.getRefCount());
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify callback") {
        public void run() {
          assertTrue(writer.wasInvoked());
        }
      });

    ////////  Update

    vm1.invoke(new CacheSerializableRunnable("Set Writer") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          writer = new TestCacheWriter() {
              public void beforeUpdate2(EntryEvent event)
                throws CacheWriterException {

                Object argument = event.getCallbackArgument();
                if (exception.equals(argument)) {
                  String s = "Test Exception";
                  throw new CacheWriterException(s);
                }

                assertEquals(arg, argument);

                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isUpdate());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(key, event.getKey());
                assertEquals(oldValue, event.getOldValue());
                assertEquals(newValue, event.getNewValue());
                assertFalse(event.getOperation().isLoad());
                assertFalse(event.getOperation().isLocalLoad());
                assertFalse(event.getOperation().isNetLoad());
                assertFalse(event.getOperation().isNetSearch());

              }
            };
          region.getAttributesMutator().setCacheWriter(writer);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Update with Exception") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          try {
            region.put(key, newValue, exception);
            fail("Should have thrown a CacheWriterException");

          } catch (CacheWriterException ex) {
            Region.Entry entry = region.getEntry(key);
            assertEquals(oldValue, entry.getValue());
            assertEquals(2, region.size());
            if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
              GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
              SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
              assertEquals(2, ma.getStats().getObjects());
              LocalRegion reRegion;
              reRegion = (LocalRegion) region;
              MemoryChunkWithRefCount mc = (MemoryChunkWithRefCount) reRegion.getRegionEntry(key)._getValue();
              assertEquals(1, mc.getRefCount());
            }
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify callback") {
        public void run() {
          assertTrue(writer.wasInvoked());
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Update with Argument") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key, newValue, arg);
          assertEquals(2, region.size());
          if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
            GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
            SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
            assertEquals(2, ma.getStats().getObjects());
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify callback") {
        public void run() {
          assertTrue(writer.wasInvoked());
        }
      });

    ////////  Destroy

    vm1.invoke(new CacheSerializableRunnable("Set Writer") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          writer = new TestCacheWriter() {
              public void beforeDestroy2(EntryEvent event)
                throws CacheWriterException {

                Object argument = event.getCallbackArgument();
                if (exception.equals(argument)) {
                  String s = "Test Exception";
                  throw new CacheWriterException(s);
                }

                assertEquals(arg, argument);

                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isDestroy());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
                assertEquals(key, event.getKey());
                assertEquals(newValue, event.getOldValue());
                assertNull(event.getNewValue());
                assertFalse(event.getOperation().isLoad());
                assertFalse(event.getOperation().isLocalLoad());
                assertFalse(event.getOperation().isNetLoad());
                assertFalse(event.getOperation().isNetSearch());
              }
            };
          region.getAttributesMutator().setCacheWriter(writer);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Destroy with Exception") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          try {
            region.destroy(key, exception);
            fail("Should have thrown a CacheWriterException");

          } catch (CacheWriterException ex) {
            assertNotNull(region.getEntry(key));
            assertEquals(2, region.size());
            if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
              GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
              SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
              assertEquals(2, ma.getStats().getObjects());
            }
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify callback") {
        public void run() {
          assertTrue(writer.wasInvoked());
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Destroy with Argument") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.destroy(key, arg);
          assertEquals(1, region.size());
          if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
            GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
            SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
            assertEquals(1, ma.getStats().getObjects());
          }
       }
      });
    vm1.invoke(new SerializableRunnable("Verify callback") {
        public void run() {
          assertTrue(writer.wasInvoked());
        }
      });

    ////////  Region Destroy

    vm1.invoke(new CacheSerializableRunnable("Set Writer") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          writer = new TestCacheWriter() {
              public void beforeRegionDestroy2(RegionEvent event)
                throws CacheWriterException {

                Object argument = event.getCallbackArgument();
                if (exception.equals(argument)) {
                  String s = "Test Exception";
                  throw new CacheWriterException(s);
                }

                assertEquals(arg, argument);

                assertEquals(region, event.getRegion());
                assertTrue(event.getOperation().isRegionDestroy());
                assertTrue(event.getOperation().isDistributed());
                assertFalse(event.getOperation().isExpiration());
                assertTrue(event.isOriginRemote());
              }
            };
          region.getAttributesMutator().setCacheWriter(writer);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Destroy with Exception") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          try {
            region.destroyRegion(exception);
            fail("Should have thrown a CacheWriterException");

          } catch (CacheWriterException ex) {
            if (region.isDestroyed()) {
              com.gemstone.gemfire.test.dunit.Assert.fail("should not have an exception if region is destroyed", ex);
            }
            assertEquals(1, region.size());
            if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
              GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
              SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
              assertEquals(1, ma.getStats().getObjects());
            }
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify callback") {
        public void run() {
          assertTrue(writer.wasInvoked());
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Destroy with Argument") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          assertEquals(1, region.size());
          if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
            GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
            SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
            assertEquals(1, ma.getStats().getObjects());
          }
          region.destroyRegion(arg);
          if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
            GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
            final SimpleMemoryAllocatorImpl ma = (SimpleMemoryAllocatorImpl) gfc.getOffHeapStore();
            WaitCriterion waitForStatChange = new WaitCriterion() {
              public boolean done() {
                return ma.getStats().getObjects() == 0;
              }
              public String description() {
                return "never saw off-heap object count go to zero. Last value was " + ma.getStats().getObjects();
              }
            };
            Wait.waitForCriterion(waitForStatChange, 3000, 10, true);
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify callback") {
        public void run() {
          assertTrue(writer.wasInvoked());
        }
      });
  }

  /**
   * Tests that, when given a choice, a local <code>CacheWriter</code>
   * is invoked instead of a remote one.
   */
  public void testLocalAndRemoteCacheWriters()
    throws InterruptedException {

    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(new CacheSerializableRunnable("Create \"Local\" Region") {
        public void run2() throws CacheException {
          Region region = createRegion(name);
          writer = new TestCacheWriter() {
              public void beforeUpdate2(EntryEvent event)
                throws CacheWriterException { }

              public void beforeCreate2(EntryEvent event)
                throws CacheWriterException { }

              public void beforeDestroy2(EntryEvent event)
                throws CacheWriterException { }

              public void beforeRegionDestroy2(RegionEvent event)
                throws CacheWriterException { }
          };
          region.getAttributesMutator().setCacheWriter(writer);
        }
      });

   SerializableRunnable create = new CacheSerializableRunnable("Create \"Local\" Region") {
      public void run2() throws CacheException {
        Region region = createRegion(name);
        writer = new TestCacheWriter() { };
        region.getAttributesMutator().setCacheWriter(writer);
      }
   };

   vm1.invoke(create);

    SerializableRunnable verify = new
      SerializableRunnable("Verify no callback") {
        public void run() {
          assertFalse(writer.wasInvoked());
        }
      };

    vm0.invoke(new CacheSerializableRunnable("Create entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key, oldValue);
          assertTrue(writer.wasInvoked());
        }
      });
    vm1.invoke(verify);

    vm0.invoke(new CacheSerializableRunnable("Update entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key, newValue);
          assertTrue(writer.wasInvoked());
        }
      });
    vm1.invoke(verify);

    vm0.invoke(new CacheSerializableRunnable("Destroy entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.destroy(key);
          assertTrue(writer.wasInvoked());
        }
      });
    vm1.invoke(verify);

    vm0.invoke(new CacheSerializableRunnable("Destroy region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.destroyRegion();
          assertTrue(writer.wasInvoked());
        }
      });
    vm1.invoke(verify);
  }

  /**
   * Tests that when a <code>CacheLoader</code> modifies the callback
   * argument in place, the change is visible to the
   * <code>CacheWriter</code> even if it is in another VM.
   */
  public void testCacheLoaderModifyingArgument()
    throws InterruptedException {

    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";
    final Object one = "ONE";
    final Object two = "TWO";

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    CacheSerializableRunnable setLoader = new CacheSerializableRunnable("Set CacheLoader") {
      public void run2() throws CacheException {
        final Region region =
          getRootRegion().getSubregion(name);
        loader = new TestCacheLoader() {
            public Object load2(LoaderHelper helper)
              throws CacheLoaderException {

              Object[] array = (Object[]) helper.getArgument();
              assertEquals(one, array[0]);
              array[0] = two;
              return value;
            }
          };
        region.getAttributesMutator().setCacheLoader(loader);
        flushIfNecessary(region);
      }
    };

    vm0.invoke(setLoader);

    // if  this is a partitioned region, we need the loader in both vms
    vm1.invoke(new CacheSerializableRunnable("Conditionally create second loader") {
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(name);
        if (region.getAttributes().getPartitionAttributes() != null) {
          loader = new TestCacheLoader() {
            public Object load2(LoaderHelper helper)
              throws CacheLoaderException {

              Object[] array = (Object[]) helper.getArgument();
              assertEquals(one, array[0]);
              array[0] = two;
              return value;
            }
          };
          region.getAttributesMutator().setCacheLoader(loader);
        }
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Set CacheWriter") {
        public void run2() throws CacheException {
          final Region region = getRootRegion().getSubregion(name);
          writer = new TestCacheWriter() {
              public void beforeCreate2(EntryEvent event)
                throws CacheWriterException {

                Object[] array = (Object[]) event.getCallbackArgument();
                assertEquals(two, array[0]);
              }
            };
          region.getAttributesMutator().setCacheWriter(writer);
          flushIfNecessary(region);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Create entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          Object[] array = { one };
          Object result = region.get(key, array);
          assertTrue(loader.wasInvoked());
          assertEquals(value, result);
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Validate callback") {
        public void run2() throws CacheException {
//          if (getRootRegion().getSubregion(name).getAttributes()
//              .getPartitionAttributes() == null) { // bug 36500 - remove check when fixed
            assertTrue(writer.wasInvoked());
//          }
        }
      });
  }


  /**
   * Tests that invoking <code>netSearch</code> in a remote loader
   * returns <code>null</code> instead of causing infinite recursion.
   */
  public void testRemoteLoaderNetSearch() throws CacheException {
    if (!supportsNetLoad()) {
      return;
    }
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable create = new CacheSerializableRunnable("Get value") {
        public void run2() throws CacheException {
          Region region = createRegion(name);
          assertEquals(value, region.get(key));
        }
      };


    vm0.invoke(new CacheSerializableRunnable("Create Region") {
        public void run2() throws CacheException {
          Region region = createRegion(name);
          region.getAttributesMutator().setCacheLoader(new
            TestCacheLoader() {
              public Object load2(LoaderHelper helper)
                throws CacheLoaderException {

                try {
                  assertNull(helper.netSearch(true));

                } catch (TimeoutException ex) {
                  com.gemstone.gemfire.test.dunit.Assert.fail("Why did I time out?", ex);
                }
                return value;
              }
            });
        }
      });

    vm1.invoke(create);
  }

  /**
   * Tests that a local loader is preferred to a remote one
   */
  public void testLocalCacheLoader() {
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable create = new CacheSerializableRunnable("Create \"remote\" region") {
        public void run2() throws CacheException {
          Region region = createRegion(name);
          loader = new TestCacheLoader() {
              public Object load2(LoaderHelper helper)
                throws CacheLoaderException {
                if (helper.getRegion().getAttributes().getPartitionAttributes() == null) {
                  fail("Should not be invoked");
                  return null;
                }
                else {
                  return value;
                }
              }
            };
          region.getAttributesMutator().setCacheLoader(loader);
        }
    };


    vm0.invoke(new CacheSerializableRunnable("Create \"local\" region") {
        public void run2() throws CacheException {
          Region region = createRegion(name);
          region.getAttributesMutator().setCacheLoader(new
            TestCacheLoader() {
              public Object load2(LoaderHelper helper)
                throws CacheLoaderException {
                return value;
              }
            });
        }
      });

    vm1.invoke(create);


    vm0.invoke(new CacheSerializableRunnable("Get") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          assertEquals(value, region.get(key));
        }
      });
    vm1.invoke(new SerializableRunnable("Verify loader not invoked") {
        public void run() {
          assertFalse(loader.wasInvoked());
        }
      });
  }

  /**
   * Tests that an entry update is propagated to other caches that
   * have that same entry defined.
   */
  public void testDistributedPut() throws Exception {
    final String rgnName = getUniqueName();

    SerializableRunnable create = new SerializableRunnable("testDistributedPut: Create Region") {
      public void run() {
        try {
          createRegion(rgnName);
          getSystem().getLogWriter().info("testDistributedPut: Created Region");
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };

    SerializableRunnable newKey = new SerializableRunnable("testDistributedPut: Create Key") {
      public void run() {
        try {
          if (!getRegionAttributes().getDataPolicy().withReplication() &&
              getRegionAttributes().getPartitionAttributes() == null) {
            Region root = getRootRegion("root");
            Region rgn = root.getSubregion(rgnName);
            rgn.create("key", null);
            getSystem().getLogWriter().info("testDistributedPut: Created Key");
         }
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    vm0.invoke(create);
    vm0.invoke(newKey);
    int vmCount = host.getVMCount();
    Set systems = new HashSet();
    for (int i = 1; i < vmCount; i++) {
      VM vm = host.getVM(i);
      vm.invoke(create);
      if (!getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm.invoke(newKey);
      }
    }
//GemFireVersion.waitForJavaDebugger(getLogWriter(), "CTRLR WAITING AFTER CREATE");


    try {
      Region rgn = null;
      rgn = createRegion(rgnName);

      rgn.put("key", "value");
      getSystem().getLogWriter().info("testDistributedPut: Put Value");

      Invoke.invokeInEveryVMRepeatingIfNecessary(new CacheSerializableRunnable("testDistributedPut: Verify Received Value") {
        public void run2() {
          Region rgn1 = getRootRegion().getSubregion(rgnName);
          assertNotNull("Could not find entry for 'key'", rgn1.getEntry("key"));
          assertEquals("value", rgn1.getEntry("key").getValue());
        }
      }, getRepeatTimeoutMs());

    }
    catch(Exception e) {
      CacheFactory.getInstance(getSystem()).close();
      getSystem().getLogWriter().fine("testDistributedPut: Caused exception in createRegion");
      throw e;
    }

  }


  ////////  Mirroring Tests

  /**
   * Tests that keys, but not values, are pushed with {@link
   * MirrorType#KEYS}.
   */
//   public void testMirroredKeys() throws InterruptedException {
//     // Note that updates will now always include the value.

//     final String name = this.getUniqueName();
//     final Object key1 = "KEY1";
//     final Object value1 = "VALUE1";
//     final Object key2 = "KEY2";

//     Object[] v = new Object[3000];
//     Arrays.fill(v, new Integer(0xCAFE));

//     final Object value2 = Arrays.asList(v);
//     final Object key3 = "KEY3";
//     final Object value3 = "VALUE3";

//     Host host = Host.getHost(0);
//     VM vm0 = host.getVM(0);
//     VM vm2 = host.getVM(2); // use VM on separate shared memory in case they are shared regions

//     final boolean persistBackup = getRegionAttributes().getDataPolicy().isPersistentReplicate();

//     SerializableRunnable create = new
//       CacheSerializableRunnable("Create Mirrored Region") {
//         public void run2() throws CacheException {
//           AttributesFactory factory =
//             new AttributesFactory(getRegionAttributes());
//           factory.setMirrorType(MirrorType.KEYS);
//           try {
//             createRegion(name, factory.create());
//             if (persistBackup) fail("Should have thrown an IllegalStateException");
//           }
//           catch (IllegalStateException e) {
//             if (!persistBackup) throw e;
//           }
//         }
//       };

//     vm0.invoke(create);
//     if (persistBackup) return;

//     vm2.invoke(create);

//     vm0.invoke(new CacheSerializableRunnable("Put data") {
//         public void run2() throws CacheException {
//           Region region =
//             getRootRegion().getSubregion(name);
//           region.put(key1, value1);
//           region.put(key2, value2);
//           region.put(key3, value3);
//         }
//       });

//     invokeRepeatingIfNecessary(vm2, new CacheSerializableRunnable("Wait for update") {
//       public void run2() throws CacheException {
//         Region region = getRootRegion().getSubregion(name);
//         assertNotNull(region.getEntry(key1));
//         assertNotNull(region.getEntry(key2));
//         assertNotNull(region.getEntry(key3));
//       }
//     });

//     // Destroy the local entries so we know that they are not found by
//     // a netSearch
//     vm0.invoke(new CacheSerializableRunnable("Remove local entries") {
//         public void run2() throws CacheException {
//           Region region =
//             getRootRegion().getSubregion(name);
//           region.localDestroyRegion();
//         }
//       });

//     invokeRepeatingIfNecessary(vm2, new CacheSerializableRunnable("Verify keys") {
//         public void run2() throws CacheException {
//           Region region =
//             getRootRegion().getSubregion(name);

//           // values1 and values3 should have been propagated since
//           // they were small
//           // value2, which is large, is also propagated since we no
//           // longer optimize updates based on size.
//           Region.Entry entry1 = region.getEntry(key1);
//           assertNotNull(entry1);
//           assertEquals(value1, entry1.getValue());

//           Region.Entry entry2 = region.getEntry(key2);
//           assertNotNull(entry2);
//           assertEquals(value2, entry2.getValue());

//           Region.Entry entry3 = region.getEntry(key3);
//           assertNotNull(entry3);
//           assertEquals(value3, entry3.getValue());
//         }
//       });
//   }

  /**
   * Indicate whether replication/GII supported
   * @return true if replication is supported
   */
  protected boolean supportsReplication() {
    return true;
  }
  
  /**
   * Tests that keys and values are pushed with {@link
   * DataPolicy#REPLICATE}.
   */
  public void testReplicate() throws InterruptedException {
    if (!supportsReplication()) {
      return;
    }
    //pauseIfNecessary(100); // wait for previous tearDown to complete

    final String name = this.getUniqueName();
    final Object key1 = "KEY1";
    final Object value1 = "VALUE1";
    final Object key2 = "KEY2";

    Object[] v = new Object[3000];
    Arrays.fill(v, new Integer(0xCAFE));

    final Object value2 = Arrays.asList(v);
    final Object key3 = "KEY3";
    final Object value3 = "VALUE3";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2); // use VM on separate shared memory in case shared regions

    SerializableRunnable create = new
      CacheSerializableRunnable("Create Mirrored Region") {
        public void run2() throws CacheException {
          RegionAttributes ra = getRegionAttributes();
          AttributesFactory factory =
            new AttributesFactory(ra);
          if (ra.getEvictionAttributes() == null
              || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
            factory.setDiskStoreName(null);
          }
          factory.setDataPolicy(DataPolicy.REPLICATE);
          createRegion(name, factory.create());
        }
      };

    vm0.invoke(create);
    vm2.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Put data") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key1, value1);
          region.put(key2, value2);
          region.put(key3, value3);
          flushIfNecessary(region);
          }
      });

    Invoke.invokeRepeatingIfNecessary(vm2, new CacheSerializableRunnable("Wait for update") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        assertNotNull(region.getEntry(key1));
        assertNotNull(region.getEntry(key2));
        assertNotNull(region.getEntry(key3));
      }
    }, getRepeatTimeoutMs());

    // Destroy the local entries so we know that they are not found by
    // a netSearch
    vm0.invoke(new CacheSerializableRunnable("Remove local entries") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.localDestroyRegion();
          flushIfNecessary(region);
        }
      });

    Invoke.invokeRepeatingIfNecessary(vm2, new CacheSerializableRunnable("Verify keys") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);

          // small
          Region.Entry entry1 = region.getEntry(key1);
          assertNotNull(entry1);
          assertEquals(value1, entry1.getValue());

          // large
          Region.Entry entry2 = region.getEntry(key2);
          assertNotNull(entry2);
          assertEquals(value2, entry2.getValue());

          // small
          Region.Entry entry3 = region.getEntry(key3);
          assertNotNull(entry3);
          assertEquals(value3, entry3.getValue());
        }
      }, getRepeatTimeoutMs());
  }
  
  /**
   * Delta implementation for the delta tests, appends " 10" if it's a string,
   * or adds 10 if it's an Integer
   */
  static class AddTen implements Delta, Serializable {

    public Object apply(EntryEvent<?, ?> putEvent) {
      Object oldValue = putEvent.getOldValue();
      if (oldValue instanceof String) {
        return (String)oldValue + " 10";
      }
      else if (oldValue instanceof Integer) {
        return new Integer(((Integer)oldValue).intValue() + 10);
      }
      else throw new IllegalStateException("unexpected old value");
    }

    public Object merge(Object toMerge, boolean isCreate) {
      return null;
    }

    public Object merge(Object toMerge) {
      return null;
    }

    public Object getResultantValue() {
      return null;
    }
  }

  /**
   * Tests that a Delta is applied correctly both locally and on a replicate
   * region.
   */
  public void testDeltaWithReplicate() throws InterruptedException {
    if (!supportsReplication()) {
      return;
    }
    //pauseIfNecessary(100); // wait for previous tearDown to complete
    
    final String name = this.getUniqueName();
    final Object key1 = "KEY1";
    final Object value1 = "VALUE1";
    final Object key2 = "KEY2";
    final Object value2 = new Integer (0xCAFE);
    final Object key3 = "KEY3";
    final Object value3 = "VALUE3";
    
    final Delta delta = new AddTen();

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2); // use VM on separate shared memory in case shared regions
    
    SerializableRunnable create = new
    CacheSerializableRunnable("Create Replicate Region") {
      public void run2() throws CacheException {
        RegionAttributes ra = getRegionAttributes();
        AttributesFactory factory =
          new AttributesFactory(ra);
        if (ra.getEvictionAttributes() == null
            || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
          factory.setDiskStoreName(null);
        }
        factory.setDataPolicy(DataPolicy.REPLICATE);
        createRegion(name, factory.create());
      }
    };
    
    vm0.invoke(create);
    Thread.sleep(250);
    vm2.invoke(create);
    Thread.sleep(250);
    
    vm0.invoke(new CacheSerializableRunnable("Put data") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.put(key1, value1);
        region.put(key2, value2);
        region.put(key3, value3);
      }
    });
    
    Invoke.invokeRepeatingIfNecessary(vm2, new CacheSerializableRunnable("Wait for update") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        assertNotNull(region.getEntry(key1));
        assertNotNull(region.getEntry(key2));
        assertNotNull(region.getEntry(key3));
      }
    }, getRepeatTimeoutMs());
    
    // apply delta
    vm0.invoke(new CacheSerializableRunnable("Apply delta") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.put(key1, delta);
        region.put(key2, delta);
        region.put(key3, delta);
      }
    });
    
    CacheSerializableRunnable verify = 
      new CacheSerializableRunnable("Verify values") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          
          Region.Entry entry1 = region.getEntry(key1);
          assertNotNull(entry1);
          assertEquals("VALUE1 10", entry1.getValue());
          
          Region.Entry entry2 = region.getEntry(key2);
          assertNotNull(entry2);
          assertEquals(new Integer(0xCAFE + 10), entry2.getValue());
          
          Region.Entry entry3 = region.getEntry(key3);
          assertNotNull(entry3);
          assertEquals("VALUE3 10", entry3.getValue());
        }
      };
    
    Invoke.invokeRepeatingIfNecessary(vm0, verify, getRepeatTimeoutMs());
    
    
    // Destroy the local entries so we know that they are not found by
    // a netSearch
    vm0.invoke(new CacheSerializableRunnable("Remove local entries") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    });
    
    Invoke.invokeRepeatingIfNecessary(vm2, verify, getRepeatTimeoutMs());
    
  }
  
  
  
  /**
   * Tests that a newly-created mirrored region contains all of the
   * entries of another region.
   */
  public void testGetInitialImage() {
    if (!supportsReplication()) {
      return;
    }
    final String name = this.getUniqueName();
    final Object key1 = "KEY1";
    final Object value1 = "VALUE1";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";
    final Object key3 = "KEY3";
    final Object value3 = "VALUE3";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2); // use VM on different shared memory area

    SerializableRunnable create = new
      CacheSerializableRunnable("Create Mirrored Region") {
        public void run2() throws CacheException {
          RegionAttributes ra = getRegionAttributes();
          AttributesFactory factory =
            new AttributesFactory(ra);
          if (ra.getEvictionAttributes() == null
              || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
            factory.setDiskStoreName(null);
          }
          factory.setDataPolicy(DataPolicy.REPLICATE);
          createRegion(name, factory.create());
        }
      };


    vm0.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Put data") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key1, value1);
          region.put(key2, value2);
          region.put(key3, value3);
        }
      });

    vm2.invoke(create);

    // Destroy the local entries so we know that they are not found by
    // a netSearch
    vm0.invoke(new CacheSerializableRunnable("Remove local entries") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Verify keys/values") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);

          Region.Entry entry1 = region.getEntry(key1);
          assertNotNull(entry1);
          assertEquals(value1, entry1.getValue());

          Region.Entry entry2 = region.getEntry(key2);
          assertNotNull(entry2);
          assertEquals(value2, entry2.getValue());

          Region.Entry entry3 = region.getEntry(key3);
          assertNotNull(entry3);
          assertEquals(value3, entry3.getValue());
        }
      });
  }


  private static final int CHUNK_SIZE = 500 * 1024; // == InitialImageOperation.CHUNK_SIZE_IN_BYTES
  private static final int NUM_ENTRIES = 100;
  private static final int VALUE_SIZE = CHUNK_SIZE * 10 / NUM_ENTRIES;
  /**
   * Tests that a newly-created mirrored region contains all of the
   * entries of another region, with a large quantity of data.
   */
  public void testLargeGetInitialImage() {
    if (!supportsReplication()) {
      return;
    }
    final String name = this.getUniqueName();
    final Integer[] keys = new Integer[NUM_ENTRIES];
    final byte[][] values = new byte[NUM_ENTRIES][];

    for (int i = 0; i < NUM_ENTRIES; i++) {
      keys[i] = new Integer(i);
      values[i] = new byte[VALUE_SIZE];
      Arrays.fill(values[i], (byte)0x42);
    }

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2); // use VM on different shared memory area

    SerializableRunnable create = new
      CacheSerializableRunnable("Create Mirrored Region") {
        public void run2() throws CacheException {
          RegionAttributes ra = getRegionAttributes();
          AttributesFactory factory =
            new AttributesFactory(ra);
          if (ra.getEvictionAttributes() == null
              || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
            factory.setDiskStoreName(null);
          }
          factory.setDataPolicy(DataPolicy.REPLICATE);
          createRegion(name, factory.create());
        }
      };


    vm0.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Put data") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          for (int i = 0; i < NUM_ENTRIES; i++) {
            region.put(keys[i], values[i]);
          }
        }
      });

    vm2.invoke(create);

    // Destroy the local entries so we know that they are not found by
    // a netSearch
    vm0.invoke(new CacheSerializableRunnable("Remove local entries") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Verify keys/values") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          assertEquals(NUM_ENTRIES, region.entrySet(false).size());
          for (int i = 0; i < NUM_ENTRIES; i++) {
            Region.Entry entry = region.getEntry(keys[i]);
            assertNotNull(entry);
            if (!(entry.getValue() instanceof byte[])) {
              fail("getValue returned a " + entry.getValue().getClass() + " instead of the expected byte[]");
            }
            assertTrue(Arrays.equals(values[i], (byte[])entry.getValue()));
          }
        }
      });
  }

  /**
   * Tests that a mirrored region gets data pushed to it from a
   * non-mirrored region and the afterCreate event is invoked on a listener.
   */
  public void testMirroredDataFromNonMirrored()
    throws InterruptedException {
    if (!supportsReplication()) {
      return;
    }

    final String name = this.getUniqueName();
    final Object key1 = "KEY1";
    final Object value1 = "VALUE1";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";
    final Object key3 = "KEY3";
    final Object value3 = "VALUE3";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2); // use a VM on a different gemfire system

    SerializableRunnable create = new CacheSerializableRunnable("Populate non-mirrored region") {
      public void run2() throws CacheException {
        RegionAttributes ra = getRegionAttributes();
        AttributesFactory fac =
          new AttributesFactory(ra);
        if (ra.getEvictionAttributes() == null
            || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
          fac.setDiskStoreName(null);
        }
        fac.setDataPolicy(DataPolicy.NORMAL);
        //fac.setPersistBackup(false);
        Region region = createRegion(name, fac.create());
        region.put(key1, value1);
        region.put(key2, value2);
        region.put(key3, value3);
        flushIfNecessary(region);
      }
    };

    class MirroredDataFromNonMirroredListener extends TestCacheListener {
      // use modifiable ArrayLists
      List expectedKeys = new ArrayList(Arrays.asList(new Object[] {
        key1, key2, key3}));
      List expectedValues = new ArrayList(Arrays.asList(new Object[] {
        value1, value2, value3}));

      public synchronized void afterCreate2(EntryEvent event) {
        //getLogWriter().info("Invoking afterCreate2 with key=" + event.getKey());
        int index = expectedKeys.indexOf(event.getKey());
        assertTrue(index >= 0);
        assertEquals(expectedValues.remove(index), event.getNewValue());
        expectedKeys.remove(index);
        com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("afterCreate called in " +
         "MirroredDataFromNonMirroredListener for key:" + event.getKey());
      }
    }

    vm0.invoke(new CacheSerializableRunnable("Create Mirrored Region") {
        public void run2() throws CacheException {
          RegionAttributes ra = getRegionAttributes();
          AttributesFactory factory =
            new AttributesFactory(ra);
          if (ra.getEvictionAttributes() == null
              || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
            factory.setDiskStoreName(null);
          }
          factory.setDataPolicy(DataPolicy.REPLICATE);
          factory.addCacheListener(new MirroredDataFromNonMirroredListener());
          createRegion(name, factory.create());
        }
      });

    vm2.invoke(create);

    // Destroy the local entries so we know that they are not found by
    // a netSearch
    vm2.invoke(new CacheSerializableRunnable("Remove local entries") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.localDestroy(key1);
          region.localDestroy(key2);
          region.localDestroy(key3);
          flushIfNecessary(region);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Verify keys/values and listener") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);

          Region.Entry entry1 = region.getEntry(key1);
          assertNotNull(entry1);
          assertEquals(value1, entry1.getValue());

          Region.Entry entry2 = region.getEntry(key2);
          assertNotNull(entry2);
          assertEquals(value2, entry2.getValue());

          Region.Entry entry3 = region.getEntry(key3);
          assertNotNull(entry3);
          assertEquals(value3, entry3.getValue());

          MirroredDataFromNonMirroredListener lsnr =
            (MirroredDataFromNonMirroredListener)region.
            getAttributes().getCacheListeners()[0];

          assertTrue(lsnr.wasInvoked());
          assertTrue("expectedKeys should be empty, but was: " + lsnr.expectedKeys,
                     lsnr.expectedKeys.isEmpty());
        }
      });
  }


  /**
   * Tests that a mirrored region does not push data to a non-mirrored
   * region.
   */
  public void testNoMirroredDataToNonMirrored()
    throws InterruptedException {
    if (!supportsReplication()) {
      return;
    }

    final String name = this.getUniqueName();
    final Object key1 = "KEY1";
    final Object value1 = "VALUE1";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";
    final Object key3 = "KEY3";
    final Object value3 = "VALUE3";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2); // use VM on different gemfire system

    vm0.invoke(new CacheSerializableRunnable("Create Non-mirrored Region") {
        public void run2() throws CacheException {
          createRegion(name, getRegionAttributes());
        }
      });

    SerializableRunnable create = new CacheSerializableRunnable("Populate mirrored region") {
      public void run2() throws CacheException {
        RegionAttributes ra = getRegionAttributes();
        AttributesFactory factory =
          new AttributesFactory(ra);
        if (ra.getEvictionAttributes() == null
            || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
          factory.setDiskStoreName(null);
        }
        factory.setDataPolicy(DataPolicy.REPLICATE);
        Region region =
          createRegion(name, factory.create());
        region.put(key1, value1);
        region.put(key2, value2);
        region.put(key3, value3);
        flushIfNecessary(region);
      }
    };

    vm2.invoke(create);

    // Make sure that data wasn't pushed
    vm0.invoke(new CacheSerializableRunnable("Verify keys/values") {
        public void run2() throws CacheException {
          final Region region =
            getRootRegion().getSubregion(name);
          Region.Entry entry1 = region.getEntry(key1);
          if (!getRegionAttributes().getDataPolicy().withReplication()) {
            if (entry1 != null) {
              com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("found entry " + entry1);
            }
            assertNull(entry1);
          }
          else {
            assertNotNull(entry1);
          }

          Region.Entry entry2 = region.getEntry(key2);
          if (!getRegionAttributes().getDataPolicy().withReplication()) {
            assertNull(entry2);
          }
          else {
            assertNotNull(entry2);
          }

          Region.Entry entry3 = region.getEntry(key3);
          if (!getRegionAttributes().getDataPolicy().withReplication()) {
            assertNull(entry3);
          }
          else {
            assertNotNull(entry3);
          }
        }
    });
  }

  /**
   * Tests that a local load occurs, even with mirroring
   */
  public void testMirroredLocalLoad() {
    if (!supportsReplication()) {
      return;
    }
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2); // use VMs on different gemfire systems

    vm0.invoke(new CacheSerializableRunnable("Create region with loader") {
        public void run2() throws CacheException {
          RegionAttributes ra = getRegionAttributes();
          AttributesFactory factory =
            new AttributesFactory(ra);
          if (ra.getEvictionAttributes() == null
              || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
            factory.setDiskStoreName(null);
          }
          factory.setDataPolicy(DataPolicy.REPLICATE);
          factory.setCacheLoader(new TestCacheLoader() {
              public Object load2(LoaderHelper helper)
                throws CacheLoaderException {
                return value;
              }
            });
          createRegion(name, factory.create());
        }
    });

    SerializableRunnable create = new CacheSerializableRunnable("Create region with bad loader") {
      public void run2() throws CacheException {
        RegionAttributes ra = getRegionAttributes();
        AttributesFactory factory =
          new AttributesFactory(ra);
        if (ra.getEvictionAttributes() == null
            || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
          factory.setDiskStoreName(null);
        }
        factory.setDataPolicy(DataPolicy.REPLICATE);
        loader = new TestCacheLoader() {
          public Object load2(LoaderHelper helper)
          throws CacheLoaderException {

            fail("Should not be invoked");
            return null;
          }
        };
        factory.setCacheLoader(loader);
        createRegion(name, factory.create());
      }
    };


    vm2.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Get") {
      public void run2() throws CacheException {
       Region region =
         getRootRegion().getSubregion(name);
         assertEquals(value, region.get(key));
      }
    });

    vm2.invoke(new CacheSerializableRunnable("Verify no load") {
      public void run2() throws CacheException {
       assertFalse(loader.wasInvoked());
      }
    });
  }

  /**
   * Tests sure that a <code>netLoad</code> occurs, even with
   * mirroring
   */
  public void testMirroredNetLoad() {
    if (!supportsReplication()) {
      return;
    }
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2); // use VMs on different gemfire systems

    SerializableRunnable create = new CacheSerializableRunnable("Create region with loader") {
      public void run2() throws CacheException {
          RegionAttributes ra = getRegionAttributes();
          AttributesFactory factory =
            new AttributesFactory(ra);
          if (ra.getEvictionAttributes() == null
              || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
            factory.setDiskStoreName(null);
          }
          factory.setDataPolicy(DataPolicy.REPLICATE);
          factory.setCacheLoader(new TestCacheLoader() {
              public Object load2(LoaderHelper helper)
                throws CacheLoaderException {
                return value;
              }
          });
         createRegion(name, factory.create());
      }
    };


    vm0.invoke(new CacheSerializableRunnable("Create region with bad loader") {
        public void run2() throws CacheException {
          RegionAttributes ra = getRegionAttributes();
          AttributesFactory factory =
            new AttributesFactory(ra);
          if (ra.getEvictionAttributes() == null
              || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
            factory.setDiskStoreName(null);
          }
          factory.setDataPolicy(DataPolicy.REPLICATE);
          createRegion(name, factory.create());
        }
      });

    vm2.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Get") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          assertEquals(value, region.get(key));
        }
      });
  }

  ////////  Region Keep Alive Tests

  /**
   * Tests that a region is not kept alive
   */
  public void testNoRegionKeepAlive() throws InterruptedException {
    final String name = this.getUniqueName();
    final Object key = "KEEP_ALIVE_KEY";
    final Object value = "VALUE";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    vm0.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          createRegion(name);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Populate region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.put(key, value);
          assertEquals(value, region.get(key));
        }
      });
    vm0.invoke(new CacheSerializableRunnable("Close cache") {
        public void run2() throws CacheException {
          closeCache();
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Re-create cache") {
        public void run2() throws CacheException {
          Region region = createRegion(name);
          // if this is a backup region, then it will find the data
          // otherwise it should not
          if (region.getAttributes().getDataPolicy().withPersistence()) {
            assertEquals(value, region.get(key));
          }
          else {
            assertNull(region.get(key));
          }
        }
      });
  }



  public void testNetSearchObservesTtl()
  throws InterruptedException
  {
    if(getRegionAttributes().getPartitionAttributes() != null)
      return;
    
    final String name = this.getUniqueName();
    final int shortTimeout = 10; // ms
    final int longTimeout = 1000000; // ms
    final Object key = "KEY";
    final Object value = "VALUE";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(2);  // Other VM is from a different gemfire system

    SerializableRunnable create = new CacheSerializableRunnable("Create with TTL") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory(getRegionAttributes());
        factory.setStatisticsEnabled(true);
        ExpirationAttributes expire =
          new ExpirationAttributes(longTimeout, ExpirationAction.DESTROY);
        factory.setEntryTimeToLive(expire);
        Region region = null;
        System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
        try {
          region = createRegion(name, factory.create());
          region.create(key, value);
        } 
        finally {
          System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
        }
      }
    };

    vm1.invoke(create);

    // vm0 - Create region, short timeout
    vm0.invoke(new CacheSerializableRunnable("Create with TTL") {
      public void run2() throws CacheException {
        RegionAttributes ra = getRegionAttributes();
        AttributesFactory factory =
          new AttributesFactory(ra);
        final boolean partitioned = ra.getPartitionAttributes() != null
        || ra.getDataPolicy().withPartitioning() ;
        // MUST be nonmirrored, so turn off persistBackup if this is a disk region test
        if (!partitioned) {
          if (ra.getEvictionAttributes() == null
              || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
            factory.setDiskStoreName(null);
          }
          factory.setDataPolicy(DataPolicy.NORMAL);
        }
        factory.setStatisticsEnabled(true);
        ExpirationAttributes expire =
          new ExpirationAttributes(shortTimeout, ExpirationAction.DESTROY);
        factory.setEntryTimeToLive(expire);
        System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
        try {
          createRegion(name, factory.create());
        } 
        finally {
          System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
        }
      }
    });

    Wait.pause(shortTimeout * 10);

    // Even though netSearch finds vm1's entry is not expired, it is considered
    // expired with respect to vm0's attributes
    vm0.invoke(new CacheSerializableRunnable("get(key), expect null") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        Object got = region.get(key);
        assertNull(got);
      }
    });

    // We see the object is actually still there
    vm1.invoke(new CacheSerializableRunnable("get(key), expect value") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        Object got = region.get(key);
        assertEquals(value, got);
      }
    });
  }

  public void testNetSearchObservesIdleTime()
  throws InterruptedException
  {
    if(getRegionAttributes().getPartitionAttributes() != null)
      return;
    
    final String name = this.getUniqueName();
    final int shortTimeout = 10; // ms
    final int longTimeout = 10000; // ms
    final Object key = "KEY";
    final Object value = "VALUE";

    // if using shared memory, make sure we use two VMs on different
    // gemfire systems
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(2);

    SerializableRunnable create = new CacheSerializableRunnable("Create with IdleTimeout") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory(getRegionAttributes());
        factory.setStatisticsEnabled(true);
        ExpirationAttributes expire =
          new ExpirationAttributes(longTimeout, ExpirationAction.DESTROY);
        factory.setEntryIdleTimeout(expire);
        Region region = null;
        System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
        try {
          region = createRegion(name, factory.create());
          region.create(key, value);
        } 
        finally {
          System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
        }
      }
    };

    vm1.invoke(create);

    // vm0 - Create region, short timeout
    vm0.invoke(new CacheSerializableRunnable("Create with IdleTimeout") {
      public void run2() throws CacheException {
        RegionAttributes ra = getRegionAttributes();
        AttributesFactory factory =
          new AttributesFactory(ra);
        final boolean partitioned = ra.getPartitionAttributes() != null
        || ra.getDataPolicy().withPartitioning() ;
        // MUST be nonmirrored, so turn off persistBackup if this is a disk region test
        if (!partitioned) {
          if (ra.getEvictionAttributes() == null
              || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
            factory.setDiskStoreName(null);
          }
          factory.setDataPolicy(DataPolicy.NORMAL);
        }
        factory.setStatisticsEnabled(true);
        ExpirationAttributes expire =
          new ExpirationAttributes(shortTimeout, ExpirationAction.DESTROY);
        factory.setEntryIdleTimeout(expire);
        System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
        try {
          createRegion(name, factory.create());
        } 
        finally {
          System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
        }
      }
    });

    Wait.pause(shortTimeout * 2);

    // Even though netSearch finds vm1's entry is not expired, it is considered
    // expired with respect to vm0's attributes
    vm0.invoke(new CacheSerializableRunnable("get(key), expect null") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        Object got = region.get(key);
        assertNull(got);
      }
    });

    // We see the object is actually still there
    vm1.invoke(new CacheSerializableRunnable("get(key), expect value") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        Object got = region.get(key);
        assertEquals(value, got);
      }
    });
  }


  static TestCacheListener destroyListener = null;

  /**
   * Tests that an entry in a distributed region that expires with a distributed
   * destroy causes an event in other VM with isExpiration flag set.
   */
    public void testEntryTtlDestroyEvent()
    throws InterruptedException {
      
      if(getRegionAttributes().getPartitionAttributes() != null)
        return;
      
      final String name = this.getUniqueName();
      final int timeout = 22; // ms
      final Object key = "KEY";
      final Object value = "VALUE";

      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);

      class DestroyListener extends TestCacheListener {
        boolean eventIsExpiration = false;

        public void afterDestroyBeforeAddEvent(EntryEvent event) {
          eventIsExpiration = event.isExpiration();
        }
        public void afterDestroy2(EntryEvent event) {
          if (event.isOriginRemote()) {
            assertTrue(!event.getDistributedMember().equals(getSystem().getDistributedMember()));
          } else {
            assertEquals(getSystem().getDistributedMember(), event.getDistributedMember());
          }
          assertEquals(Operation.EXPIRE_DESTROY, event.getOperation());
          assertEquals(value, event.getOldValue());
          eventIsExpiration = event.getOperation().isExpiration();
        }

        public void afterCreate2(EntryEvent event) {
          // ignore
        }

        public void afterUpdate2(EntryEvent event) {
          // ignore
        }
      }


      SerializableRunnable createRegion = new CacheSerializableRunnable("Create with Listener") {
        public void run2() throws CacheException {
          AttributesFactory fac = new AttributesFactory(getRegionAttributes());
          fac.addCacheListener(destroyListener = new DestroyListener());
          createRegion(name, fac.create());
        }
      };

      vm1.invoke(createRegion);

      vm0.invoke(new CacheSerializableRunnable("Create with TTL") {
          public void run2() throws CacheException {
            AttributesFactory factory = new AttributesFactory(getRegionAttributes());
            factory.setStatisticsEnabled(true);
            ExpirationAttributes expire =
              new ExpirationAttributes(timeout,
                                       ExpirationAction.DESTROY);
            factory.setEntryTimeToLive(expire);
            if (!getRegionAttributes().getDataPolicy().withReplication()) {
              factory.setDataPolicy(DataPolicy.NORMAL);
              factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
            }
            System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
            try {
              createRegion(name, factory.create());
              ExpiryTask.suspendExpiration();
              // suspend to make sure we can see that the put is distributed to this member
            } 
            finally {
              System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
            }
          }
        });
      
      try {

      // let region create finish before doing put
      //pause(10);

      vm1.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          Region region = getRootRegion().getSubregion(name);
          DestroyListener dl = (DestroyListener)region.getAttributes().getCacheListeners()[0];
          dl.enableEventHistory();
          region.put(key, value);
          // reset listener after create event
          assertTrue(dl.wasInvoked());
          List<CacheEvent> history = dl.getEventHistory();
          CacheEvent ce = history.get(0);
          dl.disableEventHistory();
          assertEquals(Operation.CREATE, ce.getOperation());
          return null;
        }
      });
      vm0.invoke(new CacheSerializableRunnable("Check create received from vm1") {
        public void run2() throws CacheException {
          final Region region = getRootRegion().getSubregion(name);
          WaitCriterion waitForCreate = new WaitCriterion() {
            public boolean done() {
              return region.getEntry(key) != null;
            }
            public String description() {
              return "never saw create of " + key;
            }
          };
          Wait.waitForCriterion(waitForCreate, 3000, 10, true);
        }
      });
      
      } finally {
        vm0.invoke(new CacheSerializableRunnable("resume expiration") {
          public void run2() throws CacheException {
            ExpiryTask.permitExpiration();
          }
        });
      }
      
      // now wait for it to expire
      vm0.invoke(new CacheSerializableRunnable("Check local destroy") {
          public void run2() throws CacheException {
            final Region region = getRootRegion().getSubregion(name);
            WaitCriterion waitForExpire = new WaitCriterion() {
              public boolean done() {
                return region.getEntry(key) == null;
              }
              public String description() {
                return "never saw expire of " + key + " entry=" + region.getEntry(key);
              }
            };
            Wait.waitForCriterion(waitForExpire, 4000, 10, true);
          }
        });

      vm1.invoke(new CacheSerializableRunnable("Verify destroyed and event") {
          public void run2() throws CacheException {
            final Region region = getRootRegion().getSubregion(name);
            WaitCriterion waitForExpire = new WaitCriterion() {
              public boolean done() {
                return region.getEntry(key) == null;
              }
              public String description() {
                return "never saw expire of " + key + " entry=" + region.getEntry(key);
              }
            };
            Wait.waitForCriterion(waitForExpire, 4000, 10, true);
            assertTrue(destroyListener.waitForInvocation(555));
            assertTrue(((DestroyListener)destroyListener).eventIsExpiration);
          }
        });
    }

  /**
     * Tests that an entry in a distributed region expires with a local
     * destroy after a given time to live.
     */
    public void testEntryTtlLocalDestroy()
    throws InterruptedException {
      if(getRegionAttributes().getPartitionAttributes() != null)
        return;
      final boolean mirrored = getRegionAttributes().getDataPolicy().withReplication();
      final boolean partitioned = getRegionAttributes().getPartitionAttributes() != null ||
           getRegionAttributes().getDataPolicy().withPartitioning();
      if (!mirrored) {
        // This test fails intermittently because the DSClock we inherit from the existing
        // distributed system is stuck in the "stopped" state.
        // The DSClock is going away when java groups is merged and at that
        // time this following can be removed.
        disconnectAllFromDS();
      }

      final String name = this.getUniqueName();
      final int timeout = 10; // ms
      final Object key = "KEY";
      final Object value = "VALUE";

      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);

      SerializableRunnable create = new CacheSerializableRunnable("Populate") {
        public void run2() throws CacheException {
        System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
        try {
          Region region = createRegion(name);
        }
        finally {
          System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
        }
      }
      };

      vm1.invoke(create);
      
      vm0.invoke(new CacheSerializableRunnable("Create with TTL") {
          public void run2() throws CacheException {
            AttributesFactory factory = new AttributesFactory(getRegionAttributes());
            factory.setStatisticsEnabled(true);
            ExpirationAttributes expire =
              new ExpirationAttributes(timeout,
                                       ExpirationAction.LOCAL_DESTROY);
            factory.setEntryTimeToLive(expire);
            if (!mirrored) {
              // make it cached all events so that remote creates will also
              // be created here
              if(!partitioned){
               factory.setDataPolicy(DataPolicy.NORMAL);
              }
              factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
              factory.addCacheListener(new CountingDistCacheListener());
            }
            /**
             * Crank up the expiration so test runs faster.
             * This property only needs to be set while the region is created
             */
            System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
            try {
              createRegion(name, factory.create());
              if (mirrored) fail("Should have thrown an IllegalStateException");
            }
            catch (IllegalStateException e) {
              if (!mirrored) throw e;
            } 
            finally {
              System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
            }
          }
        });
      if (mirrored) return;

      vm1.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          Region region = getRootRegion().getSubregion(name);
          region.put(key, value);
          return null;
        }
      });
      
      vm0.invoke(new CacheSerializableRunnable("Check local destroy") {
          public void run2() throws CacheException {
            final Region region =
              getRootRegion().getSubregion(name);
            // make sure we created the entry
            {
              CountingDistCacheListener l = (CountingDistCacheListener)
                region.getAttributes().getCacheListeners()[0];
              int retry = 1000;
              while (retry-- > 0) {
                try {
                  l.assertCount(1, 0, 0, 0);
                  // TODO: a race exists in which assertCount may also see a destroyCount of 1
                  logger.info("DEBUG: saw create");
                  break;
                } catch (AssertionFailedError e) {
                  if (retry > 0) {
                    Wait.pause(1);
                  } else {
                    throw e;
                  }
                }
              }
            }

            { // now make sure it expires
              // this should happen really fast since timeout is 10 ms.
              // But it may take longer in some cases because of thread
              // scheduling delays and machine load (see GEODE-410).
              // The previous code would fail after 100ms; now we wait 3000ms.
              WaitCriterion waitForUpdate = new WaitCriterion() {
                public boolean done() {
                  Region.Entry re = region.getEntry(key);
                  if (re != null) {
                    EntryExpiryTask eet = getEntryExpiryTask(region, key);
                    if (eet != null) {
                      long stopTime = ((InternalDistributedSystem)(region.getCache().getDistributedSystem())).getClock().getStopTime();
                      logger.info("DEBUG: waiting for expire destroy expirationTime= " + eet.getExpirationTime() + " now=" + eet.getNow() + " stopTime=" + stopTime + " currentTimeMillis=" + System.currentTimeMillis());
                    } else {
                      logger.info("DEBUG: waiting for expire destroy but expiry task is null");
                    }
                  }
                  return re == null;
                }
                public String description() {
                  String expiryInfo = "";
                  try {
                    EntryExpiryTask eet = getEntryExpiryTask(region, key);
                    if (eet != null) {
                      expiryInfo = "expirationTime= " + eet.getExpirationTime() + " now=" + eet.getNow() + " currentTimeMillis=" + System.currentTimeMillis();
                    }
                  } catch (EntryNotFoundException ex) {
                    expiryInfo ="EntryNotFoundException when getting expiry task";
                  }
                  return "Entry for key " + key + " never expired (since it still exists) " + expiryInfo;
                }
              };
              Wait.waitForCriterion(waitForUpdate, 30000, 1, true);
            }
            assertNull(region.getEntry(key));
          }
        });

      vm1.invoke(new CacheSerializableRunnable("Verify local") {
          public void run2() throws CacheException {
            Region region =
              getRootRegion().getSubregion(name);
            Region.Entry entry = region.getEntry(key);
            assertEquals(value, entry.getValue());
          }
        });
    }
    
    private static EntryExpiryTask getEntryExpiryTask(Region r, Object key) {
      EntryExpiryTask result = null;
      try {
        LocalRegion lr = (LocalRegion) r;
        result = lr.getEntryExpiryTask(key);
      } catch (EntryNotFoundException ignore) {
      }
      return result;
    }

    /**
     * Tests to makes sure that a distributed update resets the
     * expiration timer.
     */
    public void testUpdateResetsIdleTime() throws InterruptedException {

      final String name = this.getUniqueName();
      // test no longer waits for this timeout to expire
      final int timeout = 90; // seconds
      final Object key = "KEY";
      final Object value = "VALUE";

      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);


      vm0.invoke(new CacheSerializableRunnable("Create with Idle") {
        public void run2() throws CacheException {
          AttributesFactory factory = new AttributesFactory(getRegionAttributes());
          factory.setStatisticsEnabled(true);
          ExpirationAttributes expire =
              new ExpirationAttributes(timeout,
                  ExpirationAction.DESTROY);
          factory.setEntryIdleTimeout(expire);
          LocalRegion region =
              (LocalRegion) createRegion(name, factory.create());
          if (region.getDataPolicy().withPartitioning()) {
            // Force all buckets to be created locally so the
            // test will know that the create happens in this vm
            // and the update (in vm1) is remote.
            PartitionRegionHelper.assignBucketsToPartitions(region);
          }
          region.create(key, null);
          EntryExpiryTask eet = region.getEntryExpiryTask(key);
          region.create("createExpiryTime", eet.getExpirationTime());
          Wait.waitForExpiryClockToChange(region);
        }
      });

      vm1.invoke(new CacheSerializableRunnable("Create Region " + name) {
        public void run2() throws CacheException {
          AttributesFactory factory = new AttributesFactory(getRegionAttributes());
          factory.setStatisticsEnabled(true);
          ExpirationAttributes expire =
              new ExpirationAttributes(timeout,
                  ExpirationAction.DESTROY);
          factory.setEntryIdleTimeout(expire);
          if(getRegionAttributes().getPartitionAttributes() != null){
            createRegion(name, factory.create());  
          } else {
            createRegion(name);
          }          
        }
      });

      vm1.invoke(new CacheSerializableRunnable("Update entry") {
        public void run2() throws CacheException {
          final Region r = getRootRegion().getSubregion(name);
          assertNotNull(r);
          r.put(key, value);
        }
      });

      vm0.invoke(new CacheSerializableRunnable("Verify reset") {
        public void run2() throws CacheException {
          final LocalRegion region =
              (LocalRegion) getRootRegion().getSubregion(name);

          // wait for update to reach us from vm1 (needed if no-ack)
          WaitCriterion waitForUpdate = new WaitCriterion() {
            public boolean done() {
              return value.equals(region.get(key));
            }
            public String description() {
              return "never saw update of " + key;
            }
          };
          Wait.waitForCriterion(waitForUpdate, 3000, 10, true);

          EntryExpiryTask eet = region.getEntryExpiryTask(key);
          long createExpiryTime = (Long) region.get("createExpiryTime");
          long updateExpiryTime = eet.getExpirationTime();
          if (updateExpiryTime - createExpiryTime <= 0L) {
            fail("update did not reset the expiration time. createExpiryTime=" + createExpiryTime + " updateExpiryTime=" + updateExpiryTime);
          }
        }
      });
    }


  private static final int NB1_CHUNK_SIZE = 500 * 1024; // == InitialImageOperation.CHUNK_SIZE_IN_BYTES
  private static final int NB1_NUM_ENTRIES = 1000;
  private static final int NB1_VALUE_SIZE = NB1_CHUNK_SIZE * 10 / NB1_NUM_ENTRIES;

  /**
   * Tests that distributed ack operations do not block while
   * another cache is doing a getInitialImage.
   */
  public void testNonblockingGetInitialImage() throws Throwable {
    if (!supportsReplication()) {
      return;
    }
    // don't run this test if global scope since its too difficult to predict
    // how many concurrent operations will occur
    if (getRegionAttributes().getScope().isGlobal()) {
      return;
    }

    final String name = this.getUniqueName();
    final byte[][] values = new byte[NB1_NUM_ENTRIES][];

    for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
      values[i] = new byte[NB1_VALUE_SIZE];
      Arrays.fill(values[i], (byte)0x42);
    }

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);

    SerializableRunnable create = new
      CacheSerializableRunnable("Create Mirrored Region") {
        public void run2() throws CacheException {
          beginCacheXml();
          { // root region must be DACK because its used to sync up async subregions
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.NORMAL);
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
            com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("MJT DEBUG: attrs0 are " + factory.create());
            createRootRegion(factory.create());
          }
          {
            AttributesFactory factory =
              new AttributesFactory(getRegionAttributes());
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
            if (getRegionAttributes().getDataPolicy() == DataPolicy.NORMAL) {
              factory.setDataPolicy(DataPolicy.PRELOADED);
            }
            com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("MJT DEBUG: attrs1 are " + factory.create());
            Region region = createRegion(name, factory.create());
          }
          finishCacheXml(name);
          // reset slow
          com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
        }
      };


    vm0.invoke(new
      CacheSerializableRunnable("Create Nonmirrored Region") {
        public void run2() throws CacheException {
          { // root region must be DACK because its used to sync up async subregions
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.EMPTY);
            createRootRegion(factory.create());
          }
          {
            AttributesFactory factory =
              new AttributesFactory(getRegionAttributes());
            createRegion(name, factory.create());
          }
          // reset slow
          com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Put initial data") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
            region.put(new Integer(i), values[i]);
          }
          assertEquals(NB1_NUM_ENTRIES, region.keySet().size());
        }
      });

    // start asynchronous process that does updates to the data
    AsyncInvocation async = vm0.invokeAsync(new CacheSerializableRunnable("Do Nonblocking Operations") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);

        // wait for profile of getInitialImage cache to show up
        final com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor adv =
          ((com.gemstone.gemfire.internal.cache.DistributedRegion)region).getCacheDistributionAdvisor();
        final int expectedProfiles = 1;
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            DataPolicy currentPolicy = getRegionAttributes().getDataPolicy();
            if (currentPolicy == DataPolicy.PRELOADED) {
              return (adv.advisePreloadeds().size()+adv.adviseReplicates().size()) >= expectedProfiles;
            } else {
              return adv.adviseReplicates().size() >= expectedProfiles;              
            }
          }
          public String description() {
            return "replicate count never reached " + expectedProfiles;
          }
        };
        Wait.waitForCriterion(ev, 60 * 1000, 200, true);

        DataPolicy currentPolicy = getRegionAttributes().getDataPolicy();
        int numProfiles = 0;
        if (currentPolicy == DataPolicy.PRELOADED) {
          numProfiles = adv.advisePreloadeds().size()+adv.adviseReplicates().size();
        } else {
          numProfiles = adv.adviseReplicates().size();              
        }
        Assert.assertTrue(numProfiles >= expectedProfiles);
        
        // operate on every odd entry with different value, alternating between
        // updates, invalidates, and destroys. These operations are likely
        // to be nonblocking if a sufficient number of updates get through
        // before the get initial image is complete.
        for (int i = 1; i < NB1_NUM_ENTRIES; i += 2) {
          Object key = new Integer(i);
          com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Operation #"+i+" on key " + key);
          switch (i % 6) {
            case 1: // UPDATE
              // use the current timestamp so we know when it happened
              // we could have used last modification timestamps, but
              // this works without enabling statistics
              Object value = new Long(System.currentTimeMillis());
              region.put(key, value);
              // no longer safe since get is not allowed to member doing GII
//               if (getRegionAttributes().getScope().isDistributedAck()) {
//                 // do a nonblocking netSearch
//                 region.localInvalidate(key);
//                 assertEquals(value, region.get(key));
//               }
              break;
            case 3: // INVALIDATE
              region.invalidate(key);
              if (getRegionAttributes().getScope().isDistributedAck()) {
                // do a nonblocking netSearch
                assertNull(region.get(key));
              }
              break;
            case 5: // DESTROY
              region.destroy(key);
              if (getRegionAttributes().getScope().isDistributedAck()) {
                // do a nonblocking netSearch
                assertNull(region.get(key));
              }
              break;
            default: fail("unexpected modulus result: " + i);
              break;
          }
        }
        
          // add some new keys
        for (int i = NB1_NUM_ENTRIES; i < NB1_NUM_ENTRIES + 200; i++) {
          region.create(new Integer(i), new Long(System.currentTimeMillis()));
        }
        // now do a put and our DACK root region which will not complete
        // until processed on otherside which means everything done before this
        // point has been processed
        getRootRegion().put("DONE", "FLUSH_OPS");
      }
    });

    // in the meantime, do the get initial image in vm2
    // slow down image processing to make it more likely to get async updates
    if (!getRegionAttributes().getScope().isGlobal()) {
      vm2.invoke(new SerializableRunnable("Set slow image processing") {
          public void run() {
            // if this is a no_ack test, then we need to slow down more because of the
            // pauses in the nonblocking operations
            int pause = 200;
            com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = pause;
          }
        });
    }

    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Before GetInitialImage, data policy is "+getRegionAttributes().getDataPolicy()+", scope is "+getRegionAttributes().getScope());
    AsyncInvocation asyncGII = vm2.invokeAsync(create);

    if (!getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      ThreadUtils.join(async, 30 * 1000);
      vm2.invoke(new SerializableRunnable("Set fast image processing") {
          public void run() {
            com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
          }
        });
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("after async nonblocking ops complete");
    }

    // wait for GII to complete
    ThreadUtils.join(asyncGII, 30 * 1000);
    final long iiComplete = System.currentTimeMillis();
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Complete GetInitialImage at: " + System.currentTimeMillis());

    if (getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      ThreadUtils.join(async, 30 * 1000);
    }
    if (async.exceptionOccurred()) {
      com.gemstone.gemfire.test.dunit.Assert.fail("async failed", async.getException());
    }
    if (asyncGII.exceptionOccurred()) {
      com.gemstone.gemfire.test.dunit.Assert.fail("asyncGII failed", asyncGII.getException());
    }

    // Locally destroy the region in vm0 so we know that they are not found by
    // a netSearch
    vm0.invoke(new CacheSerializableRunnable("Locally destroy region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      });
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("after localDestroyRegion");

      // invoke repeating so noack regions wait for all updates to get processed
    vm2.invokeRepeatingIfNecessary(new CacheSerializableRunnable("Verify entryCount") {
        boolean entriesDumped = false;

        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          // expected entry count (subtract entries destroyed)
          int entryCount = NB1_NUM_ENTRIES + 200 - NB1_NUM_ENTRIES / 6;
          int actualCount = region.entrySet(false).size();
          if (actualCount == NB1_NUM_ENTRIES + 200) {
            // entries not destroyed, dump entries that were supposed to have been destroyed
            dumpDestroyedEntries(region);
          }
          assertEquals(entryCount, actualCount);
        }

        private void dumpDestroyedEntries(Region region) throws EntryNotFoundException {
          if (entriesDumped) return;
          entriesDumped = true;

          LogWriter logger = com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter();
          logger.info("DUMPING Entries with values in VM that should have been destroyed:");
          for (int i = 5; i < NB1_NUM_ENTRIES; i += 6) {
            try {
            logger.info(i + "-->" +
              ((com.gemstone.gemfire.internal.cache.LocalRegion)region).getValueInVM(new Integer(i)));
            } catch(EntryNotFoundException expected) {
              logger.info(i + "-->" +
                   "CORRECTLY DESTROYED");
            }
          }
        }
    }, 5000);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("after verify entryCount");


    vm2.invoke(new CacheSerializableRunnable("Verify keys/values & Nonblocking") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          // expected entry count (subtract entries destroyed)
          int entryCount = NB1_NUM_ENTRIES + 200 - NB1_NUM_ENTRIES / 6;
          assertEquals(entryCount, region.entrySet(false).size());
          // determine how many entries were updated before getInitialImage
          // was complete
          int numConcurrent = 0;
          for (int i = 0; i < NB1_NUM_ENTRIES + 200; i++) {
            Region.Entry entry = region.getEntry(new Integer(i));
            Object v = entry == null ? null : entry.getValue();
            if (i < NB1_NUM_ENTRIES) { // old keys
              switch (i % 6) {
                // even keys are originals
                case 0: case 2: case 4:
                  assertNotNull(entry);
                  assertTrue(Arrays.equals(values[i], (byte[])v));
                  break;
                case 1: // updated
                  assertNotNull(v);
                  assertTrue("Value for key " + i + " is not a Long, is a " +
                             v.getClass().getName(), v instanceof Long);
                  Long timestamp = (Long)entry.getValue();
                  if (timestamp.longValue() < iiComplete) {
                    numConcurrent++;
                  }
                  break;
                case 3: // invalidated
                  assertNotNull(entry);
                  assertNull("Expected value for " + i + " to be null, but was " + v, v);
                  break;
                case 5: // destroyed
                  assertNull(entry);
                  break;
                default:
                  fail("unexpected modulus result: " + (i % 6));
                  break;
              }
            }
            else { // new keys
              assertNotNull(v);
              assertTrue("Value for key " + i + " is not a Long, is a " +
                         v.getClass().getName(), v instanceof Long);
              Long timestamp = (Long)entry.getValue();
              if (timestamp.longValue() < iiComplete) {
                numConcurrent++;
              }
            }
          }
          com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": " + numConcurrent + " entries out of " + entryCount +
                              " were updated concurrently with getInitialImage");
          // make sure at least some of them were concurrent
          if (region.getAttributes().getScope().isGlobal()) {
            assertTrue("Too many concurrent updates when expected to block: " + numConcurrent,
                        numConcurrent < 10);
          }
          else {
            int min = 30;
            assertTrue("Not enough updates concurrent with getInitialImage occurred to my liking. "
                        + numConcurrent + " entries out of " + entryCount +
                        " were updated concurrently with getInitialImage, and I'd expect at least " +
                        min + " or so", numConcurrent >= min);
          }
        }
      });
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("after verify key/values");
  }

  /**
   * Tests that distributed ack operations do not block while
   * another cache is doing a getInitialImage.
   */
  public void testTXNonblockingGetInitialImage() throws Throwable {
    if (!supportsReplication()) {
      return;
    }
    if (!supportsTransactions()) {
      return;
    }
    // don't run this test if global scope since its too difficult to predict
    // how many concurrent operations will occur
    if (getRegionAttributes().getScope().isGlobal()
        || getRegionAttributes().getDataPolicy().withPersistence()) {
      return;
    }

    final String name = this.getUniqueName();
    final byte[][] values = new byte[NB1_NUM_ENTRIES][];

    for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
      values[i] = new byte[NB1_VALUE_SIZE];
      Arrays.fill(values[i], (byte)0x42);
    }

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);

    SerializableRunnable create = new
      CacheSerializableRunnable("Create Mirrored Region") {
        public void run2() throws CacheException {
          beginCacheXml();
          { // root region must be DACK because its used to sync up async subregions
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.NORMAL);
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
            createRootRegion(factory.create());
          }
          {
            AttributesFactory factory =
              new AttributesFactory(getRegionAttributes());
            if (getRegionAttributes().getDataPolicy() == DataPolicy.NORMAL) {
              factory.setDataPolicy(DataPolicy.PRELOADED);
            }
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
            createRegion(name, factory.create());
          }
          finishCacheXml(name);
          // reset slow
          com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
        }
      };


    vm0.invoke(new
      CacheSerializableRunnable("Create Nonmirrored Region") {
        public void run2() throws CacheException {
          { // root region must be DACK because its used to sync up async subregions
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.EMPTY);
            createRootRegion(factory.create());
          }
          {
            AttributesFactory factory =
              new AttributesFactory(getRegionAttributes());
            createRegion(name, factory.create());
          }
          // reset slow
          com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Put initial data") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
            region.put(new Integer(i), values[i]);
          }
          assertEquals(NB1_NUM_ENTRIES, region.keySet().size());
        }
      });

    // start asynchronous process that does updates to the data
    AsyncInvocation async = vm0.invokeAsync(new CacheSerializableRunnable("Do Nonblocking Operations") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);

        // wait for profile of getInitialImage cache to show up
        final com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor adv =
          ((com.gemstone.gemfire.internal.cache.DistributedRegion)region).getCacheDistributionAdvisor();
        final int expectedProfiles = 1;
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            DataPolicy currentPolicy = getRegionAttributes().getDataPolicy(); 
            if (currentPolicy == DataPolicy.PRELOADED) {
              return (adv.advisePreloadeds().size()+adv.adviseReplicates().size()) >= expectedProfiles;
            } else {
              return adv.adviseReplicates().size() >= expectedProfiles;              
            }
          }
          public String description() {
            return "replicate count never reached " + expectedProfiles;
          }
        };
        Wait.waitForCriterion(ev, 100 * 1000, 200, true);

        // operate on every odd entry with different value, alternating between
        // updates, invalidates, and destroys. These operations are likely
        // to be nonblocking if a sufficient number of updates get through
        // before the get initial image is complete.
        CacheTransactionManager txMgr = getCache().getCacheTransactionManager();
        for (int i = 1; i < NB1_NUM_ENTRIES; i += 2) {
          Object key = new Integer(i);
          switch (i % 6) {
            case 1: // UPDATE
              // use the current timestamp so we know when it happened
              // we could have used last modification timestamps, but
              // this works without enabling statistics
              Object value = new Long(System.currentTimeMillis());
              txMgr.begin();
              region.put(key, value);
              txMgr.commit();
              // no longer safe since get is not allowed to member doing GII
//               if (getRegionAttributes().getScope().isDistributedAck()) {
//                 // do a nonblocking netSearch
//                 region.localInvalidate(key);
//                 assertEquals(value, region.get(key));
//               }
              break;
            case 3: // INVALIDATE
              txMgr.begin();
              region.invalidate(key);
              txMgr.commit();
              if (getRegionAttributes().getScope().isDistributedAck()) {
                // do a nonblocking netSearch
                assertNull(region.get(key));
              }
              break;
            case 5: // DESTROY
              txMgr.begin();
              region.destroy(key);
              txMgr.commit();
              if (getRegionAttributes().getScope().isDistributedAck()) {
                // do a nonblocking netSearch
                assertNull(region.get(key));
              }
              break;
            default: fail("unexpected modulus result: " + i);
              break;
          }
        }
          // add some new keys
        for (int i = NB1_NUM_ENTRIES; i < NB1_NUM_ENTRIES + 200; i++) {
          txMgr.begin();
          region.create(new Integer(i), new Long(System.currentTimeMillis()));
          txMgr.commit();
        }
        // now do a put and our DACK root region which will not complete
        // until processed on otherside which means everything done before this
        // point has been processed
        getRootRegion().put("DONE", "FLUSH_OPS");
      }
    });

    // in the meantime, do the get initial image in vm2
    // slow down image processing to make it more likely to get async updates
    if (!getRegionAttributes().getScope().isGlobal()) {
      vm2.invoke(new SerializableRunnable("Set slow image processing") {
          public void run() {
            // if this is a no_ack test, then we need to slow down more because of the
            // pauses in the nonblocking operations
            int pause = 200;
            com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = pause;
          }
        });
    }

    AsyncInvocation asyncGII = vm2.invokeAsync(create);


    if (!getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      ThreadUtils.join(async, 30 * 1000);

      vm2.invoke(new SerializableRunnable("Set fast image processing") {
          public void run() {
            com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
          }
        });
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("after async nonblocking ops complete");
    }

    // wait for GII to complete
    ThreadUtils.join(asyncGII, 30 * 1000);
    final long iiComplete = System.currentTimeMillis();
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Complete GetInitialImage at: " + System.currentTimeMillis());
    if (getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      ThreadUtils.join(async, 30 * 1000);
    }

    if (async.exceptionOccurred()) {
      com.gemstone.gemfire.test.dunit.Assert.fail("async failed", async.getException());
    }
    if (asyncGII.exceptionOccurred()) {
      com.gemstone.gemfire.test.dunit.Assert.fail("asyncGII failed", asyncGII.getException());
    }

    // Locally destroy the region in vm0 so we know that they are not found by
    // a netSearch
    vm0.invoke(new CacheSerializableRunnable("Locally destroy region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      });

      // invoke repeating so noack regions wait for all updates to get processed
    vm2.invokeRepeatingIfNecessary(new CacheSerializableRunnable("Verify entryCount") {
        boolean entriesDumped = false;

        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          // expected entry count (subtract entries destroyed)
          int entryCount = NB1_NUM_ENTRIES + 200 - NB1_NUM_ENTRIES / 6;
          int actualCount = region.entrySet(false).size();
          if (actualCount == NB1_NUM_ENTRIES + 200) {
            // entries not destroyed, dump entries that were supposed to have been destroyed
            dumpDestroyedEntries(region);
          }
          assertEquals(entryCount, actualCount);
        }

        private void dumpDestroyedEntries(Region region) throws EntryNotFoundException {
          if (entriesDumped) return;
          entriesDumped = true;

          LogWriter logger = com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter();
          logger.info("DUMPING Entries with values in VM that should have been destroyed:");
          for (int i = 5; i < NB1_NUM_ENTRIES; i += 6) {
            logger.info(i + "-->" +
              ((com.gemstone.gemfire.internal.cache.LocalRegion)region).getValueInVM(new Integer(i)));
          }
        }
    }, 5000);


    vm2.invoke(new CacheSerializableRunnable("Verify keys/values & Nonblocking") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          // expected entry count (subtract entries destroyed)
          int entryCount = NB1_NUM_ENTRIES + 200 - NB1_NUM_ENTRIES / 6;
          assertEquals(entryCount, region.entrySet(false).size());
          // determine how many entries were updated before getInitialImage
          // was complete
          int numConcurrent = 0;
          for (int i = 0; i < NB1_NUM_ENTRIES + 200; i++) {
            Region.Entry entry = region.getEntry(new Integer(i));
            Object v = entry == null ? null : entry.getValue();
            if (i < NB1_NUM_ENTRIES) { // old keys
              switch (i % 6) {
                // even keys are originals
                case 0: case 2: case 4:
                  assertNotNull(entry);
                  assertTrue(Arrays.equals(values[i], (byte[])v));
                  break;
                case 1: // updated
                  assertNotNull(v);
                  assertTrue("Value for key " + i + " is not a Long, is a " +
                             v.getClass().getName(), v instanceof Long);
                  Long timestamp = (Long)entry.getValue();
                  if (timestamp.longValue() < iiComplete) {
                    numConcurrent++;
                  }
                  break;
                case 3: // invalidated
                  assertNotNull(entry);
                  assertNull("Expected value for " + i + " to be null, but was " + v, v);
                  break;
                case 5: // destroyed
                  assertNull(entry);
                  break;
                default:
                  fail("unexpected modulus result: " + (i % 6));
                  break;
              }
            }
            else { // new keys
              assertNotNull(v);
              assertTrue("Value for key " + i + " is not a Long, is a " +
                         v.getClass().getName(), v instanceof Long);
              Long timestamp = (Long)entry.getValue();
              if (timestamp.longValue() < iiComplete) {
                numConcurrent++;
              }
            }
          }
          com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": " + numConcurrent + " entries out of " + entryCount +
                              " were updated concurrently with getInitialImage");
          // make sure at least some of them were concurrent
          {
            int min = 30;
            assertTrue("Not enough updates concurrent with getInitialImage occurred to my liking. "
                        + numConcurrent + " entries out of " + entryCount +
                        " were updated concurrently with getInitialImage, and I'd expect at least " +
                        min + " or so", numConcurrent >= min);
          }
        }
      });
  }


  @Ignore("Disabled for 51542")
  public void DISABLED_testNBRegionInvalidationDuringGetInitialImage() throws Throwable {
    DistributedTestCase.disconnectAllFromDS();
    if (!supportsReplication()) {
      return;
    }
    // don't run this for noAck, too many race conditions
    if (getRegionAttributes().getScope().isDistributedNoAck()) return;

    final String name = this.getUniqueName();
    final byte[][] values = new byte[NB1_NUM_ENTRIES][];

    for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
      values[i] = new byte[NB1_VALUE_SIZE];
      Arrays.fill(values[i], (byte)0x42);
    }

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);

    SerializableRunnable create = new
      CacheSerializableRunnable("Create Mirrored Region") {
        public void run2() throws CacheException {
          beginCacheXml();
          { // root region must be DACK because its used to sync up async subregions
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.NORMAL);
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
            createRootRegion(factory.create());
          }
          {
            AttributesFactory factory =
              new AttributesFactory(getRegionAttributes());
            factory.setDataPolicy(DataPolicy.REPLICATE);
            createRegion(name, factory.create());
          }
          finishCacheXml(name);
          // reset slow
          com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
        }
      };


    vm0.invoke(new
      CacheSerializableRunnable("Create Nonmirrored Region") {
        public void run2() throws CacheException {
          { // root region must be DACK because its used to sync up async subregions
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.EMPTY);
            createRootRegion(factory.create());
          }
          {
            AttributesFactory factory =
              new AttributesFactory(getRegionAttributes());
            factory.setDataPolicy(DataPolicy.REPLICATE);
            createRegion(name, factory.create());
          }
          // reset slow
          com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Put initial data") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
            region.put(new Integer(i), values[i]);
          }
          assertEquals(NB1_NUM_ENTRIES, region.keySet().size());
        }
      });


//    attachDebugger(vm0, "vm0");
//    attachDebugger(vm2, "vm2");

    // start asynchronous process that does updates to the data
    AsyncInvocation async = vm0.invokeAsync(new CacheSerializableRunnable("Do Nonblocking Operations") {
      public void run2() throws CacheException {
        Region region =
          getRootRegion().getSubregion(name);

        // wait for profile of getInitialImage cache to show up
        final com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor adv =
          ((com.gemstone.gemfire.internal.cache.DistributedRegion)region).getCacheDistributionAdvisor();
        final int expectedProfiles = 1;
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return adv.adviseReplicates().size() >= expectedProfiles;
          }
          public String description() {
            return "profile count never reached " + expectedProfiles;
          }
        };
        Wait.waitForCriterion(ev, 30 * 1000, 200, true);

        // operate on every odd entry with different value, alternating between
        // updates, invalidates, and destroys. These operations are likely
        // to be nonblocking if a sufficient number of updates get through
        // before the get initial image is complete.
        for (int i = 1; i < NB1_NUM_ENTRIES; i += 2) {

          // at magical number 301, do a region invalidation, then continue
          // as before
          if (i == 301) {
//            DebuggerSupport.waitForJavaDebugger(getLogWriter(), "About to invalidate region");
            // wait for previous updates to be processed
            flushIfNecessary(region);
            region.invalidateRegion();
            flushIfNecessary(region);
          }

          Object key = new Integer(i);
          switch (i % 6) {
            case 1: // UPDATE
              // use the current timestamp so we know when it happened
              // we could have used last modification timestamps, but
              // this works without enabling statistics
              Object value = new Long(System.currentTimeMillis());
              region.put(key, value);
              // no longer safe since get is not allowed to member doing GII
//               if (getRegionAttributes().getScope().isDistributedAck()) {
//                 // do a nonblocking netSearch
//                 region.localInvalidate(key);
//                 assertEquals(value, region.get(key));
//               }
              break;
            case 3: // INVALIDATE
              region.invalidate(key);
              if (getRegionAttributes().getScope().isDistributedAck()) {
                // do a nonblocking netSearch
                value = region.get(key);
                assertNull("Expected null value for key: " + i + " but got " + value,
                          value);
              }
              break;
            case 5: // DESTROY
              region.destroy(key);
              if (getRegionAttributes().getScope().isDistributedAck()) {
                // do a nonblocking netSearch
                assertNull(region.get(key));
              }
              break;
            default: fail("unexpected modulus result: " + i);
              break;
          }
        }
        // now do a put and our DACK root region which will not complete
        // until processed on otherside which means everything done before this
        // point has been processed
        getRootRegion().put("DONE", "FLUSH_OPS");
      }
    });

    // in the meantime, do the get initial image in vm2
    // slow down image processing to make it more likely to get async updates
    if (!getRegionAttributes().getScope().isGlobal()) {
      vm2.invoke(new SerializableRunnable("Set slow image processing") {
          public void run() {
            // make sure the cache is set up before turning on slow
            // image processing
            getRootRegion();
            // if this is a no_ack test, then we need to slow down more because of the
            // pauses in the nonblocking operations
            int pause = /*getRegionAttributes().getScope().isAck() ? */100/* : 300*/;
            com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = pause;
          }
        });
    }

    AsyncInvocation asyncGII = vm2.invokeAsync(create);


    if (!getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      try {
        ThreadUtils.join(async, 30 * 1000);
      } finally {
        vm2.invoke(new SerializableRunnable("Set fast image processing") {
          public void run() {
            com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
          }
        });
      }
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("after async nonblocking ops complete");
    }

    // wait for GII to complete
    ThreadUtils.join(asyncGII, 30 * 1000);
    final long iiComplete = System.currentTimeMillis();
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Complete GetInitialImage at: " + System.currentTimeMillis());
    if (getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      ThreadUtils.join(async, 30 * 1000);
    }
    if (asyncGII.exceptionOccurred()) {
      throw new Error("asyncGII failed", asyncGII.getException());
    }
    if (async.exceptionOccurred()) {
      throw new Error("async failed", async.getException());
    }

    // Locally destroy the region in vm0 so we know that they are not found by
    // a netSearch
    vm0.invoke(new CacheSerializableRunnable("Locally destroy region") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      });


      // invoke repeating so noack regions wait for all updates to get processed
    vm2.invokeRepeatingIfNecessary(new CacheSerializableRunnable("Verify entryCount") {
        private boolean entriesDumped = false;

        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          // expected entry count (subtract entries destroyed)
          int entryCount = NB1_NUM_ENTRIES - NB1_NUM_ENTRIES / 6;
          int actualCount = region.entrySet(false).size();
          if (actualCount == NB1_NUM_ENTRIES) {
            // entries not destroyed, dump entries that were supposed to have been destroyed
            dumpDestroyedEntries(region);
          }
          assertEquals(entryCount, actualCount);
        }

        private void dumpDestroyedEntries(Region region) throws EntryNotFoundException {
          if (entriesDumped) return;
          entriesDumped = true;

          LogWriter logger = com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter();
          logger.info("DUMPING Entries with values in VM that should have been destroyed:");
          for (int i = 5; i < NB1_NUM_ENTRIES; i += 6) {
            logger.info(i + "-->" +
              ((com.gemstone.gemfire.internal.cache.LocalRegion)region).getValueInVM(new Integer(i)));
          }
        }
    }, 3000);

    vm2.invoke(new CacheSerializableRunnable("Verify keys/values & Nonblocking") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          // expected entry count (subtract entries destroyed)
          int entryCount = NB1_NUM_ENTRIES - NB1_NUM_ENTRIES / 6;
          assertEquals(entryCount, region.entrySet(false).size());
          // determine how many entries were updated before getInitialImage
          // was complete
          int numConcurrent = 0;

          for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
            Region.Entry entry = region.getEntry(new Integer(i));
            if (i < 301) {
              if (i % 6 == 5) {
                assertNull("Expected entry for " + i + " to be destroyed but it is " + entry,
                            entry); // destroyed
              }
              else {
                assertNotNull(entry);
                Object v = entry.getValue();
                assertNull("Expected value for " + i + " to be null, but was " + v,
                            v);
              }
            }
            else {
              Object v = entry == null ? null : entry.getValue();
              switch (i % 6) {
                // even keys are originals
                case 0: case 2: case 4:
                  assertNotNull(entry);
                  assertNull("Expected value for " + i + " to be null, but was " + v,
                              v);
                  break;
                case 1: // updated
                  assertNotNull("Expected to find an entry for #"+i, entry);
                  assertNotNull("Expected to find a value for #"+i, v);
                  assertTrue("Value for key " + i + " is not a Long, is a " +
                             v.getClass().getName(), v instanceof Long);
                  Long timestamp = (Long)entry.getValue();
                  if (timestamp.longValue() < iiComplete) {
                    numConcurrent++;
                  }
                  break;
                case 3: // invalidated
                  assertNotNull("Expected to find an entry for #"+i, entry);
                  assertNull("Expected value for " + i + " to be null, but was " + v, v);
                  break;
                case 5: // destroyed
                  assertNull("Expected to not find an entry for #"+i, entry);
                  break;
                default:
                  fail("unexpected modulus result: " + (i % 6));
                  break;
              }
            }
          }
          com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": " + numConcurrent + " entries out of " + entryCount +
                              " were updated concurrently with getInitialImage");

          // [sumedh] Occasionally fails. Do these assertions really make sense?
          // Looks like some random expectations that will always be a hit/miss.

          // make sure at least some of them were concurrent
          if (getRegionAttributes().getScope().isGlobal()) {
            assertTrue("Too many concurrent updates when expected to block: " + numConcurrent,
                        numConcurrent < 300);
          }
          else {
            assertTrue("Not enough updates concurrent with getInitialImage occurred to my liking. "
                        + numConcurrent + " entries out of " + entryCount +
                        " were updated concurrently with getInitialImage, and I'd expect at least 50 or so",
                        numConcurrent >= 30);
          }
        }
      });
  }

  public void testNBRegionDestructionDuringGetInitialImage() throws Throwable {
    if (!supportsReplication()) {
      return;
    }
    final String name = this.getUniqueName();
    final byte[][] values = new byte[NB1_NUM_ENTRIES][];

    for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
      values[i] = new byte[NB1_VALUE_SIZE];
      Arrays.fill(values[i], (byte)0x42);
    }

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);

    vm0.invoke(new
      CacheSerializableRunnable("Create Nonmirrored Region") {
        public void run2() throws CacheException {
          { // root region must be DACK because its used to sync up async subregions
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.EMPTY);
            createRootRegion(factory.create());
          }
          {
            AttributesFactory factory =
              new AttributesFactory(getRegionAttributes());
            createRegion(name, factory.create());
          }
          // reset slow
          com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Put initial data") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
            region.put(new Integer(i), values[i]);
          }
          assertEquals(NB1_NUM_ENTRIES, region.keySet().size());
        }
      });


//    attachDebugger(vm0, "vm0");
//    attachDebugger(vm2, "vm2");

    // start asynchronous process that does updates to the data
    AsyncInvocation async = vm0.invokeAsync(new CacheSerializableRunnable("Do Nonblocking Operations") {
      public void run2() throws CacheException {
        Wait.pause(200); // give the gii guy a chance to start
        Region region =
          getRootRegion().getSubregion(name);

        // wait for profile of getInitialImage cache to show up
        final com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor adv =
          ((com.gemstone.gemfire.internal.cache.DistributedRegion)region).getCacheDistributionAdvisor();
//        int numProfiles;
        final int expectedProfiles = 1;
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return expectedProfiles == adv.adviseReplicates().size();
          }
          public String description() {
            return "profile count never became exactly " + expectedProfiles;
          }
        };
        Wait.waitForCriterion(ev, 60 * 1000, 200, true);

        // since we want to force a GII while updates are flying, make sure
        // the other VM gets its CreateRegionResponse and starts its GII
        // before falling into the update loop
        /*
        pause(50);
        ((com.gemstone.gemfire.distributed.internal.InternalDistributedSystem)
          region.getCache().getDistributedSystem()).flushUnicastMessages();
        */

        // operate on every odd entry with different value, alternating between
        // updates, invalidates, and destroys. These operations are likely
        // to be nonblocking if a sufficient number of updates get through
        // before the get initial image is complete.
        for (int i = 1; i < 301; i += 2) {
          //getLogWriter().info("doing nonblocking op #"+i);
          Object key = new Integer(i);
          switch (i % 6) {
            case 1: // UPDATE
              // use the current timestamp so we know when it happened
              // we could have used last modification timestamps, but
              // this works without enabling statistics
              Object value = new Long(System.currentTimeMillis());
              region.put(key, value);
              // no longer safe since get is not allowed to member doing GII
//               if (getRegionAttributes().getScope().isDistributedAck()) {
//                 // do a nonblocking netSearch
//                 region.localInvalidate(key);
//                 assertEquals(value, region.get(key));
//               }
              break;
            case 3: // INVALIDATE
              region.invalidate(key);
              if (region.getAttributes().getScope().isDistributedAck()) {
                // do a nonblocking netSearch
                value = region.get(key);
                assertNull("Expected null value for key: " + i + " but got " + value,
                          value);
              }
              break;
            case 5: // DESTROY
              region.destroy(key);
              if (region.getAttributes().getScope().isDistributedAck()) {
                // do a nonblocking netSearch
                assertNull(region.get(key));
              }
              break;
            default: fail("unexpected modulus result: " + i);
              break;
          }
        }

          // at magical number 301, do a region destruction
        //getLogWriter().info("doing destroyRegion");
        region.destroyRegion();
        //getLogWriter().info("finished destroyRegion");
        // now do a put and our DACK root region which will not complete
        // until processed on otherside which means everything done before this
        // point has been processed
        {
          Region rr = getRootRegion();
          if (rr != null) {
            rr.put("DONE", "FLUSH_OPS");
          }
        }
      }
    });
    
    IgnoredException ex = IgnoredException.addIgnoredException("RegionDestroyedException");
    try {
    // in the meantime, do the get initial image in vm2
    AsyncInvocation asyncGII = vm2.invokeAsync(new CacheSerializableRunnable("Create Mirrored Region") {
        public void run2() throws CacheException {
          if (!getRegionAttributes().getScope().isGlobal()) {
            int pause = 200;
            com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = pause;
          }

          beginCacheXml();
          { // root region must be DACK because its used to sync up async subregions
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.NORMAL);
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
            createRootRegion(factory.create());
          }
          {
            RegionAttributes ra = getRegionAttributes();
            AttributesFactory factory =
              new AttributesFactory(ra);
            if(ra.getDataPolicy().withPersistence()) {
              factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
            } else {
              factory.setDataPolicy(DataPolicy.REPLICATE);
            }
            createRegion(name, factory.create());
          }
          finishCacheXml(name);
          // reset slow
          com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
          // if global scope, the region doesn't get destroyed until after region creation
          try {Thread.sleep(3000);} catch (InterruptedException ie) {fail("interrupted");}
          assertTrue(getRootRegion().getSubregion(name) == null || getRegionAttributes().getScope().isGlobal());
        }
      });
    if (getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      ThreadUtils.join(async, 30 * 1000);
      if (async.exceptionOccurred()) {
        com.gemstone.gemfire.test.dunit.Assert.fail("async invocation failed", async.getException());
      }

      vm2.invoke(new SerializableRunnable("Set fast image processing") {
          public void run() {
            com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
          }
        });
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("after async nonblocking ops complete");
    }

    // wait for GII to complete
    //getLogWriter().info("starting wait for GetInitialImage Completion");
    ThreadUtils.join(asyncGII, 30 * 1000);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Complete GetInitialImage at: " + System.currentTimeMillis());
    if (getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      ThreadUtils.join(async, 30 * 1000);
    }
    if (async.exceptionOccurred()) {
      com.gemstone.gemfire.test.dunit.Assert.fail("async failed", async.getException());
    }
    if (asyncGII.exceptionOccurred()) {
      com.gemstone.gemfire.test.dunit.Assert.fail("asyncGII failed", asyncGII.getException());
    }
    } finally { 
      ex.remove();
    }
  }

  /**
   * Tests what happens when one VM attempts to read an object for
   * which it does not have a registered <code>DataSerializer</code>.
   *
   * @since 3.5
   */
  public void testNoDataSerializer() {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": before creates");
    vm0.invoke(create);
    vm1.invoke(create);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after creates");

    final Object key = "KEY";
    final Object key2 = "KEY2";
    final int intValue = 3452;
    final long longValue = 213421;
//    final boolean[] wasInvoked = new boolean[1];

    vm2.invoke(new SerializableRunnable("Disconnect from DS") {
        public void run() {
          disconnectFromDS();
        }
      });
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after vm2 disconnect");

    try {
    vm0.invoke(new CacheSerializableRunnable("Put int") {
        public void run2() throws CacheException {
          Class c = IntWrapper.IntWrapperSerializer.class;
          IntWrapper.IntWrapperSerializer serializer =
            (IntWrapper.IntWrapperSerializer)
            DataSerializer.register(c);
          com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Registered serializer id:" + serializer.getId()
              + " class:" + c.getName());

          Region region = getRootRegion().getSubregion(name);
          region.put(key, new IntWrapper(intValue));

          flushIfNecessary(region);
          assertTrue(serializer.wasInvoked);
        }
      });
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after vm0 put");

    SerializableRunnable get = new CacheSerializableRunnable("Get int") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          // wait a while for the serializer to be registered
          // A race condition exists in the product in which
          // this thread can be stuck waiting in getSerializer
          // for 60 seconds. So it only calls getSerializer once
          // causing it to fail intermittently (see GEODE-376).
          // To workaround this the test wets WAIT_MS to 1 ms.
          // So the getSerializer will only block for 1 ms.
          // This allows the WaitCriterion to make multiple calls
          // of getSerializer and the subsequent calls will find
          // the DataSerializer.
          final int savVal = InternalDataSerializer.GetMarker.WAIT_MS;
          InternalDataSerializer.GetMarker.WAIT_MS = 1;
          try {
            WaitCriterion ev = new WaitCriterion() {
              public boolean done() {
                return InternalDataSerializer.getSerializer((byte)120) != null;
              }
              public String description() {
                return "DataSerializer with id 120 was never registered";
              }
            };
            Wait.waitForCriterion(ev, 30 * 1000, 10, true);
          } finally {
            InternalDataSerializer.GetMarker.WAIT_MS = savVal;
          }
          IntWrapper value = (IntWrapper) region.get(key);
          assertNotNull(InternalDataSerializer.getSerializer((byte)120));
          assertNotNull(value);
          assertEquals(intValue, value.intValue);
        }
      };
    vm1.invoke(get);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after vm1 get");

    // Make sure that VMs that connect after registration can get the
    // serializer
    vm2.invoke(new SerializableRunnable("Connect to DS") {
        public void run() {
          // Register a DataSerializer before connecting to system
          Class c = LongWrapper.LongWrapperSerializer.class;
          DataSerializer.register(c);

          getSystem();
        }
      });
    vm2.invoke(create);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after vm2 create");
    vm2.invoke(new CacheSerializableRunnable("Put long") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.put(key2, new LongWrapper(longValue));

          flushIfNecessary(region);

          LongWrapper.LongWrapperSerializer serializer =
            (LongWrapper.LongWrapperSerializer)
            InternalDataSerializer.getSerializer((byte) 121);
          assertTrue(serializer.wasInvoked);
        }
      });
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after vm2 put");
    vm2.invoke(get);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after vm2 get");

    SerializableRunnable get2 = new CacheSerializableRunnable("Get long") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          LongWrapper value = (LongWrapper) region.get(key2);
          assertNotNull(InternalDataSerializer.getSerializer((byte)121));
          assertNotNull(value);
          assertEquals(longValue, value.longValue);
        }
      };
    vm0.invoke(get2);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after vm0 get2");
    vm1.invoke(get2);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after vm1 get2");

    // wait a little while for other netsearch requests to return
    // before unregistering the serializers that will be needed to process these
    // responses.
    } finally {
    Wait.pause(1500);
    unregisterAllSerializers();
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(name + ": after unregister");
    }
  }

  /**
   * Tests what happens when one VM attempts to read an object for
   * which it does not have a registered <code>Instantiator</code>.
   *
   * @since 3.5
   */
  public void testNoInstantiator() {
    assertTrue(getRegionAttributes().getScope().isDistributed());

    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            createRegion(name);
          }
        };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm0.invoke(create);
    vm1.invoke(create);

    final Object key = "KEY";
    final Object key2 = "KEY2";
    final int intValue = 7201;
    final long longValue = 123612;
//    final boolean[] wasInvoked = new boolean[1];

    vm2.invoke(new SerializableRunnable("Disconnect from DS") {
        public void run() {
          disconnectFromDS();
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Put int") {
        public void run2() throws CacheException {
          Instantiator.register(new DSIntWrapper.DSIntWrapperInstantiator());

          Region region = getRootRegion().getSubregion(name);
          region.put(key, new DSIntWrapper(intValue));

          flushIfNecessary(region);
        }
      });

    SerializableRunnable get = new CacheSerializableRunnable("Get int") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          DSIntWrapper.DSIntWrapperInstantiator inst =
            new DSIntWrapper.DSIntWrapperInstantiator();
          assertNotNull(InternalInstantiator.getInstantiator(inst.getId()));
          DSIntWrapper value = (DSIntWrapper) region.get(key);
          assertNotNull(value);
          assertEquals(intValue, value.intValue);
        }
      };
    try {
    vm1.invoke(get);

    // Make sure that VMs that connect after registration can get the
    // serializer
    vm2.invoke(new SerializableRunnable("Connect to DS") {
        public void run() {
          // Register a Instantiator before connecting to system
          Instantiator.register(new DSLongWrapper.DSLongWrapperInstantiator());

          getSystem();
        }
      });
    vm2.invoke(create);
    vm2.invoke(new CacheSerializableRunnable("Put long") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.put(key2, new DSLongWrapper(longValue));

          flushIfNecessary(region);
        }
      });
    vm2.invoke(get);

    SerializableRunnable get2 = new CacheSerializableRunnable("Get long") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          DSLongWrapper.DSLongWrapperInstantiator inst =
            new DSLongWrapper.DSLongWrapperInstantiator();
          assertNotNull(InternalInstantiator.getInstantiator(inst.getId()));
          LongWrapper value = (LongWrapper) region.get(key2);
          assertNotNull(value);
          assertEquals(longValue, value.longValue);

          inst = (DSLongWrapper.DSLongWrapperInstantiator)
            InternalInstantiator.getInstantiator(inst.getId());
          assertNotNull(inst);
          assertTrue(inst.wasInvoked);
        }
      };
    vm0.invoke(get2);
    vm1.invoke(get2);

    } finally {
    // wait a little while for other netsearch requests to return
    // before unregistering the serializers that will be needed to process these
    // responses.
    Wait.pause(1500);
    unregisterAllSerializers();
    }
  }

  /**
   * Unregisters all of the <code>DataSerializer</code>s and
   * <code>Instantiators</code> in all of the VMs in the distributed
   * system.
   */
  private static void unregisterAllSerializers() {
    DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
    cleanupAllVms();
  }

  /**
   * A class used when testing <code>DataSerializable</code> and
   * distribution.
   */
  static class IntWrapper {
    int intValue;

    IntWrapper(int intValue) {
      this.intValue = intValue;
    }

    public boolean equals(Object o) {
      if (o instanceof IntWrapper) {
        return ((IntWrapper) o).intValue == this.intValue;

      } else {
        return false;
      }
    }

    static class IntWrapperSerializer extends DataSerializer {
      boolean wasInvoked = false;

      public int getId() {
        return 120;
      }
      public Class[] getSupportedClasses() {
        return new Class[] { IntWrapper.class };
      }

      public boolean toData(Object o, DataOutput out)
        throws IOException {
        if (o instanceof IntWrapper) {
          this.wasInvoked = true;
          IntWrapper iw = (IntWrapper) o;
          out.writeInt(iw.intValue);
          return true;

        } else {
          return false;
        }
      }

      public Object fromData(DataInput in)
        throws IOException, ClassNotFoundException {

        return new IntWrapper(in.readInt());
      }
    }
  }

  /**
   * An <code>IntWrapper</code> that is
   * <code>DataSerializable</code>.
   */
  static class DSIntWrapper extends IntWrapper
    implements DataSerializable {

    DSIntWrapper(int intValue) {
      super(intValue);
    }

    protected DSIntWrapper() {
      super(0);
    }

    public void toData(DataOutput out) throws IOException {
      out.writeInt(intValue);
    }

    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      this.intValue = in.readInt();
    }

    static class DSIntWrapperInstantiator extends Instantiator {
      DSIntWrapperInstantiator() {
        this(DSIntWrapper.class, (byte) 76);
      }

      DSIntWrapperInstantiator(Class c, byte id) {
        super(c, id);
      }

      public DataSerializable newInstance() {
        return new DSIntWrapper();
      }
    }
  }

  /**
   * Another class used when testing <code>DataSerializable</code> and
   * distribution.
   */
  static class LongWrapper {
    long longValue;

    LongWrapper(long longValue) {
      this.longValue = longValue;
    }

    public boolean equals(Object o) {
      if (o instanceof LongWrapper) {
        return ((LongWrapper) o).longValue == this.longValue;

      } else {
        return false;
      }
    }

    static class LongWrapperSerializer extends DataSerializer {
      boolean wasInvoked = false;

      public int getId() {
        return 121;
      }
      public Class[] getSupportedClasses() {
        return new Class[] { LongWrapper.class };
      }

      public boolean toData(Object o, DataOutput out)
        throws IOException {
        if (o instanceof LongWrapper) {
          this.wasInvoked = true;
          LongWrapper iw = (LongWrapper) o;
          out.writeLong(iw.longValue);
          return true;

        } else {
          return false;
        }
      }

      public Object fromData(DataInput in)
        throws IOException, ClassNotFoundException {

        return new LongWrapper(in.readLong());
      }
    }
  }

  /**
   * An <code>LongWrapper</code> that is
   * <code>DataSerializable</code>.
   */
  static class DSLongWrapper extends LongWrapper
    implements DataSerializable {

    DSLongWrapper(long longValue) {
      super(longValue);
    }

    protected DSLongWrapper() {
      super(0L);
    }

    public void toData(DataOutput out) throws IOException {
      out.writeLong(longValue);
    }

    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      this.longValue = in.readLong();
    }

    static class DSLongWrapperInstantiator extends Instantiator {
      boolean wasInvoked = false;

      DSLongWrapperInstantiator(Class c, byte id) {
        super(c, id);
      }

      DSLongWrapperInstantiator() {
        this(DSLongWrapper.class, (byte) 99);
      }

      public DataSerializable newInstance() {
        this.wasInvoked = true;
        return new DSLongWrapper();
      }
    }
  }

  public class MyTransactionListener implements TransactionListener {
    public volatile TransactionId expectedTxId;
    public volatile TransactionEvent lastEvent;
    public volatile int afterCommitCount;
    public volatile int afterFailedCommitCount;
    public volatile int afterRollbackCount;
    public volatile int closeCount;

    public void afterCommit(TransactionEvent event) {
      this.lastEvent = event;
      this.afterCommitCount++;
//       getSystem().getLogWriter().info("TXListener("
//                                       + System.identityHashCode(this)
//                                       + "): afterCommit "
//                                       + event.getTransactionId()
//                                       + " #" + this.afterCommitCount);
    }

    public void afterFailedCommit(TransactionEvent event) {
      this.lastEvent = event;
      this.afterFailedCommitCount++;
//       getSystem().getLogWriter().info("TXListener("
//                                       + System.identityHashCode(this)
//                                       + "): afterFailCommit "
//                                       + event.getTransactionId()
//                                       + " #" + this.afterFailedCommitCount);
    }

    public void afterRollback(TransactionEvent event) {
      this.lastEvent = event;
      this.afterRollbackCount++;
//       getSystem().getLogWriter().info("TXListener("
//                                       + System.identityHashCode(this)
//                                       + "): afterRollback "
//                                       + event.getTransactionId()
//                                       + " #" + this.afterRollbackCount);
    }
    public void close() {
      this.closeCount++;
    }
    public void reset() {
//       getSystem().getLogWriter().info("TXListener("
//                                       + System.identityHashCode(this)
//                                       + "): resetting TX listener");
      this.afterCommitCount = 0;
      this.afterFailedCommitCount = 0;
      this.afterRollbackCount = 0;
      this.closeCount = 0;
    }
    public void checkAfterCommitCount(int expected) {
//       if (this.afterCommitCount != expected) {
//         getSystem().getLogWriter().info("TXListener("
//                                         + System.identityHashCode(this)
//                                         + "):checkAfterCommitCount "
//                                         + " #" + this.afterCommitCount);
//       }
      assertEquals(expected, this.afterCommitCount);
    }
    public void assertCounts(int commitCount, int failedCommitCount,
                             int rollbackCount, int closeCount1) {
      assertEquals(commitCount, this.afterCommitCount);
      assertEquals(failedCommitCount, this.afterFailedCommitCount);
      assertEquals(rollbackCount, this.afterRollbackCount);
      assertEquals(closeCount1, this.closeCount);
    }
  }

  class CountingDistCacheListener extends CacheListenerAdapter {
    int aCreateCalls, aUpdateCalls, aInvalidateCalls, aDestroyCalls, aLocalDestroyCalls, regionOps;
    EntryEvent lastEvent;
    public void close() {}
    public synchronized void reset() {
      this.aCreateCalls = this.aUpdateCalls = this.aInvalidateCalls = this.aDestroyCalls = this.regionOps = 0;
      this.lastEvent = null;
    }
    public void afterCreate(EntryEvent e) {if (e.isOriginRemote()) synchronized(this) {++this.aCreateCalls; this.lastEvent = e;}}
    public void afterUpdate(EntryEvent e) {if (e.isOriginRemote()) synchronized(this) {++this.aUpdateCalls;this.lastEvent = e;}}
    public void afterInvalidate(EntryEvent e) {if (e.isOriginRemote()) synchronized(this) {++this.aInvalidateCalls;this.lastEvent = e;}}
    public void afterDestroy(EntryEvent e) {if (e.isOriginRemote()) synchronized(this) {++this.aDestroyCalls;this.lastEvent = e;}}
    public void afterRegionInvalidate(RegionEvent e) {++this.regionOps;}
    public void afterRegionDestroy(RegionEvent e) {++this.regionOps;}
    public synchronized void assertCount(int expectedCreate, int expectedUpdate,
                                         int expectedInvalidate, int expectedDestroy) {
      assertEquals(expectedCreate, this.aCreateCalls);
      assertEquals(expectedUpdate, this.aUpdateCalls);
      assertEquals(expectedInvalidate, this.aInvalidateCalls);
      assertEquals(expectedDestroy, this.aDestroyCalls);
      assertEquals(0, this.regionOps);
    }
    public synchronized EntryEvent getEntryEvent() {return this.lastEvent;}
    public synchronized void setEntryEvent(EntryEvent event) {this.lastEvent = event;}
  }
  public static void assertCacheCallbackEvents(String regionName, TransactionId txId,
                                               Object key, Object oldValue, Object newValue) {
    Cache johnnyCash = CacheFactory.getAnyInstance();
    Region re = johnnyCash.getRegion("root").getSubregion(regionName);
    MyTransactionListener tl =
      (MyTransactionListener) johnnyCash.getCacheTransactionManager().getListeners()[0];
    tl.expectedTxId = txId;
    assertNotNull("Cannot assert TX Callout Events with a null Region: " + regionName, re);
    final CountingDistCacheListener cdcl =
      (CountingDistCacheListener) re.getAttributes().getCacheListeners()[0];
    // May need to wait a bit for the event to be received
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return cdcl.getEntryEvent() != null;
      }
      public String description() {
        return "waiting for entry event";
      }
    };
    Wait.waitForCriterion(ev, 60 * 1000, 200, true);
    EntryEvent listenEvent = cdcl.getEntryEvent();
    assertNotNull("Cannot assert TX CacheListener Events with a null Entry Event", listenEvent);
    assertEquals(re, listenEvent.getRegion());
    assertEquals(txId, listenEvent.getTransactionId());
    assertEquals(key, listenEvent.getKey());
    assertEquals(oldValue, listenEvent.getOldValue());
    assertEquals(newValue, listenEvent.getNewValue());
    assertEquals(null, listenEvent.getCallbackArgument());
    assertEquals(true, listenEvent.isCallbackArgumentAvailable());
    assertTrue(!listenEvent.getOperation().isLoad());
    assertTrue(!listenEvent.getOperation().isNetLoad());
    assertTrue(!listenEvent.getOperation().isNetSearch());
    assertTrue(!listenEvent.getOperation().isLocalLoad());
    assertTrue(listenEvent.getOperation().isDistributed());
    assertTrue(!listenEvent.getOperation().isExpiration());
    assertTrue(listenEvent.isOriginRemote());
    cdcl.setEntryEvent(null);
  }

  ////////////////////////// TX Tests //////////////////////////////
  /**
   * Tests that an entry update is propagated to other caches that
   * have that same entry defined.
   */
  public void testTXSimpleOps() throws Exception {
    if (!supportsTransactions()) {
      return;
    }
    assertTrue(getRegionAttributes().getScope().isDistributed());
    CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();

    if (getRegionAttributes().getScope().isGlobal()
        || getRegionAttributes().getDataPolicy().withPersistence()) {
      // just make sure transactions are not allowed on global or shared regions
      Region rgn = createRegion(getUniqueName());
      txMgr.begin();
      try {
        rgn.put("testTXSimpleOpsKey1", "val");
        fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException ok) {
      }
      txMgr.rollback();
      rgn.localDestroyRegion();
      return;
    }
    final String rgnName = getUniqueName();

    SerializableRunnable create = new SerializableRunnable("testTXSimpleOps: Create Region") {
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        txMgr2.addListener(tl);
        assertEquals(null, tl.lastEvent);
        assertEquals(0, tl.afterCommitCount);
        assertEquals(0, tl.afterFailedCommitCount);
        assertEquals(0, tl.afterRollbackCount);
        assertEquals(0, tl.closeCount);
        try {
          Region rgn = createRegion(rgnName);
          CountingDistCacheListener cacheListener = new CountingDistCacheListener();
          rgn.getAttributesMutator().addCacheListener(cacheListener);
          cacheListener.assertCount(0, 0, 0, 0);
          getSystem().getLogWriter().info("testTXSimpleOps: Created region");
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };

    SerializableRunnable newKey = new SerializableRunnable("testTXSimpleOps: Create Region & Create Key") {
      public void run() {
        try {
          Region root = getRootRegion();
          Region rgn = root.getSubregion(rgnName);
          rgn.create("key", null);
          CountingDistCacheListener cacheListener = (CountingDistCacheListener)
              rgn.getAttributes().getCacheListener();
          cacheListener.assertCount(0, 0, 0, 0);
          getSystem().getLogWriter().info("testTXSimpleOps: Created Key");
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };

    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    vm.invoke(create);
    vm.invoke(newKey);
    int vmCount = host.getVMCount();
    for (int i = 1; i < vmCount; i++) {
      vm = host.getVM(i);
      vm.invoke(create);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().isPreloaded()
          ) {
        vm.invoke(newKey);
      }
    }
//GemFireVersion.waitForJavaDebugger(getLogWriter(), "CTRLR WAITING AFTER CREATE");

    try {
      Region rgn = createRegion(rgnName);
      DMStats dmStats = getSystem().getDistributionManager().getStats();
      long cmtMsgs = dmStats.getSentCommitMessages();
      long commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn.put("key", "value");
      TransactionId txId = txMgr.getTransactionId();
      txMgr.commit();
      assertEquals(cmtMsgs+1, dmStats.getSentCommitMessages());
      if (rgn.getAttributes().getScope().isAck()) {
        assertEquals(commitWaits+1, dmStats.getCommitWaits());
      } else {
        assertEquals(commitWaits, dmStats.getCommitWaits());
      }
      getSystem().getLogWriter().info("testTXSimpleOps: Create/Put Value");
      Invoke.invokeInEveryVM(MultiVMRegionTestCase.class,
                      "assertCacheCallbackEvents",
                      new Object[] {rgnName, txId, "key", null, "value"});
      Invoke.invokeInEveryVMRepeatingIfNecessary(new CacheSerializableRunnable("testTXSimpleOps: Verify Received Value") {
        public void run2() {
          Region rgn1 = getRootRegion().getSubregion(rgnName);
          assertNotNull("Could not find entry for 'key'", rgn1.getEntry("key"));
          assertEquals("value", rgn1.getEntry("key").getValue());
          CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
          MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
          tl.checkAfterCommitCount(1);
          assertEquals(0, tl.afterFailedCommitCount);
          assertEquals(0, tl.afterRollbackCount);
          assertEquals(0, tl.closeCount);
          assertEquals(rgn1.getCache(), tl.lastEvent.getCache());
          {
            Collection events;
            RegionAttributes attr = getRegionAttributes();
            if (!attr.getDataPolicy().withReplication() ||
                attr.getConcurrencyChecksEnabled()) {
              events = tl.lastEvent.getPutEvents();
            } else {
              events = tl.lastEvent.getCreateEvents();
            }
            assertEquals(1, events.size());
            EntryEvent ev = (EntryEvent)events.iterator().next();
            assertEquals(tl.expectedTxId, ev.getTransactionId());
            assertTrue(ev.getRegion() == rgn1);
            assertEquals("key", ev.getKey());
            assertEquals("value", ev.getNewValue());
            assertEquals(null, ev.getOldValue());
            assertTrue(!ev.getOperation().isLocalLoad());
            assertTrue(!ev.getOperation().isNetLoad());
            assertTrue(!ev.getOperation().isLoad());
            assertTrue(!ev.getOperation().isNetSearch());
            assertTrue(!ev.getOperation().isExpiration());
            assertEquals(null, ev.getCallbackArgument());
            assertEquals(true, ev.isCallbackArgumentAvailable());
            assertTrue(ev.isOriginRemote());
            assertTrue(ev.getOperation().isDistributed());
          }
          CountingDistCacheListener cdcL =
            (CountingDistCacheListener) rgn1.getAttributes().getCacheListeners()[0];
          cdcL.assertCount(0, 1, 0, 0);

        }
      }, getRepeatTimeoutMs());

      txMgr.begin();
      rgn.put("key", "value2");
      txId = txMgr.getTransactionId();
      txMgr.commit();
      getSystem().getLogWriter().info("testTXSimpleOps: Put(update) Value2");
      Invoke.invokeInEveryVM(MultiVMRegionTestCase.class,
                      "assertCacheCallbackEvents",
                      new Object[] {rgnName, txId, "key", "value", "value2"});
      Invoke.invokeInEveryVMRepeatingIfNecessary(new CacheSerializableRunnable("testTXSimpleOps: Verify Received Value") {
        public void run2() {
          Region rgn1 = getRootRegion().getSubregion(rgnName);
          assertNotNull("Could not find entry for 'key'", rgn1.getEntry("key"));
          assertEquals("value2", rgn1.getEntry("key").getValue());
          CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
          MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
          tl.checkAfterCommitCount(2);
          assertEquals(rgn1.getCache(), tl.lastEvent.getCache());
          {
            Collection events = tl.lastEvent.getPutEvents();
            assertEquals(1, events.size());
            EntryEvent ev = (EntryEvent)events.iterator().next();
            assertEquals(tl.expectedTxId, ev.getTransactionId());
            assertTrue(ev.getRegion() == rgn1);
            assertEquals("key", ev.getKey());
            assertEquals("value2", ev.getNewValue());
            assertEquals("value", ev.getOldValue());
            assertTrue(!ev.getOperation().isLocalLoad());
            assertTrue(!ev.getOperation().isNetLoad());
            assertTrue(!ev.getOperation().isLoad());
            assertTrue(!ev.getOperation().isNetSearch());
            assertTrue(!ev.getOperation().isExpiration());
            assertEquals(null, ev.getCallbackArgument());
            assertEquals(true, ev.isCallbackArgumentAvailable());
            assertTrue(ev.isOriginRemote());
            assertTrue(ev.getOperation().isDistributed());
          }
          CountingDistCacheListener cdcL =
            (CountingDistCacheListener) rgn1.getAttributes().getCacheListeners()[0];
          cdcL.assertCount(0, 2, 0, 0);

        }
      }, getRepeatTimeoutMs());

      txMgr.begin();
      rgn.invalidate("key");
      txId = txMgr.getTransactionId();
      txMgr.commit();
      getSystem().getLogWriter().info("testTXSimpleOps: invalidate key");
      // validate each of the CacheListeners EntryEvents
      Invoke.invokeInEveryVM(MultiVMRegionTestCase.class,
                      "assertCacheCallbackEvents",
                      new Object[] {rgnName, txId, "key", "value2", null});
      Invoke.invokeInEveryVMRepeatingIfNecessary(new CacheSerializableRunnable("testTXSimpleOps: Verify Received Value") {
        public void run2() {
          Region rgn1 = getRootRegion().getSubregion(rgnName);
          assertNotNull("Could not find entry for 'key'", rgn1.getEntry("key"));
          assertTrue(rgn1.containsKey("key"));
          assertTrue(!rgn1.containsValueForKey("key"));
          CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
          MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
          tl.checkAfterCommitCount(3);
          assertEquals(rgn1.getCache(), tl.lastEvent.getCache());
          {
            Collection events = tl.lastEvent.getInvalidateEvents();
            assertEquals(1, events.size());
            EntryEvent ev = (EntryEvent)events.iterator().next();
            assertEquals(tl.expectedTxId, ev.getTransactionId());
            assertTrue(ev.getRegion() == rgn1);
            assertEquals("key", ev.getKey());
            assertEquals(null, ev.getNewValue());
            assertEquals("value2", ev.getOldValue());
            assertTrue(!ev.getOperation().isLocalLoad());
            assertTrue(!ev.getOperation().isNetLoad());
            assertTrue(!ev.getOperation().isLoad());
            assertTrue(!ev.getOperation().isNetSearch());
            assertTrue(!ev.getOperation().isExpiration());
            assertEquals(null, ev.getCallbackArgument());
            assertEquals(true, ev.isCallbackArgumentAvailable());
            assertTrue(ev.isOriginRemote());
            assertTrue(ev.getOperation().isDistributed());
          }
          CountingDistCacheListener cdcL =
            (CountingDistCacheListener) rgn1.getAttributes().getCacheListeners()[0];
          cdcL.assertCount(0, 2, 1, 0);

        }
      }, getRepeatTimeoutMs());

      txMgr.begin();
      rgn.destroy("key");
      txId = txMgr.getTransactionId();
      txMgr.commit();
      getSystem().getLogWriter().info("testTXSimpleOps: destroy key");
      // validate each of the CacheListeners EntryEvents
      Invoke.invokeInEveryVM(MultiVMRegionTestCase.class,
                      "assertCacheCallbackEvents",
                      new Object[] {rgnName, txId, "key", null, null});
      Invoke.invokeInEveryVMRepeatingIfNecessary(new CacheSerializableRunnable("testTXSimpleOps: Verify Received Value") {
        public void run2() {
          Region rgn1 = getRootRegion().getSubregion(rgnName);
          assertTrue(!rgn1.containsKey("key"));
          CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
          MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
          tl.checkAfterCommitCount(4);
          assertEquals(rgn1.getCache(), tl.lastEvent.getCache());
          {
            Collection events = tl.lastEvent.getDestroyEvents();
            assertEquals(1, events.size());
            EntryEvent ev = (EntryEvent)events.iterator().next();
            assertEquals(tl.expectedTxId, ev.getTransactionId());
            assertTrue(ev.getRegion() == rgn1);
            assertEquals("key", ev.getKey());
            assertEquals(null, ev.getNewValue());
            assertEquals(null, ev.getOldValue());
            assertTrue(!ev.getOperation().isLocalLoad());
            assertTrue(!ev.getOperation().isNetLoad());
            assertTrue(!ev.getOperation().isLoad());
            assertTrue(!ev.getOperation().isNetSearch());
            assertTrue(!ev.getOperation().isExpiration());
            assertEquals(null, ev.getCallbackArgument());
            assertEquals(true, ev.isCallbackArgumentAvailable());
            assertTrue(ev.isOriginRemote());
            assertTrue(ev.getOperation().isDistributed());
          }
          CountingDistCacheListener cdcL =
            (CountingDistCacheListener) rgn1.getAttributes().getCacheListeners()[0];
          cdcL.assertCount(0, 2, 1, 1);
        }
      }, getRepeatTimeoutMs());

    }
    catch(Exception e) {
      CacheFactory.getInstance(getSystem()).close();
      getSystem().getLogWriter().fine("testTXSimpleOps: Caused exception in createRegion");
      throw e;
    }

  }

  /**
   * Indicate whether this region supports transactions
   * @return true if it supports transactions
   */
  protected boolean supportsTransactions() {
    return true;
  }
  
  /**
   * Tests that the push of a loaded value does not cause a conflict
   * on the side receiving the update
   */
  public void testTXUpdateLoadNoConflict() throws Exception {
    /*
     * this no longer holds true - we have load conflicts now
     * 
     */
    if(true) {
      return;
    }
    
    if (!supportsTransactions()) {
      return;
    }
    assertTrue(getRegionAttributes().getScope().isDistributed());
    CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();

    if (getRegionAttributes().getScope().isGlobal()
        || getRegionAttributes().getDataPolicy().withPersistence()) {
      return;
    }
    final String rgnName = getUniqueName();

    SerializableRunnable create = new SerializableRunnable("testTXUpdateLoadNoConflict: Create Region & Load value") {
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        txMgr2.addListener(tl);
        try {
          Region rgn = createRegion(rgnName);
          AttributesMutator mutator = rgn.getAttributesMutator();
          mutator.setCacheLoader(new CacheLoader() {
              int count = 0;
              public Object load(LoaderHelper helper)
                throws CacheLoaderException
              {
                count++;
                return "LV " + count;

              }
              public void close() {}
            });
          Object value = rgn.get("key");
          assertEquals("LV 1", value);
          getSystem().getLogWriter().info("testTXUpdateLoadNoConflict: loaded Key");
          flushIfNecessary(rgn);
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };

    VM vm0 = Host.getHost(0).getVM(0);
    // GemFireVersion.waitForJavaDebugger(getLogWriter(), "CTRLR WAITING AFTER CREATE");

    try {
      MyTransactionListener tl = new MyTransactionListener();
      txMgr.addListener(tl);
      AttributesFactory rgnAtts = new AttributesFactory(getRegionAttributes());
      rgnAtts.setDataPolicy(DataPolicy.REPLICATE);
      Region rgn = createRegion(rgnName, rgnAtts.create());

      txMgr.begin();
      TransactionId myTXId = txMgr.getTransactionId();

      rgn.create("key", "txValue");

      vm0.invoke(create);

      {
        TXStateProxy tx = ((TXManagerImpl)txMgr).internalSuspend();
        assertTrue(rgn.containsKey("key"));
        assertEquals("LV 1", rgn.getEntry("key").getValue());
        ((TXManagerImpl)txMgr).resume(tx);
      }
      // make sure transactional view is still correct
      assertEquals("txValue", rgn.getEntry("key").getValue());

      txMgr.commit();
      getSystem().getLogWriter().info("testTXUpdateLoadNoConflict: did commit");
      assertEquals("txValue", rgn.getEntry("key").getValue());
      {
        Collection events = tl.lastEvent.getCreateEvents();
        assertEquals(1, events.size());
        EntryEvent ev = (EntryEvent)events.iterator().next();
        assertEquals(myTXId, ev.getTransactionId());
        assertTrue(ev.getRegion() == rgn);
        assertEquals("key", ev.getKey());
        assertEquals("txValue", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        assertTrue(!ev.getOperation().isLocalLoad());
        assertTrue(!ev.getOperation().isNetLoad());
        assertTrue(!ev.getOperation().isLoad());
        assertTrue(!ev.getOperation().isNetSearch());
        assertTrue(!ev.getOperation().isExpiration());
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(ev.getOperation().isDistributed());
      }

      // Now setup recreate the region in the controller with NONE
      // so test can do local destroys.
      rgn.localDestroyRegion();
      rgnAtts.setDataPolicy(DataPolicy.NORMAL);
      rgn = createRegion(rgnName, rgnAtts.create());

      // now see if net loader is working
      Object v2 = rgn.get("key2");
      assertEquals("LV 2", v2);

      // now confirm that netload does not cause a conflict
      txMgr.begin();
      myTXId = txMgr.getTransactionId();

      rgn.create("key3", "txValue3");

      {
        TXStateProxy tx = ((TXManagerImpl)txMgr).internalSuspend();
        // do a get outside of the transaction to force a net load
        Object v3 = rgn.get("key3");
        assertEquals("LV 3", v3);
        ((TXManagerImpl)txMgr).resume(tx);
      }
      // make sure transactional view is still correct
      assertEquals("txValue3", rgn.getEntry("key3").getValue());

      txMgr.commit();
      getSystem().getLogWriter().info("testTXUpdateLoadNoConflict: did commit");
      assertEquals("txValue3", rgn.getEntry("key3").getValue());
      {
        Collection events = tl.lastEvent.getCreateEvents();
        assertEquals(1, events.size());
        EntryEvent ev = (EntryEvent)events.iterator().next();
        assertEquals(myTXId, ev.getTransactionId());
        assertTrue(ev.getRegion() == rgn);
        assertEquals("key3", ev.getKey());
        assertEquals("txValue3", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        assertTrue(!ev.getOperation().isLocalLoad());
        assertTrue(!ev.getOperation().isNetLoad());
        assertTrue(!ev.getOperation().isLoad());
        assertTrue(!ev.getOperation().isNetSearch());
        assertTrue(!ev.getOperation().isExpiration());
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(ev.getOperation().isDistributed());
      }

      // now see if tx net loader is working

      // now confirm that netload does not cause a conflict
      txMgr.begin();
      myTXId = txMgr.getTransactionId();
      Object v4 = rgn.get("key4");
      assertEquals("LV 4", v4);
      assertEquals("LV 4", rgn.get("key4"));
      assertEquals("LV 4", rgn.getEntry("key4").getValue());
      txMgr.rollback();
      // confirm that netLoad is transactional
      assertEquals("LV 5", rgn.get("key4"));
      assertEquals("LV 5", rgn.getEntry("key4").getValue());

      // make sure non-tx netsearch works
      assertEquals("txValue", rgn.get("key"));
      assertEquals("txValue", rgn.getEntry("key").getValue());

      // make sure net-search result does not conflict with commit
      rgn.localInvalidate("key");
      txMgr.begin();
      myTXId = txMgr.getTransactionId();

      rgn.put("key", "new txValue");

      {
        TXStateProxy tx = ((TXManagerImpl)txMgr).internalSuspend();
        // do a get outside of the transaction to force a netsearch
        assertEquals("txValue", rgn.get("key")); // does a netsearch
        assertEquals("txValue", rgn.getEntry("key").getValue());
        ((TXManagerImpl)txMgr).resume(tx);
      }
      // make sure transactional view is still correct
      assertEquals("new txValue", rgn.getEntry("key").getValue());

      txMgr.commit();
      flushIfNecessary(rgn); // give other side change to process commit
      getSystem().getLogWriter().info("testTXUpdateLoadNoConflict: did commit");
      assertEquals("new txValue", rgn.getEntry("key").getValue());
      {
        Collection events = tl.lastEvent.getPutEvents();
        assertEquals(1, events.size());
        EntryEvent ev = (EntryEvent)events.iterator().next();
        assertEquals(myTXId, ev.getTransactionId());
        assertTrue(ev.getRegion() == rgn);
        assertEquals("key", ev.getKey());
        assertEquals("new txValue", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        assertTrue(!ev.getOperation().isLocalLoad());
        assertTrue(!ev.getOperation().isNetLoad());
        assertTrue(!ev.getOperation().isLoad());
        assertTrue(!ev.getOperation().isNetSearch());
        assertTrue(!ev.getOperation().isExpiration());
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(ev.getOperation().isDistributed());
      }


      // make sure tx local invalidate allows netsearch
      Object localCmtValue = rgn.getEntry("key").getValue();
      txMgr.begin();
      assertSame(localCmtValue, rgn.getEntry("key").getValue());
      rgn.localInvalidate("key");
      assertNull(rgn.getEntry("key").getValue());
      // now make sure a get will do a netsearch and find the value
      // in the other vm instead of the one in local cmt state
      Object txValue = rgn.get("key");
      assertNotSame(localCmtValue, txValue);
      assertSame(txValue, rgn.get("key"));
      assertNotSame(localCmtValue, rgn.getEntry("key").getValue());
      // make sure we did a search and not a load
      assertEquals(localCmtValue, rgn.getEntry("key").getValue());
      // now make sure that if we do a tx distributed invalidate
      // that we will do a load and not a search
      rgn.invalidate("key");
      assertNull(rgn.getEntry("key").getValue());
      txValue = rgn.get("key");
      assertEquals("LV 6", txValue);
      assertSame(txValue, rgn.get("key"));
      assertEquals("LV 6", rgn.getEntry("key").getValue());
      // now make sure after rollback that local cmt state has not changed
      txMgr.rollback();
      assertSame(localCmtValue, rgn.getEntry("key").getValue());
    }
    catch(Exception e) {
      CacheFactory.getInstance(getSystem()).close();
      getSystem().getLogWriter().fine("testTXUpdateLoadNoConflict: Caused exception in createRegion");
      throw e;
    }

  }

  public void testTXMultiRegion() throws Exception {
    if (!supportsTransactions()) {
      return;
    }
    assertTrue(getRegionAttributes().getScope().isDistributed());
    CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();

    if (getRegionAttributes().getScope().isGlobal()
        || getRegionAttributes().getDataPolicy().withPersistence()) {
      return;
    }
    final String rgnName1 = getUniqueName() + "MR1";
    final String rgnName2 = getUniqueName() + "MR2";
    final String rgnName3 = getUniqueName() + "MR3";

    SerializableRunnable create1 = new SerializableRunnable("testTXMultiRegion: Create Region") {
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        txMgr2.setListener(tl);
        try {
          createRegion(rgnName1);
          getSystem().getLogWriter().info("testTXMultiRegion: Created region1");
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };
    SerializableRunnable newKey1 = new SerializableRunnable("testTXMultiRegion: Create Key") {
      public void run() {
        try {
          Region rgn = getRootRegion("root").getSubregion(rgnName1);
          rgn.create("key", null);
          getSystem().getLogWriter().info("testTXMultiRegion: Created key");
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };
    
    SerializableRunnable create2 = new SerializableRunnable("testTXMultiRegion: Create Region") {
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        txMgr2.setListener(tl);
        try {
          createRegion(rgnName2);
          getSystem().getLogWriter().info("testTXMultiRegion: Created region2");
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };
    SerializableRunnable newKey2 = new SerializableRunnable("testTXMultiRegion: Create Key") {
      public void run() {
        try {
          Region root = getRootRegion("root");
          Region rgn = root.getSubregion(rgnName2);
          rgn.create("key", null);
          getSystem().getLogWriter().info("testTXMultiRegion: Created Key");
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };
    
    SerializableRunnable create3 = new SerializableRunnable("testTXMultiRegion: Create Region") {
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        txMgr2.setListener(tl);
        try {
          createRegion(rgnName3);
          getSystem().getLogWriter().info("testTXMultiRegion: Created Region");
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };
    SerializableRunnable newKey3 = new SerializableRunnable("testTXMultiRegion: Create Key") {
      public void run() {
        try {
          Region root = getRootRegion("root");
          Region rgn = root.getSubregion(rgnName3);
          rgn.create("key", null);
          getSystem().getLogWriter().info("testTXMultiRegion: Created Key");
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };

    SerializableRunnable check1_3 = new SerializableRunnable("testTXMultiRegion: check") {
      public void run() {
        Region rgn1 = getRootRegion().getSubregion(rgnName1);
        {
          assertNotNull("Could not find entry for 'key'", rgn1.getEntry("key"));
          assertEquals("value1", rgn1.getEntry("key").getValue());
        }
        Region rgn3 = getRootRegion().getSubregion(rgnName3);
        {
          assertNotNull("Could not find entry for 'key'", rgn3.getEntry("key"));
          assertEquals("value3", rgn3.getEntry("key").getValue());
        }
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
        tl.checkAfterCommitCount(1);
        assertEquals(0, tl.afterFailedCommitCount);
        assertEquals(0, tl.afterRollbackCount);
        assertEquals(0, tl.closeCount);
        assertEquals(rgn1.getCache(), tl.lastEvent.getCache());
        {
          Collection events;
          RegionAttributes attr = getRegionAttributes();
          if (!attr.getDataPolicy().withReplication() ||
              attr.getConcurrencyChecksEnabled()) {
            events = tl.lastEvent.getPutEvents();
          } else {
            events = tl.lastEvent.getCreateEvents();
          }
          assertEquals(2, events.size());
          ArrayList eventList = new ArrayList(events);
          Collections.sort(eventList, new Comparator() {
              public int compare(Object o1, Object o2) {
                EntryEvent e1 = (EntryEvent)o1;
                EntryEvent e2 = (EntryEvent)o2;
                String s1 = e1.getRegion().getFullPath() + e1.getKey();
                String s2 = e2.getRegion().getFullPath() + e2.getKey();
                return s1.compareTo(s2);
            }});
          Iterator it = eventList.iterator();
          EntryEvent ev;

          ev = (EntryEvent)it.next();
          assertSame(rgn1, ev.getRegion());
          //assertEquals(tl.expectedTxId, ev.getTransactionId());
          assertEquals("key", ev.getKey());
          assertEquals("value1", ev.getNewValue());
          assertEquals(null, ev.getOldValue());
          assertTrue(!ev.getOperation().isLocalLoad());
          assertTrue(!ev.getOperation().isNetLoad());
          assertTrue(!ev.getOperation().isLoad());
          assertTrue(!ev.getOperation().isNetSearch());
          assertTrue(!ev.getOperation().isExpiration());
          assertEquals(null, ev.getCallbackArgument());
          assertEquals(true, ev.isCallbackArgumentAvailable());
          assertTrue(ev.isOriginRemote());
          assertTrue(ev.getOperation().isDistributed());

          ev = (EntryEvent)it.next();
          assertSame(rgn3, ev.getRegion());
          //assertEquals(tl.expectedTxId, ev.getTransactionId());
          assertEquals("key", ev.getKey());
          assertEquals("value3", ev.getNewValue());
          assertEquals(null, ev.getOldValue());
          assertTrue(!ev.getOperation().isLocalLoad());
          assertTrue(!ev.getOperation().isNetLoad());
          assertTrue(!ev.getOperation().isLoad());
          assertTrue(!ev.getOperation().isNetSearch());
          assertTrue(!ev.getOperation().isExpiration());
          assertEquals(null, ev.getCallbackArgument());
          assertEquals(true, ev.isCallbackArgumentAvailable());
          assertTrue(ev.isOriginRemote());
          assertTrue(ev.getOperation().isDistributed());
        }
      }
    };
    SerializableRunnable check2_3 = new SerializableRunnable("testTXMultiRegion: check") {
      public void run() {
        Region rgn2 = getRootRegion().getSubregion(rgnName2);
        {
          assertNotNull("Could not find entry for 'key'", rgn2.getEntry("key"));
          assertEquals("value2", rgn2.getEntry("key").getValue());
        }
        Region rgn3 = getRootRegion().getSubregion(rgnName3);
        {
          assertNotNull("Could not find entry for 'key'", rgn3.getEntry("key"));
          assertEquals("value3", rgn3.getEntry("key").getValue());
        }
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
        tl.checkAfterCommitCount(1);
        assertEquals(0, tl.afterFailedCommitCount);
        assertEquals(0, tl.afterRollbackCount);
        assertEquals(0, tl.closeCount);
        assertEquals(rgn2.getCache(), tl.lastEvent.getCache());
        {
          Collection events;
          RegionAttributes attr = getRegionAttributes();
          if (!attr.getDataPolicy().withReplication() ||
              attr.getConcurrencyChecksEnabled()) {
            events = tl.lastEvent.getPutEvents();
          } else {
            events = tl.lastEvent.getCreateEvents();
          }
          assertEquals(2, events.size());
          ArrayList eventList = new ArrayList(events);
          Collections.sort(eventList, new Comparator() {
              public int compare(Object o1, Object o2) {
                EntryEvent e1 = (EntryEvent)o1;
                EntryEvent e2 = (EntryEvent)o2;
                String s1 = e1.getRegion().getFullPath() + e1.getKey();
                String s2 = e2.getRegion().getFullPath() + e2.getKey();
                return s1.compareTo(s2);
            }});
          Iterator it = eventList.iterator();
          EntryEvent ev;

          ev = (EntryEvent)it.next();
          assertSame(rgn2, ev.getRegion());
          //assertEquals(tl.expectedTxId, ev.getTransactionId());
          assertEquals("key", ev.getKey());
          assertEquals("value2", ev.getNewValue());
          assertEquals(null, ev.getOldValue());
          assertTrue(!ev.getOperation().isLocalLoad());
          assertTrue(!ev.getOperation().isNetLoad());
          assertTrue(!ev.getOperation().isLoad());
          assertTrue(!ev.getOperation().isNetSearch());
          assertTrue(!ev.getOperation().isExpiration());
          assertEquals(null, ev.getCallbackArgument());
          assertEquals(true, ev.isCallbackArgumentAvailable());
          assertTrue(ev.isOriginRemote());
          assertTrue(ev.getOperation().isDistributed());

          ev = (EntryEvent)it.next();
          assertSame(rgn3, ev.getRegion());
          //assertEquals(tl.expectedTxId, ev.getTransactionId());
          assertEquals("key", ev.getKey());
          assertEquals("value3", ev.getNewValue());
          assertEquals(null, ev.getOldValue());
          assertTrue(!ev.getOperation().isLocalLoad());
          assertTrue(!ev.getOperation().isNetLoad());
          assertTrue(!ev.getOperation().isLoad());
          assertTrue(!ev.getOperation().isNetSearch());
          assertTrue(!ev.getOperation().isExpiration());
          assertEquals(null, ev.getCallbackArgument());
          assertEquals(true, ev.isCallbackArgumentAvailable());
          assertTrue(ev.isOriginRemote());
          assertTrue(ev.getOperation().isDistributed());

        }
      }
    };
    SerializableRunnable check1 = new SerializableRunnable("testTXMultiRegion: check") {
      public void run() {
          Region rgn = getRootRegion().getSubregion(rgnName1);
          assertNotNull("Could not find entry for 'key'", rgn.getEntry("key"));
          assertEquals("value1", rgn.getEntry("key").getValue());
          CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
          MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
          tl.checkAfterCommitCount(1);
          assertEquals(0, tl.afterFailedCommitCount);
          assertEquals(0, tl.afterRollbackCount);
          assertEquals(0, tl.closeCount);
          assertEquals(rgn.getCache(), tl.lastEvent.getCache());
          {
            Collection events;
            RegionAttributes attr = getRegionAttributes();
            if (!attr.getDataPolicy().withReplication() ||
                attr.getConcurrencyChecksEnabled()) {
              events = tl.lastEvent.getPutEvents();
            } else {
              events = tl.lastEvent.getCreateEvents();
            }
            assertEquals(1, events.size());
            EntryEvent ev = (EntryEvent)events.iterator().next();
            //assertEquals(tl.expectedTxId, ev.getTransactionId());
            assertTrue(ev.getRegion() == rgn);
            assertEquals("key", ev.getKey());
            assertEquals("value1", ev.getNewValue());
            assertEquals(null, ev.getOldValue());
            assertTrue(!ev.getOperation().isLocalLoad());
            assertTrue(!ev.getOperation().isNetLoad());
            assertTrue(!ev.getOperation().isLoad());
            assertTrue(!ev.getOperation().isNetSearch());
            assertTrue(!ev.getOperation().isExpiration());
            assertEquals(null, ev.getCallbackArgument());
            assertEquals(true, ev.isCallbackArgumentAvailable());
            assertTrue(ev.isOriginRemote());
            assertTrue(ev.getOperation().isDistributed());
          }
        }
    };
    SerializableRunnable check2 = new SerializableRunnable("testTXMultiRegion: check") {
      public void run() {
          Region rgn = getRootRegion().getSubregion(rgnName2);
          assertNotNull("Could not find entry for 'key'", rgn.getEntry("key"));
          assertEquals("value2", rgn.getEntry("key").getValue());
          CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
          MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
          tl.checkAfterCommitCount(1);
          assertEquals(0, tl.afterFailedCommitCount);
          assertEquals(0, tl.afterRollbackCount);
          assertEquals(0, tl.closeCount);
          assertEquals(rgn.getCache(), tl.lastEvent.getCache());
          {
            Collection events;
            RegionAttributes attr = getRegionAttributes();
            if (!attr.getDataPolicy().withReplication() ||
                attr.getConcurrencyChecksEnabled()) {
              events = tl.lastEvent.getPutEvents();
            } else {
              events = tl.lastEvent.getCreateEvents();
            }
            assertEquals(1, events.size());
            EntryEvent ev = (EntryEvent)events.iterator().next();
            //assertEquals(tl.expectedTxId, ev.getTransactionId());
            assertTrue(ev.getRegion() == rgn);
            assertEquals("key", ev.getKey());
            assertEquals("value2", ev.getNewValue());
            assertEquals(null, ev.getOldValue());
            assertTrue(!ev.getOperation().isLocalLoad());
            assertTrue(!ev.getOperation().isNetLoad());
            assertTrue(!ev.getOperation().isLoad());
            assertTrue(!ev.getOperation().isNetSearch());
            assertTrue(!ev.getOperation().isExpiration());
            assertEquals(null, ev.getCallbackArgument());
            assertEquals(true, ev.isCallbackArgumentAvailable());
            assertTrue(ev.isOriginRemote());
            assertTrue(ev.getOperation().isDistributed());
          }
        }
    };
    SerializableRunnable check3 = new SerializableRunnable("testTXMultiRegion: check") {
      public void run() {
          Region rgn = getRootRegion().getSubregion(rgnName3);
          assertNotNull("Could not find entry for 'key'", rgn.getEntry("key"));
          assertEquals("value3", rgn.getEntry("key").getValue());
          CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
          MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
          tl.checkAfterCommitCount(1);
          assertEquals(0, tl.afterFailedCommitCount);
          assertEquals(0, tl.afterRollbackCount);
          assertEquals(0, tl.closeCount);
          assertEquals(rgn.getCache(), tl.lastEvent.getCache());
          {
            Collection events;
            RegionAttributes attr = getRegionAttributes();
            if (!attr.getDataPolicy().withReplication() ||
                attr.getConcurrencyChecksEnabled()) {
              events = tl.lastEvent.getPutEvents();
            } else {
              events = tl.lastEvent.getCreateEvents();
            }
            assertEquals(1, events.size());
            EntryEvent ev = (EntryEvent)events.iterator().next();
            //assertEquals(tl.expectedTxId, ev.getTransactionId());
            assertTrue(ev.getRegion() == rgn);
            assertEquals("key", ev.getKey());
            assertEquals("value3", ev.getNewValue());
            assertEquals(null, ev.getOldValue());
            assertTrue(!ev.getOperation().isLocalLoad());
            assertTrue(!ev.getOperation().isNetLoad());
            assertTrue(!ev.getOperation().isLoad());
            assertTrue(!ev.getOperation().isNetSearch());
            assertTrue(!ev.getOperation().isExpiration());
            assertEquals(null, ev.getCallbackArgument());
            assertEquals(true, ev.isCallbackArgumentAvailable());
            assertTrue(ev.isOriginRemote());
            assertTrue(ev.getOperation().isDistributed());
          }
        }
    };

    assertEquals(4, Host.getHost(0).getVMCount());
//GemFireVersion.waitForJavaDebugger(getLogWriter(), "CTRLR WAITING AFTER CREATE");
    VM vm0 = Host.getHost(0).getVM(0);
    VM vm1 = Host.getHost(0).getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);
    VM vm3 = Host.getHost(0).getVM(3);

    try {
      DMStats dmStats = getSystem().getDistributionManager().getStats();
      Region rgn1;
      Region rgn2;
      Region rgn3;
      long cmtMsgs;
//      long sentMsgs;
      long commitWaits;

      // vm0->R1,R3 vm1->R1,R3 vm2->R2 vm3->R2,R3
      vm0.invoke(create1);
      vm0.invoke(newKey1);
      vm0.invoke(create3);
      vm0.invoke(newKey3);
      
      vm1.invoke(create1);
      vm1.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm1.invoke(newKey1);
        vm1.invoke(newKey3);
      }
      
      vm2.invoke(create2);
      vm2.invoke(newKey2);
      
      vm3.invoke(create2);
      vm3.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm3.invoke(newKey2);
        vm3.invoke(newKey3);
      }
      
      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");
//      TransactionId txId = txMgr.getTransactionId();
      getSystem().getLogWriter().info("testTXMultiRegion: vm0->R1,R3 vm1->R1,R3 vm2->R2 vm3->R2,R3");
      txMgr.commit();
      assertEquals(cmtMsgs+3, dmStats.getSentCommitMessages());
      if (rgn1.getAttributes().getScope().isAck()
          || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()
          ) {
        assertEquals(commitWaits+1, dmStats.getCommitWaits());
      } else {
        assertEquals(commitWaits, dmStats.getCommitWaits());
        // pause to give cmt message a chance to be processed
        // [bruce] changed from 200 to 2000 for mcast testing
        try {Thread.sleep(2000);} catch (InterruptedException chomp) {fail("interrupted");}
      }
      vm0.invoke(check1_3);
      vm1.invoke(check1_3);
      vm2.invoke(check2);
      vm3.invoke(check2_3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1,R3 vm1->R1,R3 vm2->R2,R3 vm3->R2,R3
      vm0.invoke(create1);
      vm0.invoke(newKey1);
      vm0.invoke(create3);
      vm0.invoke(newKey3);
      
      vm1.invoke(create1);
      vm1.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm1.invoke(newKey1);
        vm1.invoke(newKey3);
      }
      
      vm2.invoke(create2);
      vm2.invoke(newKey2);
      vm2.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm2.invoke(newKey3);
      }
      
      vm3.invoke(create2);
      vm3.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm3.invoke(newKey2);
        vm3.invoke(newKey3);
      }
      
      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");
//      txId = txMgr.getTransactionId();
      getSystem().getLogWriter().info("testTXMultiRegion: vm0->R1,R3 vm1->R1,R3 vm2->R2,R3 vm3->R2,R3");
      txMgr.commit();
      assertEquals(cmtMsgs+2, dmStats.getSentCommitMessages());
      if (rgn1.getAttributes().getScope().isAck()
          || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()
          ) {
        assertEquals(commitWaits+1, dmStats.getCommitWaits());
      } else {
        assertEquals(commitWaits, dmStats.getCommitWaits());
        // pause to give cmt message a chance to be processed
        try {Thread.sleep(200);} catch (InterruptedException chomp) {fail("interrupted");}
      }
      vm0.invoke(check1_3);
      vm1.invoke(check1_3);
      vm2.invoke(check2_3);
      vm3.invoke(check2_3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1,R3 vm1->R1,R3 vm2->R2 vm3->R1,R3
      vm0.invoke(create1);
      vm0.invoke(newKey1);
      vm0.invoke(create3);
      vm0.invoke(newKey3);
      
      vm1.invoke(create1);
      vm1.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm1.invoke(newKey1);
        vm1.invoke(newKey3);
      }

      vm2.invoke(create2);
      vm2.invoke(newKey2);
      
      vm3.invoke(create1);
      vm3.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm3.invoke(newKey1);
        vm3.invoke(newKey3);
      }
      
      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");
//      txId = txMgr.getTransactionId();
      getSystem().getLogWriter().info("testTXMultiRegion: vm0->R1,R3 vm1->R1,R3 vm2->R2 vm3->R1,R3");
      txMgr.commit();
      assertEquals(cmtMsgs+2, dmStats.getSentCommitMessages());
      if (rgn1.getAttributes().getScope().isAck()
          || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()
          ) {
        assertEquals(commitWaits+1, dmStats.getCommitWaits());
      } else {
        assertEquals(commitWaits, dmStats.getCommitWaits());
        // pause to give cmt message a chance to be processed
        try {Thread.sleep(200);} catch (InterruptedException chomp) {fail("interrupted");}
      }
      vm0.invoke(check1_3);
      vm1.invoke(check1_3);
      vm2.invoke(check2);
      vm3.invoke(check1_3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1 vm1->R1 vm2->R2 vm3->R3
      vm0.invoke(create1);
      vm0.invoke(newKey1);
      
      vm1.invoke(create1);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm1.invoke(newKey1);
      }

      vm2.invoke(create2);
      vm2.invoke(newKey2);
      
      vm3.invoke(create3);
      vm3.invoke(newKey3);
      
      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");
//      txId = txMgr.getTransactionId();
      getSystem().getLogWriter().info("testTXMultiRegion: vm0->R1 vm1->R1 vm2->R2 vm3->R3");
      txMgr.commit();
      assertEquals(cmtMsgs+3, dmStats.getSentCommitMessages());
      if (rgn1.getAttributes().getScope().isAck()
          || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()
          ) {
        assertEquals(commitWaits+1, dmStats.getCommitWaits());
      } else {
        assertEquals(commitWaits, dmStats.getCommitWaits());
        // pause to give cmt message a chance to be processed
        try {Thread.sleep(200);} catch (InterruptedException chomp) {fail("interrupted");}
      }
      vm0.invoke(check1);
      vm1.invoke(check1);
      vm2.invoke(check2);
      vm3.invoke(check3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1,R3 vm1->R2,R3 vm2->R2 vm3->R3
      vm0.invoke(create1);
      vm0.invoke(newKey1);
      vm0.invoke(create3);
      vm0.invoke(newKey3);
      
      vm1.invoke(create2);
      vm1.invoke(newKey2);
      vm1.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm1.invoke(newKey3);
      }
      
      vm2.invoke(create2);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm2.invoke(newKey2);
      }
      
      vm3.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm3.invoke(newKey3);
      }
      
      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");
//      txId = txMgr.getTransactionId();
      getSystem().getLogWriter().info("testTXMultiRegion: vm0->R1,R3 vm1->R2,R3 vm2->R2 vm3->R3");
      txMgr.commit();
      assertEquals(cmtMsgs+4, dmStats.getSentCommitMessages());
      if (rgn1.getAttributes().getScope().isAck()
          || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()
          ) {
        assertEquals(commitWaits+1, dmStats.getCommitWaits());
      } else {
        assertEquals(commitWaits, dmStats.getCommitWaits());
        // pause to give cmt message a chance to be processed
        try {Thread.sleep(200);} catch (InterruptedException chomp) {fail("interrupted");}
      }
      vm0.invoke(check1_3);
      vm1.invoke(check2_3);
      vm2.invoke(check2);
      vm3.invoke(check3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1,R3 vm1->R1,R3 vm2->R1,R3 vm3->R1,R3
      vm0.invoke(create1);
      vm0.invoke(newKey1);
      vm0.invoke(create3);
      vm0.invoke(newKey3);
      
      vm1.invoke(create1);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm1.invoke(newKey1);
      }
      vm1.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm1.invoke(newKey3);
      }
      
      vm2.invoke(create1);
      vm2.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm2.invoke(newKey1);
        vm2.invoke(newKey3);
      }
      
      vm3.invoke(create1);
      vm3.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication() &&
          !getRegionAttributes().getDataPolicy().isPreloaded()) {
        vm3.invoke(newKey1);
        vm3.invoke(newKey3);
      }
      
      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");
//      txId = txMgr.getTransactionId();
      getSystem().getLogWriter().info("testTXMultiRegion: vm0->R1,R3 vm1->R1,R3 vm2->R1,R3 vm3->R1,R3");
      txMgr.commit();
      assertEquals(cmtMsgs+1, dmStats.getSentCommitMessages());
      if (rgn1.getAttributes().getScope().isAck()
          || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()
          ) {
        assertEquals(commitWaits+1, dmStats.getCommitWaits());
      } else {
        assertEquals(commitWaits, dmStats.getCommitWaits());
        // pause to give cmt message a chance to be processed
        try {Thread.sleep(200);} catch (InterruptedException chomp) {fail("interrupted");}
      }
      vm0.invoke(check1_3);
      vm1.invoke(check1_3);
      vm2.invoke(check1_3);
      vm3.invoke(check1_3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();
    }
    catch(Exception e) {
      CacheFactory.getInstance(getSystem()).close();
      getSystem().getLogWriter().fine("testTXMultiRegion: Caused exception in createRegion");
      throw e;
    }

  }

  public void testTXRmtMirror() throws Exception {
    if (!supportsTransactions()) {
      return;
    }
    assertTrue(getRegionAttributes().getScope().isDistributed());
    CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();

    if (getRegionAttributes().getScope().isGlobal()
        || getRegionAttributes().getDataPolicy().withPersistence()) {
      return;
    }
    final String rgnName = getUniqueName();

    SerializableRunnable createMirror = new SerializableRunnable("textTXRmtMirror: Create Mirrored Region") {
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        txMgr2.addListener(tl);
        try {
          AttributesFactory rgnAtts = new AttributesFactory(getRegionAttributes());
          rgnAtts.setDataPolicy(DataPolicy.REPLICATE);
          createRegion(rgnName, rgnAtts.create());
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };

    SerializableRunnable createNonMirror = new SerializableRunnable("textTXRmtMirror: Create Mirrored Region") {
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        txMgr2.addListener(tl);
        try {
          AttributesFactory rgnAtts = new AttributesFactory(getRegionAttributes());
          rgnAtts.setDataPolicy(DataPolicy.NORMAL);
          createRegion(rgnName, rgnAtts.create());
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };
    SerializableRunnable checkExists = new SerializableRunnable("textTXRmtMirror: checkExists") {
      public void run() {
        Region rgn = getRootRegion().getSubregion(rgnName);
        {
          assertNotNull("Could not find entry for 'key'", rgn.getEntry("key"));
          assertEquals("value", rgn.getEntry("key").getValue());
        }
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
        tl.checkAfterCommitCount(1);
        assertEquals(0, tl.afterFailedCommitCount);
        assertEquals(0, tl.afterRollbackCount);
        assertEquals(0, tl.closeCount);
        assertEquals(rgn.getCache(), tl.lastEvent.getCache());
        {
          Collection events = tl.lastEvent.getCreateEvents();
          assertEquals(1, events.size());
          EntryEvent ev = (EntryEvent)events.iterator().next();
          //assertEquals(tl.expectedTxId, ev.getTransactionId());
          assertTrue(ev.getRegion() == rgn);
          assertEquals("key", ev.getKey());
          assertEquals("value", ev.getNewValue());
          assertEquals(null, ev.getOldValue());
          assertTrue(!ev.getOperation().isLocalLoad());
          assertTrue(!ev.getOperation().isNetLoad());
          assertTrue(!ev.getOperation().isLoad());
          assertTrue(!ev.getOperation().isNetSearch());
          assertTrue(!ev.getOperation().isExpiration());
          assertEquals(null, ev.getCallbackArgument());
          assertEquals(true, ev.isCallbackArgumentAvailable());
          assertTrue(ev.isOriginRemote());
          assertTrue(ev.getOperation().isDistributed());
        }
      }
    };
    SerializableRunnable checkNoKey = new SerializableRunnable("textTXRmtMirror: checkNoKey") {
      public void run() {
        Region rgn = getRootRegion().getSubregion(rgnName);
        {
          assertTrue(rgn.getEntry("key") == null);
        }
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
        tl.checkAfterCommitCount(1);
        assertEquals(0, tl.afterFailedCommitCount);
        assertEquals(0, tl.afterRollbackCount);
        assertEquals(0, tl.closeCount);
        assertEquals(0, tl.lastEvent.getCreateEvents().size());
        assertEquals(0, tl.lastEvent.getPutEvents().size());
        assertEquals(rgn.getCache(), tl.lastEvent.getCache());
      }
    };


//GemFireVersion.waitForJavaDebugger(getLogWriter(), "CTRLR WAITING AFTER CREATE");

    try {
      Region rgn;

      // Make sure that a remote create done in a tx is created in a remote mirror
      // and dropped in a remote non-mirror
      Host.getHost(0).getVM(0).invoke(createMirror);
      Host.getHost(0).getVM(1).invoke(createNonMirror);
      rgn = createRegion(rgnName);

      txMgr.begin();
      rgn.create("key", "value");
//      TransactionId txId = txMgr.getTransactionId();
      getSystem().getLogWriter().info("textTXRmtMirror: create mirror and non-mirror");
      txMgr.commit();
      if (!rgn.getAttributes().getScope().isAck()){
        // pause to give cmt message a chance to be processed
        try {Thread.sleep(200);} catch (InterruptedException chomp) {fail("interrupted");}
      }
      Host.getHost(0).getVM(0).invoke(checkExists);
      Host.getHost(0).getVM(1).invoke(checkNoKey);
      rgn.destroyRegion();

      // @todo darrel/mitch: how do a get rid of the entry remotely
      // Make sure that a remote put, that modifies an existing entry,
      // done in a tx is dropped in a remote mirror that does not have the entry.
    }
    catch(Exception e) {
      CacheFactory.getInstance(getSystem()).close();
      getSystem().getLogWriter().fine("textTXRmtMirror: Caused exception in createRegion");
      throw e;
    }

  }

  public void todo_testTXAlgebra() throws Exception {
    assertTrue(getRegionAttributes().getScope().isDistributed());
    if (getRegionAttributes().getScope().isGlobal()
        || getRegionAttributes().getDataPolicy().withPersistence()) {
      return;
    }

    CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();
    MyTransactionListener localTl = new MyTransactionListener();
    TransactionId myTXId;
    txMgr.addListener(localTl);
    assertEquals(null, localTl.lastEvent);
    localTl.assertCounts(0, 0, 0, 0);
    final String rgnName = getUniqueName();

    SerializableRunnable create = new SerializableRunnable("testTXAlgebra: Create Region") {
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        txMgr2.addListener(tl);
        assertEquals(null, tl.lastEvent);
        tl.assertCounts(0, 0, 0, 0);
        try {
          Region rgn = createRegion(rgnName);
          if (!getRegionAttributes().getDataPolicy().withReplication()) {
            rgn.create("key", null);
            getSystem().getLogWriter().info("testTXAlgebra: Created Key");
          }
        }
        catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", e);
        }
      }
    };

    Invoke.invokeInEveryVM(create);
    // make sure each op sequence has the correct affect transaction event
    // check C + C -> EX
    // check C + P -> C
    try {
      AttributesFactory rgnAtts = new AttributesFactory(getRegionAttributes());
      rgnAtts.setDataPolicy(DataPolicy.NORMAL);
      Region rgn = createRegion(rgnName, rgnAtts.create());
      //// callbackVal.reset();
      txMgr.begin();
      myTXId = txMgr.getTransactionId();
      rgn.create("key", "value1");
      //// callbackVal.assertCreateCnt(1);
      try {
        rgn.create("key", "value2");
        fail("expected EntryExistsException");
      } catch (EntryExistsException ok) {
      }
      //// callbackVal.assertCreateCnt(1, /*remember*/ false);
      rgn.put("key", "value2");
      //// callbackVal.assertUpdateCnt(1);
      assertEquals("value2", rgn.getEntry("key").getValue());
      txMgr.commit();
      // Make sure commit did not trigger callbacks
      //// callbackVal.reAssert();
      Invoke.invokeInEveryVM(new CacheSerializableRunnable("testTXAlgebra: check: C+P->C") {
          public void run2() {
            Region rgn1 = getRootRegion().getSubregion(rgnName);
            CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];

            assertEquals("value2", rgn1.getEntry("key").getValue());
            tl.assertCounts(1, 0, 0, 0);
            {
              Collection events;
              RegionAttributes attr = getRegionAttributes();
              if (!attr.getDataPolicy().withReplication() ||
                  attr.getConcurrencyChecksEnabled()) {
                events = tl.lastEvent.getPutEvents();
              } else {
                events = tl.lastEvent.getCreateEvents();
              }
              assertEquals(1, events.size());
              EntryEvent ev = (EntryEvent)events.iterator().next();
              // assertEquals(tl.expectedTxId, ev.getTransactionId());
              assertTrue(ev.getRegion() == rgn1);
              assertEquals("key", ev.getKey());
              assertEquals("value2", ev.getNewValue());
              assertNull(ev.getOldValue());
              assertTrue(!ev.getOperation().isLocalLoad());
              assertTrue(!ev.getOperation().isNetLoad());
              assertTrue(!ev.getOperation().isLoad());
              assertTrue(!ev.getOperation().isNetSearch());
              assertTrue(!ev.getOperation().isExpiration());
              assertEquals(null, ev.getCallbackArgument());
              assertEquals(true, ev.isCallbackArgumentAvailable());
              assertTrue(ev.isOriginRemote());
              assertTrue(ev.getOperation().isDistributed());
            }
          }
        });
      assertEquals("value2", rgn.getEntry("key").getValue());
      {
        localTl.assertCounts(1, 0, 0, 0);
        Collection events = localTl.lastEvent.getCreateEvents();
        assertEquals(myTXId, localTl.lastEvent.getTransactionId());
        assertEquals(1, events.size());
        EntryEvent ev = (EntryEvent)events.iterator().next();
        assertEquals(myTXId, ev.getTransactionId());
        assertTrue(ev.getRegion() == rgn);
        assertEquals("key", ev.getKey());
        assertEquals("value2", ev.getNewValue());
        assertNull(ev.getOldValue());
        assertTrue(!ev.getOperation().isLocalLoad());
        assertTrue(!ev.getOperation().isNetLoad());
        assertTrue(!ev.getOperation().isLoad());
        assertTrue(!ev.getOperation().isNetSearch());
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
      if (!getRegionAttributes().getDataPolicy().withReplication()) {
        rgn.invalidate("key");
        rgn.localDestroy("key");
      } else {
        rgn.destroy("key");
      }

      // Check C + DI -> C (invalid value)
      //// callbackVal.reset();
      txMgr.begin();
      myTXId = txMgr.getTransactionId();
      rgn.create("key", "value1");
      //// callbackVal.assertCreateCnt(1);
      rgn.invalidate("key");
      //// callbackVal.assertInvalidateCnt(1);
      assertTrue(rgn.containsKey("key"));
      assertTrue(!rgn.containsValueForKey("key"));
      txMgr.commit();
      //// callbackVal.reAssert();
      Invoke.invokeInEveryVM(new CacheSerializableRunnable("testTXAlgebra: check: C+DI->C (invalid value)") {
          public void run2() {
            Region rgn1 = getRootRegion().getSubregion(rgnName);
            CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];

            assertTrue(rgn1.containsKey("key"));
            assertTrue(!rgn1.containsValueForKey("key"));
            tl.assertCounts(2, 0, 0, 0);
            {
              Collection events;
              RegionAttributes attr = getRegionAttributes();
              if (!attr.getDataPolicy().withReplication() ||
                  attr.getConcurrencyChecksEnabled()) {
                events = tl.lastEvent.getPutEvents();
              } else {
                events = tl.lastEvent.getCreateEvents();
              }
              assertEquals(1, events.size());
              EntryEvent ev = (EntryEvent)events.iterator().next();
              // assertEquals(tl.expectedTxId, ev.getTransactionId());
              assertTrue(ev.getRegion() == rgn1);
              assertEquals("key", ev.getKey());
              assertNull(ev.getNewValue());
              assertNull(ev.getOldValue());
              assertTrue(!ev.getOperation().isLocalLoad());
              assertTrue(!ev.getOperation().isNetLoad());
              assertTrue(!ev.getOperation().isLoad());
              assertTrue(!ev.getOperation().isNetSearch());
              assertTrue(!ev.getOperation().isExpiration());
              assertEquals(null, ev.getCallbackArgument());
              assertEquals(true, ev.isCallbackArgumentAvailable());
              assertTrue(ev.isOriginRemote());
              assertTrue(ev.getOperation().isDistributed());
            }
          }
        });
      assertTrue(rgn.containsKey("key"));
      assertTrue(!rgn.containsValueForKey("key"));
      localTl.assertCounts(2, 0, 0, 0);
      {
        Collection events = localTl.lastEvent.getCreateEvents();
        assertEquals(myTXId, localTl.lastEvent.getTransactionId());
        assertEquals(1, events.size());
        EntryEvent ev = (EntryEvent)events.iterator().next();
        assertEquals(myTXId, ev.getTransactionId());
        assertTrue(ev.getRegion() == rgn);
        assertEquals("key", ev.getKey());
        assertNull(ev.getNewValue());
        assertNull(ev.getOldValue());
        assertTrue(!ev.getOperation().isLocalLoad());
        assertTrue(!ev.getOperation().isNetLoad());
        assertTrue(!ev.getOperation().isLoad());
        assertTrue(!ev.getOperation().isNetSearch());
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }

      // check (commited) LI + DI -> NOOP
      assertTrue(rgn.containsKey("key"));
      assertTrue(!rgn.containsValueForKey("key"));
      txMgr.begin();
      myTXId = txMgr.getTransactionId();
      rgn.invalidate("key");
      assertTrue(rgn.containsKey("key"));
      assertTrue(!rgn.containsValueForKey("key"));
      txMgr.commit();
      Invoke.invokeInEveryVM(new CacheSerializableRunnable("testTXAlgebra: check: committed LI + TX DI-> NOOP") {
          public void run2() {
            Region rgn1 = getRootRegion().getSubregion(rgnName);
            CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];

            assertEquals("value1", rgn1.getEntry("key").getValue());
            tl.assertCounts(2, 0, 0, 0); // There should be no change in counts
          }
        });
      assertTrue(rgn.containsKey("key"));
      assertTrue(!rgn.containsValueForKey("key"));
      localTl.assertCounts(2, 0, 0, 0); // There should be no change in counts

      // @todo mitch
      // check DI + LI -> DI
      // check DI + DI -> DI

      if (!getRegionAttributes().getDataPolicy().withReplication()) {
        // check (exists commited) LI + DI -> LI
        rgn.put("key", "value1");
        assertTrue(rgn.containsKey("key"));
        assertEquals("value1", rgn.getEntry("key").getValue());
        txMgr.begin();
        myTXId = txMgr.getTransactionId();
        rgn.localInvalidate("key");
        rgn.invalidate("key");
        assertTrue(rgn.containsKey("key"));
        assertTrue(!rgn.containsValueForKey("key"));
        txMgr.commit();
        Invoke.invokeInEveryVM(new CacheSerializableRunnable("testTXAlgebra: check: TX LI + TX DI -> LI") {
            public void run2() {
              Region rgn1 = getRootRegion().getSubregion(rgnName);
              CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
              MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
              assertTrue(rgn1.containsKey("key"));
              assertTrue(rgn1.containsValueForKey("key"));
              assertEquals("value1", rgn1.getEntry("key").getValue());
              tl.assertCounts(2, 0, 0, 0); // nothing happened remotely
            }
          });
        assertTrue(rgn.containsKey("key"));
        assertTrue(!rgn.containsValueForKey("key"));
        assertNull(rgn.getEntry("key").getValue());
        localTl.assertCounts(3, 0, 0, 0);
        {
          Collection events = localTl.lastEvent.getInvalidateEvents();
          assertEquals(1, events.size());
          EntryEvent ev = (EntryEvent)events.iterator().next();
          assertEquals(myTXId, ev.getTransactionId());
          assertTrue(ev.getRegion() == rgn);
          assertEquals("key", ev.getKey());
          assertNull(ev.getNewValue());
          assertEquals("value1", ev.getOldValue());
          assertTrue(!ev.getOperation().isLocalLoad());
          assertTrue(!ev.getOperation().isNetLoad());
          assertTrue(!ev.getOperation().isLoad());
          assertTrue(!ev.getOperation().isNetSearch());
          assertEquals(null, ev.getCallbackArgument());
          assertEquals(true, ev.isCallbackArgumentAvailable());
          assertTrue(!ev.isOriginRemote());
          assertTrue(!ev.getOperation().isExpiration());
          assertTrue(ev.getOperation().isDistributed());
        }

        rgn.invalidate("key");
        rgn.localDestroy("key");
      } else {
        rgn.destroy("key");
      }

      // check C + DD -> DD
      //// callbackVal.reset();
      txMgr.begin();
      myTXId = txMgr.getTransactionId();
      rgn.create("key", "value0");
      //// callbackVal.assertCreateCnt(1);
      rgn.destroy("key");
      //// callbackVal.assertDestroyCnt(1);
      assertTrue(!rgn.containsKey("key"));
      txMgr.commit();
      //// callbackVal.reAssert();
      Invoke.invokeInEveryVM(new CacheSerializableRunnable("testTXAlgebra: check: C+DD->DD") {
          public void run2() {
            Region rgn1 = getRootRegion().getSubregion(rgnName);
            CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];

            assertTrue(!rgn1.containsKey("key"));
            assertTrue(!rgn1.containsValueForKey("key"));
            tl.assertCounts(3, 0, 0, 0);
            {
              Collection events = tl.lastEvent.getDestroyEvents();
              assertEquals(1, events.size());
              EntryEvent ev = (EntryEvent)events.iterator().next();
              // assertEquals(tl.expectedTxId, ev.getTransactionId());
              assertTrue(ev.getRegion() == rgn1);
              assertNull(ev.getKey());
              assertNull(ev.getNewValue());
              assertNull(ev.getOldValue());
              assertTrue(!ev.getOperation().isLocalLoad());
              assertTrue(!ev.getOperation().isNetLoad());
              assertTrue(!ev.getOperation().isLoad());
              assertTrue(!ev.getOperation().isNetSearch());
              assertTrue(!ev.getOperation().isExpiration());
              assertEquals(null, ev.getCallbackArgument());
              assertEquals(true, ev.isCallbackArgumentAvailable());
              assertTrue(ev.isOriginRemote());
              assertTrue(ev.getOperation().isDistributed());
            }
          }
        });
      assertTrue(!rgn.containsKey("key"));
      localTl.assertCounts(3, 0, 0, 0); // no change

      // Check C + LI -> C
      if (!getRegionAttributes().getDataPolicy().withReplication()) {
        // assume that remote regions have same mirror type as local
        Invoke.invokeInEveryVM(new CacheSerializableRunnable("testTXAlgebra: C+LI-> entry creation") {
            public void run2() {
              Region rgn1 = getRootRegion().getSubregion(rgnName);
              try {
                rgn1.create("key", null);
              } catch (CacheException e) {
                com.gemstone.gemfire.test.dunit.Assert.fail("While creating key", e);
              }
            }
          });
      }
      assertTrue(!rgn.containsKey("key"));
      txMgr.begin();
      myTXId = txMgr.getTransactionId();
      rgn.create("key", "value1");
      //// callbackVal.assertCreateCnt(1);
      rgn.localInvalidate("key");
      //// callbackVal.assertInvalidateCnt(1);
      assertTrue(rgn.containsKey("key"));
      assertTrue(!rgn.containsValueForKey("key"));
      txMgr.commit();
      //// callbackVal.reAssert();
      Invoke.invokeInEveryVM(new CacheSerializableRunnable("testTXAlgebra: check: C+LI->C (with value)") {
          public void run2() {
            Region rgn1 = getRootRegion().getSubregion(rgnName);
            CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            MyTransactionListener tl = (MyTransactionListener)txMgr2.getListeners()[0];
            tl.assertCounts(4, 0, 0, 0);
            assertTrue(rgn1.containsKey("key"));
            assertEquals("value1", rgn1.getEntry("key").getValue());
            {
              Collection events;
              RegionAttributes attr = getRegionAttributes();
              if (!attr.getDataPolicy().withReplication() ||
                  attr.getConcurrencyChecksEnabled()) {
                 events = tl.lastEvent.getPutEvents();
              } else {
                 events = tl.lastEvent.getCreateEvents();
              }
              assertEquals(1, events.size());
              EntryEvent ev = (EntryEvent)events.iterator().next();
              // assertEquals(tl.expectedTxId, ev.getTransactionId());
              assertTrue(ev.getRegion() == rgn1);
              assertEquals("key", ev.getKey());
              assertEquals("value1", ev.getNewValue());
              assertNull(ev.getOldValue());
              assertTrue(!ev.getOperation().isLocalLoad());
              assertTrue(!ev.getOperation().isNetLoad());
              assertTrue(!ev.getOperation().isLoad());
              assertTrue(!ev.getOperation().isNetSearch());
              assertTrue(!ev.getOperation().isExpiration());
              assertEquals(null, ev.getCallbackArgument());
              assertEquals(true, ev.isCallbackArgumentAvailable());
              assertTrue(ev.isOriginRemote());
              assertTrue(ev.getOperation().isDistributed());
            }
          }
        });
      assertTrue(rgn.containsKey("key"));
      assertTrue(!rgn.containsValueForKey("key"));
      localTl.assertCounts(4, 0, 0, 0);
      {
        Collection events = localTl.lastEvent.getCreateEvents();
        assertEquals(myTXId, localTl.lastEvent.getTransactionId());
        assertEquals(1, events.size());
        EntryEvent ev = (EntryEvent) events.iterator().next();
        assertEquals(myTXId, ev.getTransactionId());
        assertTrue(ev.getRegion() == rgn);
        assertEquals("key", ev.getKey());
        assertNull(ev.getNewValue());
        assertNull(ev.getOldValue());
        assertTrue(!ev.getOperation().isLocalLoad());
        assertTrue(!ev.getOperation().isNetLoad());
        assertTrue(!ev.getOperation().isLoad());
        assertTrue(!ev.getOperation().isNetSearch());
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
      rgn.destroy("key");

      // Check P + LI + C -> EX
      // Check C + LI + P -> C
      //// callbackVal.reset();
      txMgr.begin();
      myTXId = txMgr.getTransactionId();
      rgn.create("key", "value1");
      //// callbackVal.assertCreateCnt(1);
      rgn.localInvalidate("key");
      //// callbackVal.assertInvalidateCnt(1);
      try {
        rgn.create("key", "ex");
        fail("expected EntryExistsException");
      } catch (EntryExistsException ok) {
      }
      //// callbackVal.assertCreateCnt(1, /*remember*/ false);
      rgn.put("key", "value2");
      //// callbackVal.assertUpdateCnt(1);
      assertTrue(rgn.containsKey("key"));
      assertEquals("value2", rgn.getEntry("key").getValue());
      txMgr.commit();
      //// callbackVal.reAssert();
      assertTrue(rgn.containsKey("key"));
      assertEquals("value2", rgn.getEntry("key").getValue());
      localTl.assertCounts(5, 0, 0, 0);
      {
        Collection events = localTl.lastEvent.getCreateEvents();
        assertEquals(myTXId, localTl.lastEvent.getTransactionId());
        assertEquals(1, events.size());
        EntryEvent ev = (EntryEvent)events.iterator().next();
        assertEquals(myTXId, ev.getTransactionId());
        assertTrue(ev.getRegion() == rgn);
        assertEquals("key", ev.getKey());
        assertEquals("value2", ev.getNewValue());
        assertNull( ev.getOldValue());
        assertTrue(!ev.getOperation().isLocalLoad());
        assertTrue(!ev.getOperation().isNetLoad());
        assertTrue(!ev.getOperation().isLoad());
        assertTrue(!ev.getOperation().isNetSearch());
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
      rgn.localDestroy("key");




    }
    catch(Exception e) {
      CacheFactory.getInstance(getSystem()).close();
      getSystem().getLogWriter().fine("testTXAlgebra: Caused exception in createRegion");
      throw e;
    }

  }
  
  //////////////////////////////////////////////////////////////////////////////////////
  //                 M E T H O D S   F O R   V E R S I O N I N G   F O L L O W
  //////////////////////////////////////////////////////////////////////////////////////
  
  
  public static LocalRegion CCRegion;
  static int distributedSystemID;
  static int afterCreates;


  /**
   * return the region attributes for the given type of region.
   * See GemFireCache.getRegionAttributes(String).
   * Subclasses are expected to reimplement this method.  See
   * DistributedAckRegionCCEDUnitTest.getRegionAttributes(String).
   */
  protected RegionAttributes getRegionAttributes(String type) {
    throw new IllegalStateException("subclass must reimplement this method");
  }

  /*
   * This test creates a server cache in vm0 and a peer cache in vm1.
   * It then tests to see if GII transferred tombstones to vm1 like it's supposed to.
   * A client cache is created in vm2 and the same sort of check is performed
   * for register-interest.
   */
  
  public void versionTestGIISendsTombstones() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    
    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2.  Afterward make
    // sure that all three regions are consistent
    
    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
        public void run() {
          try {
            RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
            CCRegion = (LocalRegion)f.create(name);
            if (VM.getCurrentVMNum() == 0) {
              CacheServer bridge = CCRegion.getCache().addCacheServer();
              bridge.setPort(serverPort);
              try {
                bridge.start();
              } catch (IOException ex) {
                com.gemstone.gemfire.test.dunit.Assert.fail("While creating bridge", ex);
              }
            }
          } catch (CacheException ex) {
            com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", ex);
          }
        }
      };
      
    SerializableRunnable asserter = new SerializableRunnable("ensure tombstone has been received") {
      public void run() {
        RegionEntry entry = CCRegion.getRegionEntry("object2");
        Assert.assertTrue(entry != null);
        Assert.assertTrue(entry.isTombstone());
      }
    };
    
    vm0.invoke(createRegion);
    vm0.invoke(new SerializableRunnable("create some tombstones") {
      public void run() {
        CCRegion.put("object1", "value1");
        CCRegion.put("object2", "value2");
        CCRegion.put("object3", "value3");
        CCRegion.destroy("object2");
      }
    });
    
//    SerializableRunnable createClientCache = new SerializableRunnable("Create client cache") {
//      public void run() {
//        ClientCacheFactory ccf = new ClientCacheFactory();
//        ccf.addPoolServer("localhost"/*getServerHostName(Host.getHost(0))*/, serverPort);
//        ccf.setPoolSubscriptionEnabled(true);
//        ccf.set("log-level", getGemFireLogLevel());
//        ClientCache cCache = getClientCache(ccf);
//        ClientRegionFactory<Integer, String> crf = cCache
//            .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
//        crf.setConcurrencyChecksEnabled(true);
//        CCRegion = (LocalRegion)crf.create(name);
//        CCRegion.registerInterest("ALL_KEYS");
//      }
//    };

    try {
      vm0.invoke(asserter);
      vm1.invoke(createRegion);
      vm1.invoke(asserter);
//      vm2.invoke(createClientCache);
//      vm2.invoke(asserter);
    } finally {
      disconnectAllFromDS();
    }
  }


  protected void disconnect(VM vm) {
    SerializableRunnable disconnect = new SerializableRunnable("disconnect") {
      public void run() {
//        GatewayBatchOp.VERSION_WITH_OLD_WAN = false;
        distributedSystemID = -1;
        CCRegion.getCache().getDistributedSystem().disconnect();
      }
    };
    vm.invoke(disconnect);
  }

  
  /**
   * This tests the concurrency versioning system to ensure that event conflation
   * happens correctly and that the statistic is being updated properly
   */
  public void versionTestConcurrentEvents() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2.  Afterward make
    // sure that all three regions are consistent
    
    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
        public void run() {
          try {
            RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
            CCRegion = (LocalRegion)f.create(name);
          } catch (CacheException ex) {
            com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", ex);
          }
        }
      };
      
    vm0.invoke(createRegion);
    vm1.invoke(createRegion);
    
    
    SerializableRunnable performOps = new SerializableRunnable("perform concurrent ops") {
      public void run() {
        try {
          doOpsLoop(5000, false);
          long events = CCRegion.getCachePerfStats().getConflatedEventsCount();
          if (!CCRegion.getScope().isGlobal()) {
            assertTrue("expected some event conflation", events>0);
          }
        } catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("while performing concurrent operations", e);
        }
//        } catch (InterruptedException e) {
//          fail("someone interrupted my sleep");
//        }
      }
    };
    
    AsyncInvocation a0 = vm0.invokeAsync(performOps);
    AsyncInvocation a1 = vm1.invokeAsync(performOps);

    try { Thread.sleep(500); } catch (InterruptedException e) { fail("sleep was interrupted"); }
    
    vm2.invoke(createRegion);
    
    boolean a0failed = waitForAsyncProcessing(a0, "expected some event conflation");
    boolean a1failed = waitForAsyncProcessing(a1, "expected some event conflation");

    if (a0failed && a1failed) {
      fail("neither member saw event conflation - check stats for " + name);
    }
    
    // check consistency of the regions
    Map r0Contents = (Map)vm0.invoke(this.getClass(), "getCCRegionContents");
    Map r1Contents = (Map)vm1.invoke(this.getClass(), "getCCRegionContents");
    Map r2Contents = (Map)vm2.invoke(this.getClass(), "getCCRegionContents");
    
    for (int i=0; i<10; i++) {
      String key = "cckey" + i;
      assertEquals("region contents are not consistent for " + key, r0Contents.get(key), r1Contents.get(key));
      assertEquals("region contents are not consistent for " + key, r1Contents.get(key), r2Contents.get(key));
      for (int subi=1; subi<3; subi++) {
        String subkey = key + "-" + subi;
        if (r0Contents.containsKey(subkey)) {
          assertEquals("region contents are not consistent for " + subkey, r0Contents.get(subkey), r1Contents.get(subkey));
          assertEquals("region contents are not consistent for " + subkey, r1Contents.get(subkey), r2Contents.get(subkey));
        } else {
          assertTrue(!r1Contents.containsKey(subkey));
        }
      }
    }
    
    
    // now we move on to testing deltas
    
    if (!getRegionAttributes().getScope().isDistributedNoAck()) { // no-ack doesn't support deltas 
      vm0.invoke(this.getClass(), "clearCCRegion");
      
      performOps = new SerializableRunnable("perform concurrent delta ops") {
        public void run() {
          try {
            long stopTime = System.currentTimeMillis() + 5000;
            Random ran = new Random(System.currentTimeMillis());
            while (System.currentTimeMillis() < stopTime) {
              for (int i=0; i<10; i++) {
                CCRegion.put("cckey" + i, new DeltaValue("ccvalue" + ran.nextInt()));
              }
            }
            long events = CCRegion.getCachePerfStats().getDeltaFailedUpdates();
            assertTrue("expected some failed deltas", events>0);
          } catch (CacheException e) {
            com.gemstone.gemfire.test.dunit.Assert.fail("while performing concurrent operations", e);
          }
        }
      };
  
      a0 = vm0.invokeAsync(performOps);
      a1 = vm1.invokeAsync(performOps);
      
      a0failed = waitForAsyncProcessing(a0, "expected some failed deltas");
      a1failed = waitForAsyncProcessing(a1, "expected some failed deltas");
  
      if (a0failed && a1failed) {
        fail("neither member saw failed deltas - check stats for " + name);
      }
  
      // check consistency of the regions
      r0Contents = (Map)vm0.invoke(this.getClass(), "getCCRegionContents");
      r1Contents = (Map)vm1.invoke(this.getClass(), "getCCRegionContents");
      r2Contents = (Map)vm2.invoke(this.getClass(), "getCCRegionContents");
      
      for (int i=0; i<10; i++) {
        String key = "cckey" + i;
        assertEquals("region contents are not consistent", r0Contents.get(key), r1Contents.get(key));
        assertEquals("region contents are not consistent", r1Contents.get(key), r2Contents.get(key));
        for (int subi=1; subi<3; subi++) {
          String subkey = key + "-" + subi;
          if (r0Contents.containsKey(subkey)) {
            assertEquals("region contents are not consistent", r0Contents.get(subkey), r1Contents.get(subkey));
            assertEquals("region contents are not consistent", r1Contents.get(subkey), r2Contents.get(subkey));
          } else {
            assertTrue(!r1Contents.containsKey(subkey));
          }
        }
      }
      
      // The region version vectors should now all be consistent with the version stamps in the entries.
      
      InternalDistributedMember vm0Id = (InternalDistributedMember)vm0.invoke(this.getClass(), "getMemberId");
      InternalDistributedMember vm1Id = (InternalDistributedMember)vm1.invoke(this.getClass(), "getMemberId");
      InternalDistributedMember vm2Id = (InternalDistributedMember)vm2.invoke(this.getClass(), "getMemberId");
  
      long start = System.currentTimeMillis();
      RegionVersionVector vm0vv = getVersionVector(vm0);
      long end = System.currentTimeMillis();
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("version vector transmission took " + (end-start) + " ms");
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("vm0 vector = " + vm0vv);
  
      RegionVersionVector vm1vv = getVersionVector(vm1);    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("vm1 vector = " + vm1vv);
      RegionVersionVector vm2vv = getVersionVector(vm2);    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("vm2 vector = " + vm2vv);
      
      Map<String, VersionTag> vm0Versions = (Map<String, VersionTag>)vm0.invoke(this.getClass(), "getCCRegionVersions");
      Map<String, VersionTag> vm1Versions = (Map<String, VersionTag>)vm1.invoke(this.getClass(), "getCCRegionVersions");
      Map<String, VersionTag> vm2Versions = (Map<String, VersionTag>)vm2.invoke(this.getClass(), "getCCRegionVersions");
  
      for (Map.Entry<String, VersionTag> entry: vm0Versions.entrySet()) {
        VersionTag tag = entry.getValue();
        tag.replaceNullIDs(vm0Id);
        assertTrue(vm0Id + " should contain " + tag, vm0vv.contains(tag.getMemberID(), tag.getRegionVersion()));
        assertTrue(vm1Id + " should contain " + tag, vm1vv.contains(tag.getMemberID(), tag.getRegionVersion()));
        assertTrue(vm2Id + " should contain " + tag, vm2vv.contains(tag.getMemberID(), tag.getRegionVersion()));
      }
  
      for (Map.Entry<String, VersionTag> entry: vm1Versions.entrySet()) {
        VersionTag tag = entry.getValue();
        tag.replaceNullIDs(vm1Id);
        assertTrue(vm0Id + " should contain " + tag, vm0vv.contains(tag.getMemberID(), tag.getRegionVersion()));
        assertTrue(vm1Id + " should contain " + tag, vm1vv.contains(tag.getMemberID(), tag.getRegionVersion()));
        assertTrue(vm2Id + " should contain " + tag, vm2vv.contains(tag.getMemberID(), tag.getRegionVersion()));
      }
  
      for (Map.Entry<String, VersionTag> entry: vm2Versions.entrySet()) {
        VersionTag tag = entry.getValue();
        tag.replaceNullIDs(vm2Id);
        assertTrue(vm0Id + " should contain " + tag, vm0vv.contains(tag.getMemberID(), tag.getRegionVersion()));
        assertTrue(vm1Id + " should contain " + tag, vm1vv.contains(tag.getMemberID(), tag.getRegionVersion()));
        assertTrue(vm2Id + " should contain " + tag, vm2vv.contains(tag.getMemberID(), tag.getRegionVersion()));
      }
    }
  }
  
  
  private RegionVersionVector getVersionVector(VM vm) throws Exception {
    byte[] serializedForm = (byte[])vm.invoke(this.getClass(), "getCCRegionVersionVector");
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serializedForm));
    return (RegionVersionVector)DataSerializer.readObject(dis);
  }
  
  
  
  
  
  /** comparison method that allows one or both arguments to be null */
  private boolean notEqual(Object o1, Object o2) {
    if (o1 == null) {
      return o2 != null;
    }
    if (o2 == null) {
      return true;
    }
    return !o1.equals(o2);
  }
  
  protected AsyncInvocation performOps4ClearWithConcurrentEvents(VM vm, final int msToRun) {
    SerializableRunnable performOps = new SerializableRunnable("perform concurrent ops") {
      public void run() {
        try {
          boolean includeClear = true;
          doOpsLoop(msToRun, includeClear);
        } catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("while performing concurrent operations", e);
        }
      }
    };
    return vm.invokeAsync(performOps);
  }

  protected void createRegionWithAttribute(VM vm, final String name, final boolean syncDiskWrite) {
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
      public void run() {
        try {
          RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
          f.setDiskSynchronous(syncDiskWrite);
          CCRegion = (LocalRegion)f.create(name);
        } catch (CacheException ex) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", ex);
        }
      }
    };
    vm.invoke(createRegion);
  }

  public void versionTestClearWithConcurrentEvents() throws Exception {
    z_versionTestClearWithConcurrentEvents(true);
  }

  public void versionTestClearWithConcurrentEventsAsync() throws Exception {
    z_versionTestClearWithConcurrentEvents(false);
  }

  protected void z_versionTestClearWithConcurrentEvents(boolean syncDiskWrite) throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2.  Afterward make
    // sure that all three regions are consistent
    
    final String name = this.getUniqueName() + "-CC";
    createRegionWithAttribute(vm0, name, syncDiskWrite);
    createRegionWithAttribute(vm1, name, syncDiskWrite);
    
    AsyncInvocation a0 = performOps4ClearWithConcurrentEvents(vm0, 5000);
    AsyncInvocation a1 = performOps4ClearWithConcurrentEvents(vm1, 5000);

    try { Thread.sleep(500); } catch (InterruptedException e) { fail("sleep was interrupted"); }
    
    createRegionWithAttribute(vm2, name, syncDiskWrite);
    
    waitForAsyncProcessing(a0, "");
    waitForAsyncProcessing(a1, "");

//    if (a0failed && a1failed) {
//      fail("neither member saw event conflation - check stats for " + name);
//    }
    
    // check consistency of the regions
    Map r0Contents = (Map)vm0.invoke(this.getClass(), "getCCRegionContents");
    Map r1Contents = (Map)vm1.invoke(this.getClass(), "getCCRegionContents");
    Map r2Contents = (Map)vm2.invoke(this.getClass(), "getCCRegionContents");
    
    for (int i=0; i<10; i++) {
      String key = "cckey" + i;
      assertEquals("region contents are not consistent", r0Contents.get(key), r1Contents.get(key));
      assertEquals("region contents are not consistent", r1Contents.get(key), r2Contents.get(key));
      for (int subi=1; subi<3; subi++) {
        String subkey = key + "-" + subi;
        if (r0Contents.containsKey(subkey)) {
          assertEquals("region contents are not consistent", r0Contents.get(subkey), r1Contents.get(subkey));
          assertEquals("region contents are not consistent", r1Contents.get(subkey), r2Contents.get(subkey));
        } else {
          assertTrue("expected containsKey("+subkey+") to return false", !r1Contents.containsKey(subkey));
        }
      }
    }
    
    vm0.invoke(this.getClass(), "assertNoClearTimeouts");
    vm1.invoke(this.getClass(), "assertNoClearTimeouts");
    vm2.invoke(this.getClass(), "assertNoClearTimeouts");
  }
  
  
  public void versionTestClearOnNonReplicateWithConcurrentEvents() throws Exception {
    if ( ! "bruces".equals(System.getProperty("user.name")) ) {
      // bug #45704, this test fails too often with this problem to be executed in all dunit runs
      return;
    }
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2.  Afterward make
    // sure that all three regions are consistent
    
    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
        public void run() {
          try {
            RegionFactory f = null;
            if (VM.getCurrentVMNum() == 0) {
              f = getCache().createRegionFactory(getRegionAttributes(RegionShortcut.REPLICATE_PROXY.toString()));
            } else if (VM.getCurrentVMNum() == 1) {
              f = getCache().createRegionFactory(getRegionAttributes());
              f.setDataPolicy(DataPolicy.NORMAL);
            } else {
              f = getCache().createRegionFactory(getRegionAttributes());
            }
            CCRegion = (LocalRegion)f.create(name);
          } catch (CacheException ex) {
            com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", ex);
          }
        }
      };
      
    vm0.invoke(createRegion); // empty region
    vm1.invoke(createRegion); // "normal" region
    vm2.invoke(createRegion); // replicate
    vm3.invoke(createRegion); // replicate
    
    
    SerializableRunnable performOps = new SerializableRunnable("perform concurrent ops") {
      public void run() {
        try {
          doOpsLoop(5000, true);
        } catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("while performing concurrent operations", e);
        }
//        } catch (InterruptedException e) {
//          fail("someone interrupted my sleep");
//        }
        if (CCRegion.getScope().isDistributedNoAck()) {
          sendSerialMessageToAll(); // flush the ops
        }
      }
    };
    
    AsyncInvocation a0 = vm0.invokeAsync(performOps);
    AsyncInvocation a1 = vm1.invokeAsync(performOps);

    try { Thread.sleep(500); } catch (InterruptedException e) { fail("sleep was interrupted"); }
    
    waitForAsyncProcessing(a0, "");
    waitForAsyncProcessing(a1, "");

    // check consistency of the regions
//    Map r0Contents = (Map)vm0.invoke(this.getClass(), "getCCRegionContents"); empty region
    Map r1Contents = (Map)vm1.invoke(this.getClass(), "getCCRegionContents"); // normal region
    Map r2Contents = (Map)vm2.invoke(this.getClass(), "getCCRegionContents"); // replicated
    Map r3Contents = (Map)vm3.invoke(this.getClass(), "getCCRegionContents"); // replicated
    
    for (int i=0; i<10; i++) {
      String key = "cckey" + i;
      // because datapolicy=normal regions do not accept ops when an entry is not present
      // they may miss invalidates/creates/destroys that other VMs see while applying ops
      // that they originate that maybe should have been elided.  For this reason we can't
      // guarantee their consistency and don't check for it here.
//      if (r1Contents.containsKey(key)) {
//        assertEquals("region contents are not consistent", r1Contents.get(key), r2Contents.get(key));
//      }
      assertEquals("region contents are not consistent for " + key, r2Contents.get(key), r3Contents.get(key));
      for (int subi=1; subi<3; subi++) {
        String subkey = key + "-" + subi;
        if (r2Contents.containsKey(subkey)) {
//          assertEquals("region contents are not consistent for " + subkey, r1Contents.get(subkey), r2Contents.get(subkey));
          assertEquals("region contents are not consistent for " + subkey, r2Contents.get(subkey), r3Contents.get(subkey));
        } else {
          // can't assert this because a clear() op will cause non-replicated to start rejecting
          // updates/creates from other members
//          assertTrue("expected r2 to not contain " + subkey, !r2Contents.containsKey(subkey));
        }
      }
    }
    // vm0 has no storage, but the others should have hit no timeouts waiting for
    // their region version vector to dominate the vector sent with the clear() operation.
    // If they did, then some ops were not properly recorded in the vector and something
    // is broken.
    vm1.invoke(this.getClass(), "assertNoClearTimeouts");
    vm2.invoke(this.getClass(), "assertNoClearTimeouts");
    vm3.invoke(this.getClass(), "assertNoClearTimeouts");
  }
  
  
  public void versionTestTombstones() {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numEntries = 1000;
    
    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2.  Afterward make
    // sure that all three regions are consistent
    final long oldServerTimeout = TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT;
    final long oldClientTimeout = TombstoneService.CLIENT_TOMBSTONE_TIMEOUT;
    final long oldExpiredTombstoneLimit = TombstoneService.EXPIRED_TOMBSTONE_LIMIT;
    final boolean oldIdleExpiration = TombstoneService.IDLE_EXPIRATION;
    try {
      SerializableRunnable setTimeout = new SerializableRunnable() {
        public void run() {
          TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT = 20000;
          TombstoneService.CLIENT_TOMBSTONE_TIMEOUT = 19000;
          TombstoneService.EXPIRED_TOMBSTONE_LIMIT = 1000;
          TombstoneService.IDLE_EXPIRATION = true;
        }
      };
      vm0.invoke(setTimeout);
      vm1.invoke(setTimeout);
      final String name = this.getUniqueName() + "-CC";
      SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
        public void run() {
          try {
            RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
            CCRegion = (LocalRegion)f.create(name);
            for (int i=0; i<numEntries; i++) {
              CCRegion.put("cckey"+i, "ccvalue");
            }
            if (CCRegion.getScope().isDistributedNoAck()) {
              sendSerialMessageToAll(); // flush the ops
            }
          } catch (CacheException ex) {
            com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", ex);
          }
        }
      };

      vm0.invoke(createRegion);
      vm1.invoke(createRegion);

      vm0.invoke(new SerializableRunnable("destroy entries and check tombstone count") {
        public void run() {
          try {
            for (int i=0; i<numEntries; i++) {
              CCRegion.destroy("cckey" + i);
              assertTrue("entry should not exist", !CCRegion.containsKey("cckey"+i));
              assertTrue("entry should not contain a value", !CCRegion.containsValueForKey("cckey"+i));
            }
            long count = CCRegion.getTombstoneCount();
            assertEquals("expected "+numEntries+" tombstones", numEntries, count);
            assertTrue("region should not contain a tombstone", !CCRegion.containsValue(Token.TOMBSTONE));
            if (CCRegion.getScope().isDistributedNoAck()) {
              sendSerialMessageToAll(); // flush the ops
            }
          } catch (CacheException e) {
            com.gemstone.gemfire.test.dunit.Assert.fail("while performing destroy operations", e);
          }
//          OSProcess.printStacks(0, getLogWriter(), false);
        }
      });

      vm1.invoke(new SerializableRunnable("check tombstone count(2)") {
        public void run() {
          final long count = CCRegion.getTombstoneCount();
          assertEquals("expected "+numEntries+" tombstones", numEntries, count);
          // ensure that some GC is performed - due to timing it may not
          // be the whole batch, but some amount should be done
          WaitCriterion waitForExpiration = new WaitCriterion() {
            @Override
            public boolean done() {
              return CCRegion.getTombstoneCount() < numEntries;
            }
            @Override
            public String description() {
              return "Waiting for some tombstones to expire.  There are now " + CCRegion.getTombstoneCount()
                + " tombstones left out of " + count + " initial tombstones";
            }
          };
          try {
            Wait.waitForCriterion(waitForExpiration, TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT+10000, 1000, true);
          } catch (AssertionFailedError e) {
            CCRegion.dumpBackingMap();
            com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("tombstone service state: " + CCRegion.getCache().getTombstoneService());
            throw e;
          }
        }
      });

      // Now check to see if tombstones are resurrected by a put/create.
      // The entries should be created okay and the callback should be afterCreate.
      // The tombstone count won't go down until the entries are swept, but then
      // the count should fall to zero.

      vm0.invoke(new SerializableRunnable("create/destroy entries and check tombstone count") {
        public void run() {
          double oldLimit = TombstoneService.GC_MEMORY_THRESHOLD;
          long count = CCRegion.getTombstoneCount();
          final long origCount = count;
          try {
            TombstoneService.GC_MEMORY_THRESHOLD = 0;  // turn this off so heap profile won't cause test to fail
            WaitCriterion waitForExpiration = new WaitCriterion() {
              @Override
              public boolean done() {
                return CCRegion.getTombstoneCount() ==  0;
              }
              @Override
              public String description() {
                return "Waiting for all tombstones to expire.  There are now " + CCRegion.getTombstoneCount()
                  + " tombstones left out of " + origCount + " initial tombstones";
              }
            };
            Wait.waitForCriterion(waitForExpiration, TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT+10000, 1000, true);
            logger.debug("creating tombstones.  current count={}", CCRegion.getTombstoneCount());
            for (int i=0; i<numEntries; i++) {
              CCRegion.create("cckey" + i, i);
              CCRegion.destroy("cckey" + i);
            }
            logger.debug("done creating tombstones.  current count={}", CCRegion.getTombstoneCount());
            count = CCRegion.getTombstoneCount();
            assertEquals("expected "+numEntries+" tombstones", numEntries, count);
            assertEquals(0, CCRegion.size());
            afterCreates = 0;
            AttributesMutator m = CCRegion.getAttributesMutator();
            m.addCacheListener(new CacheListenerAdapter() {
              @Override
              public void afterCreate(EntryEvent event) {
                afterCreates++;
              }
            });
            if (CCRegion.getScope().isDistributedNoAck()) {
              sendSerialMessageToAll(); // flush the ops
            }
          } catch (AssertionFailedError e) {
            CCRegion.dumpBackingMap();
            com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("tombstone service state: " + CCRegion.getCache().getTombstoneService());
            throw e;
          } catch (CacheException e) {
            com.gemstone.gemfire.test.dunit.Assert.fail("while performing create/destroy operations", e);
          } finally {
            TombstoneService.GC_MEMORY_THRESHOLD = oldLimit;
          }
        }
      });

      vm1.invoke(new SerializableRunnable("check tombstone count and install listener") {
        public void run() {
          long count = CCRegion.getTombstoneCount();
          assertEquals("expected ten tombstones", numEntries, count);
          afterCreates = 0;
          AttributesMutator m = CCRegion.getAttributesMutator();
          m.addCacheListener(new CacheListenerAdapter() {
            @Override
            public void afterCreate(EntryEvent event) {
              afterCreates++;
            }
          });
        }});

      vm0.invoke(new SerializableRunnable("create entries and check afterCreate and tombstone count") {
        public void run() {
          try {
            for (int i=0; i<numEntries; i++) {
              CCRegion.create("cckey" + i, i);
            }
            long count = CCRegion.getTombstoneCount();
            assertEquals("expected zero tombstones", 0, count);
            assertEquals("expected "+numEntries+" afterCreates", numEntries, afterCreates);
            assertEquals(numEntries, CCRegion.size());
            if (CCRegion.getScope().isDistributedNoAck()) {
              sendSerialMessageToAll(); // flush the ops
            }
          } catch (CacheException e) {
            com.gemstone.gemfire.test.dunit.Assert.fail("while performing create operations", e);
          }
        }
      });

      vm1.invoke(new SerializableRunnable("check afterCreate and tombstone count") {
        public void run() {
          long count = CCRegion.getTombstoneCount();
          assertEquals("expected zero tombstones", 0, count);
          assertEquals("expected "+numEntries+" afterCreates", numEntries, afterCreates);
          assertEquals(numEntries, CCRegion.size());
          // make sure the actual removal of the tombstones by the sweeper doesn't mess
          // up anything
          try {
            Thread.sleep(TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT + 5000);
          } catch (InterruptedException e) {
            fail("sleep was interrupted");
          }
          count = CCRegion.getTombstoneCount();
          assertEquals("expected zero tombstones", 0, count);
          assertEquals(numEntries, CCRegion.size());
        }
      });

      vm0.invoke(new SerializableRunnable("check region size and tombstone count") {
        public void run() {
          long count = CCRegion.getTombstoneCount();
          assertEquals("expected all tombstones to be expired", 0, count);
          assertEquals(numEntries, CCRegion.size());
        }
      });
    } finally {
      SerializableRunnable resetTimeout = new SerializableRunnable() {
        public void run() {
          TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT = oldServerTimeout;
          TombstoneService.CLIENT_TOMBSTONE_TIMEOUT = oldClientTimeout;
          TombstoneService.EXPIRED_TOMBSTONE_LIMIT = oldExpiredTombstoneLimit;
          TombstoneService.IDLE_EXPIRATION = oldIdleExpiration;
        }
      };
      vm0.invoke(resetTimeout);
      vm1.invoke(resetTimeout);
    }
  }
  

  
  
  
  
  
  
  /**
   * This tests the concurrency versioning system to ensure that event conflation
   * happens correctly and that the statistic is being updated properly
   */
  public void versionTestConcurrentEventsOnEmptyRegion() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3); // this VM, but treat as a remote for uniformity
    
    // create an empty region in vm0 and replicated regions in VM 1 and 3,
    // then perform concurrent ops
    // on the same key while creating the region in VM2.  Afterward make
    // sure that all three regions are consistent
    
    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
        public void run() {
          try {
            final RegionFactory f;
            if (VM.getCurrentVMNum() == 0) {
              f = getCache().createRegionFactory(getRegionAttributes(RegionShortcut.REPLICATE_PROXY.toString()));
            } else {
              f = getCache().createRegionFactory(getRegionAttributes());
            }
            CCRegion = (LocalRegion)f.create(name);
          } catch (CacheException ex) {
            com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", ex);
          }
        }
      };
      
    vm0.invoke(createRegion);
    vm1.invoke(createRegion);
    vm3.invoke(createRegion);
    
    
    SerializableRunnable performOps = new SerializableRunnable("perform concurrent ops") {
      public void run() {
        try {
          doOpsLoop(5000, false);
          sendSerialMessageToAll();
          if (CCRegion.getAttributes().getDataPolicy().withReplication()) {
            long events = CCRegion.getCachePerfStats().getConflatedEventsCount();
            assertTrue("expected some event conflation", events>0);
          }
        } catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("while performing concurrent operations", e);
        }
      }
    };
    
    AsyncInvocation a0 = vm0.invokeAsync(performOps);
    AsyncInvocation a1 = vm1.invokeAsync(performOps);

    try { Thread.sleep(500); } catch (InterruptedException e) { fail("sleep was interrupted"); }
    
    vm2.invoke(createRegion);
    
    boolean a0failed = waitForAsyncProcessing(a0, "expected some event conflation");
    boolean a1failed = waitForAsyncProcessing(a1, "expected some event conflation");

    if (a0failed && a1failed) {
      fail("neither member saw event conflation - check stats for " + name);
    }
    
    // check consistency of the regions
    Map r1Contents = (Map)vm1.invoke(this.getClass(), "getCCRegionContents");
    Map r2Contents = (Map)vm2.invoke(this.getClass(), "getCCRegionContents");
    Map r3Contents = (Map)vm3.invoke(this.getClass(), "getCCRegionContents");
    
    for (int i=0; i<10; i++) {
      String key = "cckey" + i;
      assertEquals("region contents are not consistent for " + key, r1Contents.get(key), r2Contents.get(key));
      assertEquals("region contents are not consistent for " + key, r2Contents.get(key), r3Contents.get(key));
      for (int subi=1; subi<3; subi++) {
        String subkey = key + "-" + subi;
        if (r1Contents.containsKey(subkey)) {
          assertEquals("region contents are not consistent for " + subkey, r1Contents.get(subkey), r2Contents.get(subkey));
          assertEquals("region contents are not consistent for " + subkey, r2Contents.get(subkey), r3Contents.get(subkey));
        } else {
          assertTrue(!r2Contents.containsKey(subkey));
          assertTrue(!r3Contents.containsKey(subkey));
        }
      }
    }
  }
  
  
  
  void doOpsLoop(int runTimeMs, boolean includeClear) {
    doOpsLoopNoFlush(runTimeMs, includeClear, true);
    if (CCRegion.getScope().isDistributedNoAck()) {
      sendSerialMessageToAll(); // flush the ops
    }
  }
  
  public static void doOpsLoopNoFlush(int runTimeMs, boolean includeClear, boolean includePutAll) {
    long stopTime = System.currentTimeMillis() + runTimeMs;
    Random ran = new Random(System.currentTimeMillis());
    String key = null;
    String value = null;
    String oldkey = null;
    String oldvalue = null;
    while (System.currentTimeMillis() < stopTime) {
      for (int i=0; i<10; i++) {
        oldkey = key;
        oldvalue = value;
        int v = ran.nextInt();
        key = "cckey" + i;
        value = "ccvalue"+v;
        try {
          switch (v & 0x7) {
          case 0:
            CCRegion.put(key, value);
            break;
          case 1:
            if (CCRegion.getAttributes().getDataPolicy().withReplication()) {
              if (oldkey != null) {
                CCRegion.replace(oldkey, oldvalue, value);
              }
              break;
            } // else fall through to putAll
            //$FALL-THROUGH$
          case 2:
            v = ran.nextInt();
            // too many putAlls make this test run too long
            if (includePutAll && (v & 0x7) < 3) {
              Map map = new HashMap();
              map.put(key, value);
              map.put(key+"-1", value);
              map.put(key+"-2", value);
              CCRegion.putAll(map, "putAllCallback");
            } else {
              CCRegion.put(key, value);
            }
            break;
          case 3:
            if (CCRegion.getAttributes().getDataPolicy().withReplication()) {
              // since we have a known key/value pair that was used before,
              // use the oldkey/oldvalue for remove(k,v)
              if (oldkey != null) {
                CCRegion.remove(oldkey, oldvalue);
              }
              break;
            } // else fall through to destroy
            //$FALL-THROUGH$
          case 4:
            CCRegion.destroy(key);
            break;
          case 5:
            if (includeClear) {
              CCRegion.clear();
              break;
            } else {
              if (CCRegion.getAttributes().getDataPolicy().withReplication()) {
                if (oldkey != null) {
                  CCRegion.putIfAbsent(oldkey, value);
                }
                break;
              } // else fall through to invalidate
            }
            //$FALL-THROUGH$
          case 6:
            CCRegion.invalidate(key);
            break;
          }
        } catch (EntryNotFoundException e) {
          // expected
        }
      }
    }
  }
  
  
  
  
  
  /**
   * This tests the concurrency versioning system to ensure that event conflation
   * happens correctly and that the statistic is being updated properly
   */
  public void versionTestConcurrentEventsOnNonReplicatedRegion() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3); // this VM, but treat as a remote for uniformity
    final boolean noAck = !getRegionAttributes().getScope().isAck();
    
    // create an empty region in vm0 and replicated regions in VM 1 and 3,
    // then perform concurrent ops
    // on the same key while creating the region in VM2.  Afterward make
    // sure that all three regions are consistent
    
    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
        public void run() {
          try {
            final RegionFactory f;
            if (VM.getCurrentVMNum() == 0) {
              f = getCache().createRegionFactory(getRegionAttributes(RegionShortcut.LOCAL.toString()));
              f.setScope(getRegionAttributes().getScope());
            } else {
              f = getCache().createRegionFactory(getRegionAttributes());
            }
            CCRegion = (LocalRegion)f.create(name);
          } catch (CacheException ex) {
            com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", ex);
          }
        }
      };
      
    vm0.invoke(createRegion);
    vm1.invoke(createRegion);
    vm3.invoke(createRegion);
    
    
    SerializableRunnable performOps = new SerializableRunnable("perform concurrent ops") {
      public void run() {
        try {
          doOpsLoop(5000, false);
          sendSerialMessageToAll();
          if (CCRegion.getAttributes().getDataPolicy().withReplication()) {
            long events = CCRegion.getCachePerfStats().getConflatedEventsCount();
            assertTrue("expected some event conflation", events>0);
          }
        } catch (CacheException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("while performing concurrent operations", e);
        }
      }
    };
    
    AsyncInvocation a0 = vm0.invokeAsync(performOps);
    AsyncInvocation a1 = vm1.invokeAsync(performOps);

    try { Thread.sleep(500); } catch (InterruptedException e) { fail("sleep was interrupted"); }
    
    vm2.invoke(createRegion);
    
    boolean a0failed = waitForAsyncProcessing(a0, "expected some event conflation");
    boolean a1failed = waitForAsyncProcessing(a1, "expected some event conflation");

    if (a0failed && a1failed) {
      fail("neither member saw event conflation - check stats for " + name);
    }
    
    // check consistency of the regions
    Map r0Contents = (Map)vm0.invoke(this.getClass(), "getCCRegionContents");
    Map r1Contents = (Map)vm1.invoke(this.getClass(), "getCCRegionContents");
    Map r2Contents = (Map)vm2.invoke(this.getClass(), "getCCRegionContents");
    Map r3Contents = (Map)vm3.invoke(this.getClass(), "getCCRegionContents");
    
    for (int i=0; i<10; i++) {
      String key = "cckey" + i;
      assertEquals("region contents are not consistent", r1Contents.get(key), r2Contents.get(key));
      assertEquals("region contents are not consistent", r2Contents.get(key), r3Contents.get(key));
//      if (r0Contents.containsKey(key)) {
//        assertEquals("region contents are not consistent", r1Contents.get(key), r0Contents.get(key));
//      }
      for (int subi=1; subi<3; subi++) {
        String subkey = key + "-" + subi;
        if (r1Contents.containsKey(subkey)) {
          assertEquals("region contents are not consistent", r1Contents.get(subkey), r2Contents.get(subkey));
          assertEquals("region contents are not consistent", r2Contents.get(subkey), r3Contents.get(subkey));
          if (r0Contents.containsKey(subkey)) {
            assertEquals("region contents are not consistent", r1Contents.get(subkey), r0Contents.get(subkey));
          }
        } else {
          assertTrue(!r2Contents.containsKey(subkey));
          assertTrue(!r3Contents.containsKey(subkey));
          assertTrue(!r0Contents.containsKey(subkey));
        }
      }
    }
    
    // a non-replicate region should not carry tombstones, so we'll check for that.
    // then we'll  use a cache loader and make sure that 1-hop is used to put the
    // entry into the cache in a replicate.  The new entry should have a version
    // of 1 when it's first created, the tombstone should have version 2 and
    // the loaded value should have version 3
    final String loadKey = "loadKey";
    vm0.invoke(new SerializableRunnable("add cache loader and create destroyed entry") {
      public void run() {
        CCRegion.getAttributesMutator().setCacheLoader(new CacheLoader() {
          public void close() {
          }
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("The test CacheLoader has been invoked for key '" + helper.getKey() + "'");
            return "loadedValue";
          }
        });
        
        CCRegion.put(loadKey, "willbeLoadedInitialValue");
        CCRegion.destroy(loadKey);
        if (noAck) { // flush for validation to work
          sendSerialMessageToAll();
        }
        // this assertion guarantees that non-replicated regions do not create tombstones.
        // this is currently not the case but is an open issue
        //assertTrue(CCRegion.getRegionEntry(loadKey) == null);
      }
    });
    vm1.invoke(new SerializableRunnable("confirm tombstone") {
      public void run() {
        assertTrue(Token.TOMBSTONE == CCRegion.getRegionEntry(loadKey).getValueInVM(CCRegion));
      }
    });
    vm0.invoke(new SerializableRunnable("use cache loader") {
      public void run() {
        assertEquals("loadedValue", CCRegion.get(loadKey));
        assertEquals(3, (CCRegion.getRegionEntry(loadKey)).getVersionStamp().getEntryVersion());
      }
    });
    if (!noAck) { // might be 3 or 4 with no-ack
      vm1.invoke(new SerializableRunnable("verify version number") {
        public void run() {
          assertEquals("loadedValue", CCRegion.get(loadKey));
          assertEquals(3, (CCRegion.getRegionEntry(loadKey)).getVersionStamp().getEntryVersion());
        }
      });
    }
  }
  
  
  public void versionTestGetAllWithVersions() {
    if (!getRegionAttributes().getScope().isAck()) {
      // this test has timing issues in no-ack scopes
      return;
    }
    VM vm0 = Host.getHost(0).getVM(0);
    VM vm1 = Host.getHost(0).getVM(1);
    
    final String regionName = getUniqueName() + "CCRegion";
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
      public void run() {
        try {
          final RegionFactory f;
          if (VM.getCurrentVMNum() == 0) {
            f = getCache().createRegionFactory(getRegionAttributes(RegionShortcut.LOCAL.toString()));
            f.setScope(getRegionAttributes().getScope());
          } else {
            f = getCache().createRegionFactory(getRegionAttributes());
          }
          CCRegion = (LocalRegion)f.create(regionName);
        } catch (CacheException ex) {
          com.gemstone.gemfire.test.dunit.Assert.fail("While creating region", ex);
        }
      }
    };
    
    vm0.invoke(createRegion);
    vm1.invoke(createRegion);
    vm1.invoke(new SerializableRunnable("Populate region and perform some ops") {
      public void run() {
        for (int i=0; i<100; i++) {
          CCRegion.put("cckey"+i, Integer.valueOf(i));
        }
        for (int i=0; i<100; i++) {
          CCRegion.put("cckey"+i, Integer.valueOf(i+1));
        }
      }
    });
    
    vm0.invoke(new SerializableRunnable("Perform getAll") {
      public void run() {
        List keys = new LinkedList();
        for (int i=0; i<100; i++) {
          keys.add("cckey"+i);
        }
        Map result = CCRegion.getAll(keys);
        assertTrue(result.size() == keys.size());
        LocalRegion r = CCRegion;
        for (int i=0; i<100; i++) {
          RegionEntry entry = r.getRegionEntry("cckey"+i);
          int stamp = entry.getVersionStamp().getEntryVersion();
          com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("checking key cckey" + i + " having version " + stamp + " entry=" + entry);
          assertEquals(2, stamp);
          assertEquals(result.get("cckey"+i), i+1);
        }
      }
    });
  }

  
  
  protected boolean waitForAsyncProcessing(AsyncInvocation async, String expectedError) {
    boolean failed = false;
    try {
      async.getResult();
    } catch (Throwable e) {
      if (e.getCause() instanceof RMIException) {
        Throwable e2 = e.getCause();
        if (e2.getCause() instanceof AssertionFailedError &&
            e2.getCause().getMessage().equals(expectedError)) {
          failed=true;
        }
      }
      if (!failed) {
        com.gemstone.gemfire.test.dunit.Assert.fail("asyncInvocation 0 returned exception", e);
      }
    }
    return failed;
  }
  
  /**
   * The number of milliseconds to try repeating validation code in the
   * event that AssertionFailedError is thrown.  For ACK scopes, no
   * repeat should be necessary.
   */
  protected long getRepeatTimeoutMs() {
    return 0;
  }
  
  public static void assertNoClearTimeouts() {
    // if there are timeouts on waiting for version vector dominance then
    // some operation was not properly recorded in the VM throwing this
    // assertion error.  All ops need to be recorded in the version vector
    assertEquals("expected there to be no timeouts - something is broken", 0, CCRegion.getCachePerfStats().getClearTimeouts());
  }
  
  
  public static void clearCCRegion() {
    CCRegion.clear();
  }
  
  public static Map getCCRegionContents() {
    Map result = new HashMap();
    for (Iterator i=CCRegion.entrySet().iterator(); i.hasNext(); ) {
      Region.Entry e = (Region.Entry)i.next();
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }
  
  
  /**
   * Since version vectors aren't java.io.Serializable we use DataSerializer
   * to return a serialized form of the vector
   */
  public static byte[] getCCRegionVersionVector() throws Exception {
    Object id = getMemberId();
    int vm = VM.getCurrentVMNum();
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("vm" + vm + " with id " + id + " copying " + CCRegion.getVersionVector().fullToString());
    RegionVersionVector vector = CCRegion.getVersionVector().getCloneForTransmission();
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("clone is " + vector);
    HeapDataOutputStream dos = new HeapDataOutputStream(3000, Version.CURRENT);
    DataSerializer.writeObject(vector, dos);
    byte[] bytes = dos.toByteArray();
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("serialized size is " + bytes.length);
    return bytes;
  }

  /**
   * returns a map of keys->versiontags for the test region
   */
  public static Map<String, VersionTag> getCCRegionVersions() {
    Map result = new HashMap();
    for (Iterator i=CCRegion.entrySet().iterator(); i.hasNext(); ) {
      Region.Entry e = (Region.Entry)i.next();
      result.put(e.getKey(), CCRegion.getRegionEntry(e.getKey()).getVersionStamp().asVersionTag());
    }
    return result;
  }


  public static InternalDistributedMember getMemberId() {
    return CCRegion.getDistributionManager().getDistributionManagerId();
  }


  /** a class for testing handling of concurrent delta operations */
  static class DeltaValue implements com.gemstone.gemfire.Delta, Serializable {
    
    private String value;
    
    public DeltaValue() {
    }
    
    public DeltaValue(String value) {
      this.value = value;
    }

    public boolean hasDelta() {
      return true;
    }

    public void toDelta(DataOutput out) throws IOException {
      out.writeUTF(this.value);
    }

    public void fromDelta(DataInput in) throws IOException,
        InvalidDeltaException {
      this.value = in.readUTF();
    }

    @Override
    public int hashCode() {
      return this.value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if ( !(obj instanceof DeltaValue) ) {
        return false;
      }
      return this.value.equals(((DeltaValue)obj).value);
    }

    @Override
    public String toString() {
      return this.value;
    }
    
  }

}
