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

import java.util.concurrent.locks.Lock;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This class tests distributed locking of global region entries.
 */
public class GlobalLockingDUnitTest extends CacheTestCase {

  public static Region region_testBug32356;
  
  public GlobalLockingDUnitTest(String name) {
    super(name);
  }

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected RegionAttributes getGlobalAttrs() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    return factory.create();
  }

  protected Region getOrCreateRootRegion() {
    Region root = getRootRegion();
    if (root == null) {
      try {
        root = createRootRegion(getGlobalAttrs());
      } catch (RegionExistsException ex) {
        fail("Huh?");
      } catch (TimeoutException ex) {
        fail(ex.toString());
      }
    }
    return root;
  }

//////////////////////  Test Methods  //////////////////////

  /** 
   * Tests for 32356 R2 tryLock w/ 0 timeout broken in Distributed Lock Service
   */
  public void testBug32356() throws Exception {
    LogWriterUtils.getLogWriter().fine("[testBug32356]");
    Host host = Host.getHost(0);
    final String name = this.getUniqueName();
    final Object key = "32356";

    // lock/unlock '32356' in all vms... (make all vms aware of token)
    LogWriterUtils.getLogWriter().fine("[testBug32356] lock/unlock '32356' in all vms");
    for (int i = 0; i < 4; i++) {
      final int vm = i;
      host.getVM(vm).invoke(new CacheSerializableRunnable("testBug32356_step1") {
        public void run2() throws CacheException {
          region_testBug32356 =
              getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
          Lock lock = region_testBug32356.getDistributedLock(key);
          lock.lock();
          lock.unlock();
        }
      });
    }

    // attempt try-lock of zero wait time in all vms
    LogWriterUtils.getLogWriter().fine("[testBug32356] attempt try-lock of zero wait time in all vms");
    for (int i = 0; i < 4; i++) {
      final int vm = i;
      host.getVM(vm).invoke(new CacheSerializableRunnable("testBug32356_step2") {
        public void run2() throws CacheException {
          Lock lock = region_testBug32356.getDistributedLock(key);
          // bug 32356 should cause this to fail...
          assertTrue("Found bug 32356", lock.tryLock()); 
          lock.unlock();
        }
      });
    }
  }

  public void testNonGlobalRegion() throws CacheException {
    String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory(getGlobalAttrs());
    factory.setScope(Scope.LOCAL);
    Region region = getOrCreateRootRegion().createSubregion(name + "LOCAL", factory.create());
    try {
      region.getDistributedLock("obj");
      fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException ex) {
      // pass...
    }
    factory.setScope(Scope.DISTRIBUTED_ACK);
    region = getOrCreateRootRegion().createSubregion(name + "DACK", factory.create());
    try {
      region.getDistributedLock("obj");
      fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException ex) {
      // pass...
    }
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    region = getOrCreateRootRegion().createSubregion(name + "DNOACK", factory.create());
    try {
      region.getDistributedLock("obj");
      fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException ex) {
      // pass...
    }
  }

  public void testSingleVMLockUnlock() throws CacheException {
    String name = this.getUniqueName() + "-GLOBAL";
    Region region = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());

    Lock lock = region.getDistributedLock("obj");
    lock.lock();
    lock.unlock();
  }
  
  public void testIsLockGrantorAttribute() throws Exception {
    String name = this.getUniqueName() + "-testIsLockGrantorAttribute";
    AttributesFactory factory = new AttributesFactory(getGlobalAttrs());
    factory.setLockGrantor(true);
    Region region = getOrCreateRootRegion().createSubregion(
        name, factory.create());
    assertEquals(
        "Setting isLockGrantor failed to result in becoming lock grantor", 
        true, 
        ((com.gemstone.gemfire.internal.cache.DistributedRegion)
            region).getLockService().isLockGrantor());
  }

  /**
   * Get the lock in one VM, try to create in other
   */
  public void testCreateLockTimeout() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName();
    final Object key = new Integer(5);
    vm0.invoke(new CacheSerializableRunnable("Get lock") {
      public void run2() throws CacheException {
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        Lock lock = r.getDistributedLock(key);
        lock.lock();
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Lock timeout creating entry") {
      public void run2() throws CacheException {
        getOrCreateRootRegion().getCache().setLockTimeout(2);
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        try {
          r.create(key, "the value");
          fail("create() should have thrown TimeoutException");
        } catch (TimeoutException ex) {
          // pass
        }
      }
    });

  }

  /**
   * get the lock in one VM, try to put() in other
   */
  public void testPutLockTimeout() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName();
    final Object key = new Integer(5);
    vm0.invoke(new CacheSerializableRunnable("Get lock") {
      public void run2() throws CacheException {
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        Lock lock = r.getDistributedLock(key);
        lock.lock();
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Lock timeout putting entry") {
      public void run2() throws CacheException {
        getOrCreateRootRegion().getCache().setLockTimeout(2);
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        try {
          r.put(key, "the value");
          fail("put() should have thrown TimeoutException");
        } catch (TimeoutException ex) {
          // pass
        }
      }
    });
  }
  
  /**
   * get lock in one VM, try to invoke loader in other
   */
  public void testLoadLockTimeout() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName();
    final Object key = new Integer(5);
    
    // In first VM, get a lock on the entry
    vm0.invoke(new CacheSerializableRunnable("Get lock") {
      public void run2() throws CacheException {
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        Lock lock = r.getDistributedLock(key);
        lock.lock();
      }
    });

    // In second VM, do a get that tries to invoke a loader
    vm1.invoke(new CacheSerializableRunnable("Lock timeout local loader") {
      public void run2() throws CacheException {
        getOrCreateRootRegion().getCache().setLockTimeout(2);
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        r.getAttributesMutator().setCacheLoader(new CacheLoader() {
          public Object load(LoaderHelper helper)
          throws CacheLoaderException {
            throw new CacheLoaderException("Made it into the loader!");
          }
          public void close() {}
        });
        try {
          r.get(key);
          fail("get() should have thrown TimeoutException");
        } catch (TimeoutException ex) {
          // pass
        }
      }
    });
  }
  
  /**
   * get lock in one VM, try to invalidate in other
   */
  public void testInvalidateLockTimeout() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName();
    final Object key = new Integer(5);
    vm0.invoke(new CacheSerializableRunnable("Get lock") {
      public void run2() throws CacheException {
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        Lock lock = r.getDistributedLock(key);
        lock.lock();
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Lock timeout invalidating entry") {
      public void run2() throws CacheException {
        getOrCreateRootRegion().getCache().setLockTimeout(2);
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        try {
          r.invalidate(key);
          fail("invalidate() should have thrown TimeoutException");
        } catch (TimeoutException ex) {
          // pass
        }
      }
    });
  }
  
  /**
   * get lock in one VM, try to destroy in other
   */
  public void testDestroyLockTimeout() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName();
    final Object key = new Integer(5);
    vm0.invoke(new CacheSerializableRunnable("Get lock") {
      public void run2() throws CacheException {
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        Lock lock = r.getDistributedLock(key);
        lock.lock();
        r.put(key, "value");
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Lock timeout destroying entry") {
      public void run2() throws CacheException {
        getOrCreateRootRegion().getCache().setLockTimeout(2);
        Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        r.get(key);
        try {
          r.destroy(key);
          fail("destroy() should have thrown TimeoutException");
        } catch (TimeoutException ex) {
          // pass
        }
      }
    });
  }
  
  /**
   * get the lock, region.get(), region.put(), release lock
   */
  public void testLockGetPut() throws CacheException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName() + "-GLOBAL";
    final Object key = new Integer(5);

    // First, create region & entry, and lock the entry, in Master VM
    Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
    Lock lock = r.getDistributedLock(key);
    lock.lock();
    r.create(key, "value 1");
    assertEquals("value 1", r.get(key));

    // Now, make sure a locking operation times out in another VM
    vm0.invoke(new CacheSerializableRunnable("Unsuccessful locking operation") {
      public void run2() throws CacheException {
        try {
          getOrCreateRootRegion().getCache().setLockTimeout(2);
          Region r2 = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
          assertEquals("value 1", r2.get(key));
          r2.put(key, "wrong value");
          fail("put() should have thrown TimeoutException");
        } catch (TimeoutException ex) {
          // pass
        }
      }
    });
    
    // Now, in Master, do another locking operation, then release the lock
    r.put(key, "value 2");
    lock.unlock();
    
    // Finally, successfully perform a locking in other VM
    vm1.invoke(new CacheSerializableRunnable("Successful locking operation") {
      public void run2() throws CacheException {
        getOrCreateRootRegion().getCache().setLockTimeout(2);
        Region r2 = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
        assertEquals("value 2", r2.get(key));
        r2.put(key, "value 3");
      }
    });
    
    assertEquals("value 3", r.get(key));

  }
  
  /**
   * Test Region.getRegionDistributedLock(), calling lock() and then unlock()
   */
  public void testRegionDistributedLockSimple() throws CacheException
  {
    final String name = this.getUniqueName();
    Region r = getOrCreateRootRegion().createSubregion(name, getGlobalAttrs());
    Lock lock = r.getRegionDistributedLock();
    lock.lock();
    lock.unlock();
  }

}
