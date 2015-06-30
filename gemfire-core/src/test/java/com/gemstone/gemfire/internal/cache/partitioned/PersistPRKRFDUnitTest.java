/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import java.util.Properties;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * Tests the basic use cases for PR persistence.
 * @author dsmith
 *
 */
public class PersistPRKRFDUnitTest extends PersistentPartitionedRegionTestBase {
  private static final int NUM_BUCKETS = 15;
  private static final int MAX_WAIT = 30 * 1000;
  static Object lockObject = new Object();
  
  public PersistPRKRFDUnitTest(String name) {
    super(name);
  }
  
  /**
   * do a put/modify/destroy while closing disk store
   * 
   * to turn on debug, add following parameter in local.conf:
   * hydra.VmPrms-extraVMArgs += "-Ddisk.KRF_DEBUG=true";
   */
  public void testCloseDiskStoreWhenPut() {
    final String title = "testCloseDiskStoreWhenPut:";
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
   
    createPR(vm0, 0);
    createData(vm0, 0, 10, "a");
    vm0.invoke(new CacheSerializableRunnable(title+"server add writer") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        // let the region to hold on the put until diskstore is closed
        if (!DiskStoreImpl.KRF_DEBUG) {
          region.getAttributesMutator().setCacheWriter(new MyWriter());
        }
      }
    });
    
    // create test
    AsyncInvocation async1 = vm0.invokeAsync(new CacheSerializableRunnable(title+"async create") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        ExpectedException expect = addExpectedException("CacheClosedException");
        try {
          region.put(10, "b");
          fail("Expect CacheClosedException here");
        } catch (CacheClosedException cce) {
          System.out.println(title+cce.getMessage());
          if (DiskStoreImpl.KRF_DEBUG) {
            assert cce.getMessage().contains("The disk store is closed.");
          } else {
            assert cce.getMessage().contains("The disk store is closed");
          }
        } finally {
          expect.remove();
        }
      }
    });
    vm0.invoke(new CacheSerializableRunnable(title+"close disk store") {
      public void run2() throws CacheException {
        GemFireCacheImpl gfc = (GemFireCacheImpl)getCache();
        pause(500);
        gfc.closeDiskStores();
        synchronized(lockObject) {
          lockObject.notify();
        }
      }
    });
    DistributedTestCase.join(async1, MAX_WAIT, getLogWriter());
    closeCache(vm0);
    
    // update
    createPR(vm0, 0);
    vm0.invoke(new CacheSerializableRunnable(title+"server add writer") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        // let the region to hold on the put until diskstore is closed
        if (!DiskStoreImpl.KRF_DEBUG) {
          region.getAttributesMutator().setCacheWriter(new MyWriter());
        }
      }
    });
    async1 = vm0.invokeAsync(new CacheSerializableRunnable(title+"async update") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        ExpectedException expect = addExpectedException("CacheClosedException");
        try {
          region.put(1, "b");
          fail("Expect CacheClosedException here");
        } catch (CacheClosedException cce) {
          System.out.println(title+cce.getMessage());
          if (DiskStoreImpl.KRF_DEBUG) {
            assert cce.getMessage().contains("The disk store is closed.");
          } else {
            assert cce.getMessage().contains("The disk store is closed");
          }
        } finally {
          expect.remove();
        }
      }
    });
    vm0.invoke(new CacheSerializableRunnable(title+"close disk store") {
      public void run2() throws CacheException {
        GemFireCacheImpl gfc = (GemFireCacheImpl)getCache();
        pause(500);
        gfc.closeDiskStores();
        synchronized(lockObject) {
          lockObject.notify();
        }
      }
    });
    DistributedTestCase.join(async1, MAX_WAIT, getLogWriter());
    closeCache(vm0);

    // destroy
    createPR(vm0, 0);
    vm0.invoke(new CacheSerializableRunnable(title+"server add writer") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        // let the region to hold on the put until diskstore is closed
        if (!DiskStoreImpl.KRF_DEBUG) {
          region.getAttributesMutator().setCacheWriter(new MyWriter());
        }
      }
    });
    async1 = vm0.invokeAsync(new CacheSerializableRunnable(title+"async destroy") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        ExpectedException expect = addExpectedException("CacheClosedException");
        try {
          region.destroy(2, "b");
          fail("Expect CacheClosedException here");
        } catch (CacheClosedException cce) {
          System.out.println(title+cce.getMessage());
          if (DiskStoreImpl.KRF_DEBUG) {
            assert cce.getMessage().contains("The disk store is closed.");
          } else {
            assert cce.getMessage().contains("The disk store is closed");
          }
        } finally {
          expect.remove();
        }
      }
    });
    vm0.invoke(new CacheSerializableRunnable(title+"close disk store") {
      public void run2() throws CacheException {
        GemFireCacheImpl gfc = (GemFireCacheImpl)getCache();
        pause(500);
        gfc.closeDiskStores();
        synchronized(lockObject) {
          lockObject.notify();
        }
      }
    });
    DistributedTestCase.join(async1, MAX_WAIT, getLogWriter());
    
    checkData(vm0, 0, 10, "a");
    checkData(vm0, 10, 11, null);
    closeCache(vm0);
  }

  private static class MyWriter extends CacheWriterAdapter implements Declarable {
    public MyWriter() {
    }

    public void init(Properties props) {
    }

    public void beforeCreate(EntryEvent event) {
      try {
        synchronized(lockObject) {
          lockObject.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    public void beforeUpdate(EntryEvent event) {
      try {
        synchronized(lockObject) {
          lockObject.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    public void beforeDestroy(EntryEvent event) {
      try {
        synchronized(lockObject) {
          lockObject.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
