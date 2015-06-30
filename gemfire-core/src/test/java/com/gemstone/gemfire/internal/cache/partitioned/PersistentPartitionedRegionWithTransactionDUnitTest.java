/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Tests the basic use cases for PR persistence.
 * @author dsmith
 *
 */
public class PersistentPartitionedRegionWithTransactionDUnitTest extends PersistentPartitionedRegionTestBase {

  private static final long MAX_WAIT = 0;

  public PersistentPartitionedRegionWithTransactionDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    invokeInEveryVM(new SerializableRunnable() {
      
      public void run() {
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = false;
        System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "false");
      }
    });
    
  }



  @Override
  public void setUp() throws Exception {
    super.setUp();
    invokeInEveryVM(new SerializableRunnable() {
      
      public void run() {
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = true;
        System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "true");
      }
    });
  }

  
  public void testRollback() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int redundancy = 1;
    
    int numBuckets = 50;
    
    createPR(vm0, redundancy);
    createPR(vm1, redundancy);
    createPR(vm2, redundancy);
    
    createData(vm0, 0, numBuckets, "a");
    
    createDataWithRollback(vm0, 0, numBuckets, "b");
    
    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);
    
    AsyncInvocation a1 = createPRAsync(vm0, redundancy);
    AsyncInvocation a2 = createPRAsync(vm1, redundancy);
    AsyncInvocation a3 = createPRAsync(vm2, redundancy);
    
    a1.getResult(MAX_WAIT);
    a2.getResult(MAX_WAIT);
    a3.getResult(MAX_WAIT);
    
    
    checkData(vm0, 0, numBuckets, "a");
    checkData(vm1, 0, numBuckets, "a");
    checkData(vm2, 0, numBuckets, "a");
    
    createData(vm0, 0, numBuckets, "b");
    
    checkData(vm0, 0, numBuckets, "b");
    checkData(vm1, 0, numBuckets, "b");
    checkData(vm2, 0, numBuckets, "b");
  }

  /**
   * @param vm0
   * @param i
   * @param numBuckets
   * @param string
   */
  private void createDataWithRollback(VM vm, final int startKey, final int endKey, final String value) {
    SerializableRunnable createData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        
        CacheTransactionManager tx = cache.getCacheTransactionManager();
        Region region = cache.getRegion(PR_REGION_NAME);
        
        for(int i =startKey; i < endKey; i++) {
          tx.begin();
          region.put(i, value);
          region.destroy(i + 113, value);
          region.invalidate(i + 113 * 2, value);
          tx.rollback();
        }
      }
    };
    vm.invoke(createData);
    
  }

  @Override
  protected void createData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    getLogWriter().info("creating runnable to create data for region " + regionName);
    SerializableRunnable createData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        getLogWriter().info("getting region " + regionName);
        Region region = cache.getRegion(regionName);
        
        for(int i =startKey; i < endKey; i++) {
          CacheTransactionManager tx = cache.getCacheTransactionManager();
          tx.begin();
          region.put(i, value);
          region.put(i + 113, value);
          region.put(i + 113 * 2, value);
          tx.commit();
        }
        { // add a destroy to make sure bug 43063 is fixed
          CacheTransactionManager tx = cache.getCacheTransactionManager();
          tx.begin();
          region.put(endKey+113*3, value);
          tx.commit();
          tx.begin();
          region.remove(endKey+113*3);
          tx.commit();
        }
      }
    };
    vm.invoke(createData);
  }

  @Override
  protected void checkData(VM vm0, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable checkData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        getLogWriter().info("checking data in " + regionName);
        Region region = cache.getRegion(regionName);
        
        for(int i =startKey; i < endKey; i++) {
          assertEquals(value, region.get(i));
          assertEquals(value, region.get(i + 113));
          assertEquals(value, region.get(i + 113 * 2));
        }
        assertEquals(null, region.get(endKey+113*3));
      }
    };
    
    vm0.invoke(checkData);
  }
}
