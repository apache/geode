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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This test is dunit test for the multiple Partition Regions in 4 VMs.
 * 
 * @author gthombar, modified by Tushar (for bug#35713)
 *  
 */
public class PartitionedRegionMultipleDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  /** Prefix is used in name of Partition Region */
  protected static String prPrefix = null;

  /** Maximum number of regions * */
  static int MAX_REGIONS = 1;

  /** Start index for destroying the kes */
  int startIndexForDestroy = 20;

  /** End index for destroying keys */
  int endIndexForDestroy = 40;

  /** Start index for key */
  int startIndexForKey = 0;

  /** End index for key */
  int endIndexForKey = 50;

  /** redundancy used for the creation of the partition region */
  final int redundancy = 0;

  /** local maxmemory used for the creation of the partition region */
  int localMaxMemory = 200;

  /** constructor */
  public PartitionedRegionMultipleDUnitTest(String name) {
    super(name);
  }

  /**
   * This test performs following operations: <br>
   * 1. Create multiple Partition Regions in 4 VMs</br><br>
   * 2. Validates the Partitioned region metadata by checking sizes of
   * allPartition Region, bucket2Node.</br><br>
   * 3. Performs put()operations on all the partitioned region from all the VM's
   * </br><br>
   * 4. Performs get() operations on all partitioned region and check the
   * returned values.</br>
   */
  public void testPartitionedRegionPutAndGet() throws Throwable
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    /** Prefix will be used for naming the partititon Region */
    prPrefix = "testPartitionedRegionPutAndGet";

    /** these indices represents range of partition regions present in each VM */
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;

    /** creationg and performing put(),get() operations on Partition Region */
    createMultiplePartitionRegion(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionPutAndGet() - Partition Regions Successfully Created ");
    validateMultiplePartitionedRegions(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionPutAndGet() - Partition Regions Successfully Validated ");
    putInMultiplePartitionedRegion(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionPutAndGet() - Put() Operation done Successfully in Partition Regions ");
    getInMultiplePartitionedRegion(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionPutAndGet() - Partition Regions Successfully Validated ");
  }

  /**
   * This test performs following operations: <br>
   * 1. Create multiple Partition Regions in 4
   * VMs</br><br>
   * 2. Performs put()operations on all the partitioned region from all the VM's
   * </br><br>
   * 3. Performs destroy(key)operations for some of the keys of all the
   * partitioned region from all the VM's</br><br>
   * 4. Performs get() operations for destroyed keys on all partitioned region
   * and checks the returned values is null .</br><br>
   * 5. Performs put()operations for the destroyed keys on all the partitioned
   * region from all the VM's</br><br>
   * 4. Performs get() operations for destroyed keys on all partitioned region
   * and checks the returned values is not null .</br>
   */
  public void testPartitionedRegionDestroyKeys() throws Throwable
  {

    Host host = Host.getHost(0);
    /** creating 4 VM's */
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    prPrefix = "testPartitionedRegionDestroyKeys";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int afterPutFlag = 0;

    /**
     * creating Partition Regions and performing put(), destroy(),get(),put()
     * operations in the sequence
     */
    createMultiplePartitionRegion(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyKeys() - Partition Regions Successfully Created ");
    validateMultiplePartitionedRegions(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyKeys() - Partition Regions Successfully Validated ");
    putInMultiplePartitionedRegion(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyKeys() - Put() Operation done Successfully in Partition Regions ");
    destroyInMultiplePartitionedRegion(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyKeys() - Destroy(Key) Operation done Successfully in Partition Regions ");
    getDestroyedEntryInMultiplePartitionedRegion(vm0, vm1, vm2, vm3,
        startIndexForRegion, endIndexForRegion, afterPutFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyKeys() - Get() Operation after destoy keys done Successfully in Partition Regions ");
    putDestroyedEntryInMultiplePartitionedRegion(vm0, vm1, vm2, vm3,
        startIndexForRegion, endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyKeys() - Put() Operation after destroy keys done Successfully in Partition Regions ");
    afterPutFlag = 1;
    getDestroyedEntryInMultiplePartitionedRegion(vm0, vm1, vm2, vm3,
        startIndexForRegion, endIndexForRegion, afterPutFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyKeys() - Get() Operation after Put() done Successfully in Partition Regions ");
  }

  /**
   * This test performs following operations: <br>
   * 1. Create multiple Partition Regions in 4
   * VMs</br><br>
   * 2. Performs put()operations on all the partitioned region from all the VM's
   * </br><br>
   * 3. Performs destroy(key)operations for some of the keys of all the
   * partitioned region from all the VM's</br><br>
   * 4. Chekcs containsKey and ContainsValueForKey APIs</br>
   */
  public void testPartitionedRegionDestroyAndContainsAPI() throws Throwable
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    prPrefix = "testPartitionedRegionDestroyAndContainsAPI";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];

    /** creating Partition Regions and testing for the APIs contains() */
    createMultiplePartitionRegion(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyAndContainsAPI() - Partition Regions Successfully Created ");
    validateMultiplePartitionedRegions(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyAndContainsAPI() - Partition Regions Successfully Validated ");
    putInMultiplePartitionedRegion(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyAndContainsAPI() - Put() Operation done Successfully in Partition Regions ");
    destroyInMultiplePartitionedRegion(vm0, vm1, vm2, vm3, startIndexForRegion,
        endIndexForRegion);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyAndContainsAPI() - Destroy(Key) Operation done Successfully in Partition Regions ");
    async[0] = vm0.invokeAsync(validateContainsAPIForPartitionRegion(
        startIndexForRegion, endIndexForRegion));
    async[1] = vm1.invokeAsync(validateContainsAPIForPartitionRegion(
        startIndexForRegion, endIndexForRegion));
    async[2] = vm2.invokeAsync(validateContainsAPIForPartitionRegion(
        startIndexForRegion, endIndexForRegion));
    async[3] = vm3.invokeAsync(validateContainsAPIForPartitionRegion(
        startIndexForRegion, endIndexForRegion));

    for (int count = 0; count < AsyncInvocationArrSize; count++) {
        ThreadUtils.join(async[count], 120 * 1000);
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        Assert.fail("exception during " + count, async[count].getException());
      }
   }

    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionedRegionDestroyAndContainsAPI() - Validation of Contains APIs done Successfully in Partition Regions ");
  }

  /**
   * This function creates multiple partition regions in 4 VMs. The range is specified with parameters
   * startIndexForRegion, endIndexForRegion
   */
  private void createMultiplePartitionRegion(VM vm0, VM vm1, VM vm2, VM vm3,
      int startIndexForRegion, int endIndexForRegion)
  {
//    int AsyncInvocationArrSize = 8;
//    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];

    vm0.invoke(createMultiplePartitionRegion(prPrefix, startIndexForRegion,
        endIndexForRegion, redundancy, localMaxMemory));
    vm1.invoke(createMultiplePartitionRegion(prPrefix, startIndexForRegion,
        endIndexForRegion, redundancy, localMaxMemory));
    vm2.invoke(createMultiplePartitionRegion(prPrefix, startIndexForRegion,
        endIndexForRegion, redundancy, localMaxMemory));
    vm3.invoke(createMultiplePartitionRegion(prPrefix, startIndexForRegion,
        endIndexForRegion, redundancy, localMaxMemory));
  }

  /**
   * <br>
   * This function performs following checks on allPartitionRegion and
   * bucket2Node region of Partition Region</br><br>
   * 1. allPartitionRegion should not be null</br><br>
   * 2. Size of allPartitionRegion should be no. of regions + 1.</br><br>
   * 3. Bucket2Node should not be null and size should = no. of regions</br>
   * <br>
   * 4. Name of the Bucket2Node should be
   * PartitionedRegionHelper.BUCKET_2_NODE_TABLE_PREFIX + pr.getName().</br>
   */
  private void validateMultiplePartitionedRegions(VM vm0, VM vm1, VM vm2,
      VM vm3, int startIndexForRegion, int endIndexForRegion) throws Throwable
  {
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm0.invokeAsync(validateMultiplePartitionRegion(prPrefix,
        startIndexForRegion, endIndexForRegion));
    async[1] = vm1.invokeAsync(validateMultiplePartitionRegion(prPrefix,
        startIndexForRegion, endIndexForRegion));
    async[2] = vm2.invokeAsync(validateMultiplePartitionRegion(prPrefix,
        startIndexForRegion, endIndexForRegion));
    async[3] = vm3.invokeAsync(validateMultiplePartitionRegion(prPrefix,
        startIndexForRegion, endIndexForRegion));

    for (int count = 0; count < AsyncInvocationArrSize; count++) {
        ThreadUtils.join(async[count], 30 * 1000);
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        Assert.fail("exception during " + count, async[count].getException());
      }
    }
  }

  /**
   * This function performs put() operations in multiple Partition Regions.
   * Range of the keys which are put is 0 to 400. Each Vm puts different set of
   * keys
   */
  private void putInMultiplePartitionedRegion(VM vm0, VM vm1, VM vm2, VM vm3,
      int startIndexForRegion, int endIndexForRegion) throws Throwable
  {
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    int delta = (endIndexForKey - startIndexForKey) / 4;
    async[0] = vm0.invokeAsync(putInMultiplePartitionRegion(prPrefix,
        startIndexForKey, startIndexForKey + 1 * delta, startIndexForRegion,
        endIndexForRegion));
    async[1] = vm1.invokeAsync(putInMultiplePartitionRegion(prPrefix,
        startIndexForKey + 1 * delta, startIndexForKey + 2 * delta,
        startIndexForRegion, endIndexForRegion));
    async[2] = vm2.invokeAsync(putInMultiplePartitionRegion(prPrefix,
        startIndexForKey + 2 * delta, startIndexForKey + 3 * delta,
        startIndexForRegion, endIndexForRegion));
    async[3] = vm3.invokeAsync(putInMultiplePartitionRegion(prPrefix,
        startIndexForKey + 3 * delta, endIndexForKey, startIndexForRegion,
        endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
        ThreadUtils.join(async[count], 30 * 1000);
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        Assert.fail("exception during " + count, async[count].getException());
      }
    }
  }

  /**
   * This function performs get() operations in multiple Partition Regions. Each
   * Vm gets keys from 0 to 400.
   */
  private void getInMultiplePartitionedRegion(VM vm0, VM vm1, VM vm2, VM vm3,
      int startIndexForRegion, int endIndexForRegion) throws Throwable
  {
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm0.invokeAsync(getInMultiplePartitionRegion(prPrefix,
        startIndexForKey, endIndexForKey, startIndexForRegion,
        endIndexForRegion));
    async[1] = vm1.invokeAsync(getInMultiplePartitionRegion(prPrefix,
        startIndexForKey, endIndexForKey, startIndexForRegion,
        endIndexForRegion));
    async[2] = vm2.invokeAsync(getInMultiplePartitionRegion(prPrefix,
        startIndexForKey, endIndexForKey, startIndexForRegion,
        endIndexForRegion));
    async[3] = vm3.invokeAsync(getInMultiplePartitionRegion(prPrefix,
        startIndexForKey, endIndexForKey, startIndexForRegion,
        endIndexForRegion));
    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) { 
        ThreadUtils.join(async[count], 30 * 1000);
    }
     
    for (int count = 0; count < AsyncInvocationArrSize; count++) { 
      if (async[count].exceptionOccurred()) {
        Assert.fail("Failed due to exception: "+ async[count].getException(),
            async[count].getException());
      }
    }  
  }

  /**
   * This function performs destroy(key) operations in multiple Partiton
   * Regions. The range of keys to be destroyed is from 100 to 200. Each Vm
   * destroys different set of the keys.
   */
  private void destroyInMultiplePartitionedRegion(VM vm0, VM vm1, VM vm2,
      VM vm3, int startIndexForRegion, int endIndexForRegion)throws Throwable
  {

    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    int delta = (endIndexForDestroy - startIndexForDestroy) / 4;

    async[0] = vm0.invokeAsync(destroyInMultiplePartitionRegion(prPrefix,
        startIndexForDestroy, startIndexForDestroy + 1 * delta,
        startIndexForRegion, endIndexForRegion));
    async[1] = vm1.invokeAsync(destroyInMultiplePartitionRegion(prPrefix,
        startIndexForDestroy + 1 * delta, startIndexForDestroy + 2 * delta,
        startIndexForRegion, endIndexForRegion));
    async[2] = vm2.invokeAsync(destroyInMultiplePartitionRegion(prPrefix,
        startIndexForDestroy + 2 * delta, startIndexForDestroy + 3 * delta,
        startIndexForRegion, endIndexForRegion));
    async[3] = vm3.invokeAsync(destroyInMultiplePartitionRegion(prPrefix,
        startIndexForDestroy + 3 * delta, endIndexForDestroy,
        startIndexForRegion, endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
        ThreadUtils.join(async[count], 30 * 1000);
    }

    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        Assert.fail("exception during " + count, async[count].getException());
      }
   }   
  }

  /**
   * This function returns CacheSerializableRunnable Object which checks
   * contains() and containsValueForKey() APIs.
   */
  private CacheSerializableRunnable validateContainsAPIForPartitionRegion(
      final int startIndexForRegion, final int endIndexForRegion)
  {
    CacheSerializableRunnable validateRegionAPIs = new CacheSerializableRunnable(
        "validateInserts") {
      String innerprPrefix = prPrefix;

      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      int innerStartIndexForKey = startIndexForKey;

      int innerEndIndexForKey = endIndexForKey;

      int innerStartIndexForDestroy = startIndexForDestroy;

      int innerEndIndexForDestroy = endIndexForDestroy;

      public void run2() throws CacheException
      {
        Cache cache = getCache();
        // Get Validation.
        for (int j = innerStartIndexForRegion; j < innerEndIndexForRegion; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + innerprPrefix + (j));
          assertNotNull(pr);
          assertEquals(pr.getName(), innerprPrefix + (j));
          for (int i = innerStartIndexForKey; i < innerEndIndexForKey; i++) {
            Object val = null;
            val = pr.get(j + innerprPrefix + i);
            if (i >= innerStartIndexForDestroy && i < innerEndIndexForDestroy) {
              assertNull(val);
            }
            else if (val != null) {
              assertEquals(val, innerprPrefix + i);
              assertTrue(pr.containsValue(innerprPrefix+i));
              // pass
            }
            else {
              fail("Validation failed for key = " + j + innerprPrefix + i
                  + "Value got = " + val);
            }
          }

          LogWriterUtils.getLogWriter()
              .info(
                  "validateContainsAPIForPartitionRegion() - Get() Validations done Successfully in Partition Region "
                      + pr.getName());
          // containsKey validation.
          for (int i = innerStartIndexForKey; i < innerEndIndexForKey; i++) {
            boolean conKey = pr.containsKey(j + innerprPrefix + i);
            if (i >= innerStartIndexForDestroy && i < innerEndIndexForDestroy) {
              assertFalse(conKey);
            }
            else {
              assertTrue(conKey);
            }
          }

          LogWriterUtils.getLogWriter()
              .info(
                  "validateContainsAPIForPartitionRegion() - containsKey() Validations done Successfully in Partition Region "
                      + pr.getName());

          // containsValueForKey
          for (int i = innerStartIndexForKey; i < innerEndIndexForKey; i++) {
            boolean conKey = pr.containsValueForKey(j + innerprPrefix + i);
            if (i >= innerStartIndexForDestroy && i < innerEndIndexForDestroy) {
              assertFalse(conKey);
            }
            else {
              assertTrue(conKey);
            }
          }
          LogWriterUtils.getLogWriter()
              .info(
                  "validateContainsAPIForPartitionRegion() - containsValueForKey() Validations done Successfully in Partition Region "
                      + pr.getName());
          // containsValue
          for (int i = innerStartIndexForKey; i < innerEndIndexForKey; i++) {
            boolean conKey = pr.containsValue(innerprPrefix + i);
            if (i >= innerStartIndexForDestroy && i < innerEndIndexForDestroy) {
              assertFalse(conKey);
            }
            else {
              assertTrue(conKey);
            }
          }
          LogWriterUtils.getLogWriter()
              .info(
                  "validateContainsAPIForPartitionRegion() - containsValue() Validations done Successfully in Partition Region "
                      + pr.getName());
        }
      }
    };
    return validateRegionAPIs;
  }

  /**
   * This function performs get() operations in multiple Partition Regions after
   * destroy of keys. Each Vm gets keys from 100 to 200.
   */
  private void getDestroyedEntryInMultiplePartitionedRegion(VM vm0, VM vm1,
      VM vm2, VM vm3, int startIndexForRegion, int endIndexForRegion,
      int afterPutFlag) throws Throwable
  {
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];

    async[0] = vm0.invokeAsync(getRemovedOrDestroyedInMultiplePartitionRegion(
        prPrefix, startIndexForDestroy, endIndexForDestroy,
        startIndexForRegion, endIndexForRegion, afterPutFlag));
    async[1] = vm1.invokeAsync(getRemovedOrDestroyedInMultiplePartitionRegion(
        prPrefix, startIndexForDestroy, endIndexForDestroy,
        startIndexForRegion, endIndexForRegion, afterPutFlag));
    async[2] = vm2.invokeAsync(getRemovedOrDestroyedInMultiplePartitionRegion(
        prPrefix, startIndexForDestroy, endIndexForDestroy,
        startIndexForRegion, endIndexForRegion, afterPutFlag));
    async[3] = vm3.invokeAsync(getRemovedOrDestroyedInMultiplePartitionRegion(
        prPrefix, startIndexForDestroy, endIndexForDestroy,
        startIndexForRegion, endIndexForRegion, afterPutFlag));
    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
        ThreadUtils.join(async[count], 30 * 1000);
        if (async[count].exceptionOccurred()) {
          Assert.fail("exception during " + count, async[count].getException());
        }
    }
    
//    for (int count = 0; count < AsyncInvocationArrSize; count++) {
//      async[count].join();
//    }
//    
//    for (int count = 0; count < AsyncInvocationArrSize; count++) {
//      if (async[count].exceptionOccurred()) {
//        fail("exception during " + count, async[count].getException());
//      }
//    }
  }

  /**
   * This function performs put() operations in multiple Partition Regions after
   * destroy of keys. Range of the keys which are put is 100 to 200. Each Vm
   * puts different set of keys
   */
  private void putDestroyedEntryInMultiplePartitionedRegion(VM vm0, VM vm1,
      VM vm2, VM vm3, int startIndexForRegion, int endIndexForRegion) throws Throwable
  {
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    int delta = (endIndexForDestroy - startIndexForDestroy) / 4;
    async[0] = vm0.invokeAsync(putInMultiplePartitionRegion(prPrefix,
        startIndexForDestroy, startIndexForDestroy + delta * 1,
        startIndexForRegion, endIndexForRegion));
    async[1] = vm1.invokeAsync(putInMultiplePartitionRegion(prPrefix,
        startIndexForDestroy + delta * 1, startIndexForDestroy + delta * 2,
        startIndexForRegion, endIndexForRegion));
    async[2] = vm2.invokeAsync(putInMultiplePartitionRegion(prPrefix,
        startIndexForDestroy + delta * 2, startIndexForDestroy + delta * 3,
        startIndexForRegion, endIndexForRegion));
    async[3] = vm3.invokeAsync(putInMultiplePartitionRegion(prPrefix,
        startIndexForDestroy + delta * 3, endIndexForDestroy,
        startIndexForRegion, endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
        ThreadUtils.join(async[count], 30 * 1000);
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        Assert.fail("exception during " + count, async[count].getException());
      }
    }
  }
}