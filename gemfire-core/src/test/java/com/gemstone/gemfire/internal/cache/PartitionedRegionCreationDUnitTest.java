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

/**
 * This test is to test and validate the partitioned region creation in multiple
 * vm scenario. This will verify the functionality under distributed scenario.
 */

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

@SuppressWarnings("serial")
public class PartitionedRegionCreationDUnitTest extends
    PartitionedRegionDUnitTestCase
{
  /**
   * constructor
   * 
   * @param name
   */
  public PartitionedRegionCreationDUnitTest(String name) {

    super(name);
  }

  static Properties props = new Properties();

  static final int MAX_REGIONS = 1;

  static final int totalNumBuckets = 7;
  /**
   * This tests creates partition regions with scope = DISTRIBUTED_ACK and then
   * validating thoes partition regions
   */
  public void testSequentialCreation() throws Exception
  {
    getLogWriter().info("*****CREATION TEST ACK STARTED*****");
    final String name = getUniqueName();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    for (int cnt = 0; cnt < MAX_REGIONS; cnt++) {
      vm0.invoke(getCacheSerializableRunnableForPRCreate(name
          + String.valueOf(cnt), 0, 0, "NONE"));
      vm1.invoke(getCacheSerializableRunnableForPRCreate(name
          + String.valueOf(cnt), 0, 0, "NONE"));
      vm2.invoke(getCacheSerializableRunnableForPRCreate(name
          + String.valueOf(cnt), 0, 0, "NONE"));
      vm3.invoke(getCacheSerializableRunnableForPRCreate(name
          + String.valueOf(cnt), 0, 0, "NONE"));
    }

    // validating that regions are successfully created
    vm0.invoke(getCacheSerializableRunnableForPRValidate(name));
    vm1.invoke(getCacheSerializableRunnableForPRValidate(name));
    vm2.invoke(getCacheSerializableRunnableForPRValidate(name));
    vm3.invoke(getCacheSerializableRunnableForPRValidate(name));
    getLogWriter().info("*****CREATION TEST ACK ENDED*****");
  }

  /**
   * This test create regions with scope = DISTRIBUTED_NO_ACK and then
   * validating these partition regons
   * 
   * @throws Exception
   */
  // TODO: fix the hang that concurent creation often runs into -- mthomas
  // 2/8/06
  public void testConcurrentCreation() throws Throwable
  {
    getLogWriter().info("*****CREATION TEST NO_ACK STARTED*****");
    final String name = getUniqueName();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm0.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));
    async[1] = vm1.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));
    async[2] = vm2.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));
    async[3] = vm3.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }

    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during " + count, async[count].getException());
      }
    }
    
    // //validating that regions are successfully created
    vm0.invoke(getCacheSerializableRunnableForPRValidate(name));
    vm1.invoke(getCacheSerializableRunnableForPRValidate(name));
    vm2.invoke(getCacheSerializableRunnableForPRValidate(name));
    vm3.invoke(getCacheSerializableRunnableForPRValidate(name));
    getLogWriter().info("*****CREATION TEST NO_ACK ENDED*****");
  }

  /**
   * This test create regions with scope = DISTRIBUTED_NO_ACK and then
   * validating these partition regons. Test specially added for SQL fabric
   * testing since that always creates regions in parallel.
   * 
   * @throws Exception
   */
  public void testConcurrentCreation_2() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    int AsyncInvocationArrSize = 4;
    final String regionNamePrefix = "PARTREG";
    final String replRegion = "TESTREG";
    CacheSerializableRunnable createRepl = new CacheSerializableRunnable(
        "Create Repl") {
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setScope(Scope.DISTRIBUTED_ACK);
        attr.setDataPolicy(DataPolicy.REPLICATE);
        cache.createRegion(replRegion, attr.create());
      }
    };
    createRepl.run2();
    vm0.invoke(createRepl);
    vm1.invoke(createRepl);
    vm2.invoke(createRepl);
    vm3.invoke(createRepl);

    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    CacheSerializableRunnable createPR = new CacheSerializableRunnable(
        "Create PR") {
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        Region partitionedregion = null;
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(2).create());
        // wait for put
        Region reg = cache.getRegion(replRegion);
        Region.Entry regEntry;
        while ((regEntry = reg.getEntry("start")) == null
            || regEntry.getValue() == null) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        for (int index = 0; index < MAX_REGIONS; ++index) {
          final String regionName = regionNamePrefix + String.valueOf(index);
          partitionedregion = cache.createRegion(regionName, attr.create());
          assertNotNull("Partitioned Region ref null", partitionedregion);
          assertNotNull("Cache does not contain PR " + regionName, cache
              .getRegion(regionName));
          assertTrue("Partitioned Region ref claims to be destroyed",
              !partitionedregion.isDestroyed());
        }
      }
    };

    // create accessor on the main thread
    CacheSerializableRunnable createAccessorPR = new CacheSerializableRunnable(
        "Create Accessor PR") {
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        Region partitionedregion = null;
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(2).setLocalMaxMemory(0).create());
        // wait for put
        Region reg = cache.getRegion(replRegion);
        Region.Entry regEntry;
        while ((regEntry = reg.getEntry("start")) == null
            || regEntry.getValue() == null) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        for (int index = 0; index < MAX_REGIONS; ++index) {
          final String regionName = regionNamePrefix + String.valueOf(index);
          partitionedregion = cache.createRegion(regionName, attr.create());
          assertNotNull("Partitioned Region ref null", partitionedregion);
          assertNotNull("Cache does not contain PR " + regionName, cache
              .getRegion(regionName));
          assertTrue("Partitioned Region ref claims to be destroyed",
              !partitionedregion.isDestroyed());
        }
      }
    };

    Thread th = new Thread(createAccessorPR);
    th.start();

    async[0] = vm0.invokeAsync(createPR);
    async[1] = vm1.invokeAsync(createPR);
    async[2] = vm2.invokeAsync(createPR);
    async[3] = vm3.invokeAsync(createPR);

    // do the put
    Region reg = getCache().getRegion(replRegion);
    reg.put("start", "true");

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    th.join(30 * 1000);

    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during " + count, async[count].getException());
      }
    }

    // //validating that regions are successfully created
    vm0.invoke(getCacheSerializableRunnableForPRValidate(regionNamePrefix));
    vm1.invoke(getCacheSerializableRunnableForPRValidate(regionNamePrefix));
    vm2.invoke(getCacheSerializableRunnableForPRValidate(regionNamePrefix));
    vm3.invoke(getCacheSerializableRunnableForPRValidate(regionNamePrefix));
  }

  /**
   * Test Partitioned Region names that contain spaces
   * @throws Exception
   */
  public void testPartitionedRegionNameWithSpaces() throws Exception 
  {
    final String rName = getUniqueName() + " with some spaces";
    
    CacheSerializableRunnable csr = new CacheSerializableRunnable("validateNoExceptionWhenUsingNameWithSpaces") {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region partitionedregion = null;
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(0).create());
        partitionedregion = cache.createRegion(rName, attr
            .create());
        assertNotNull("Partitioned Region ref null", partitionedregion);
        assertNotNull("Cache does not contain PR " + rName, cache
            .getRegion(rName));
        assertTrue("Partitioned Region ref claims to be destroyed",
            !partitionedregion.isDestroyed());
      }
    };
    Host.getHost(0).getVM(2).invoke(csr);
    Host.getHost(0).getVM(3).invoke(csr);
  }

  /**
   * Test whether partition region creation is preveented when
   * an instance is created that has the incorrect redundancy
   */
  public void testPartitionedRegionRedundancyConflict() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final String rName = getUniqueName();
    vm0.invoke(new CacheSerializableRunnable("validateNoException") {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region partitionedregion = null;
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(0).create());
        partitionedregion = cache.createRegion(rName, attr
            .create());
        assertNotNull("Partitioned Region ref null", partitionedregion);
        assertNotNull("Cache does not contain PR " + rName, cache
            .getRegion(rName));
        assertTrue("Partitioned Region ref claims to be destroyed",
            !partitionedregion.isDestroyed());
      }
    });

    vm1.invoke(new CacheSerializableRunnable("validatePRCreationException") {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region partitionedregion = null;
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(1).create());
        try {
          cache.getLogger().info("<ExpectedException action=add>" + 
              "IllegalStateException</ExpectedException>");
          partitionedregion = cache.createRegion(rName, attr
              .create());
          fail("Expected exception upon creation with invalid redundancy");
        }
        catch (IllegalStateException expected) {
        }
        finally {
          cache.getLogger().info("<ExpectedException action=remove>" + 
          "IllegalStateException</ExpectedException>");
        }
        assertNull("Partitioned Region ref null", partitionedregion);
        assertNull("Cache contains PR " + rName + "!!", cache.getRegion(rName));
      }
    });

    vm1.invoke(new CacheSerializableRunnable("validatePRCreationException") {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region partitionedregion = null;
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(2).create());
        try {
          cache.getLogger().info("<ExpectedException action=add>" + 
          "IllegalStateException</ExpectedException>");
          partitionedregion = cache.createRegion(rName, attr
              .create());
          fail("Expected exception upon creation with invalid redundancy");
        }
        catch (IllegalStateException expected) {
        }
        finally {
          cache.getLogger().info("<ExpectedException action=remove>" + 
              "IllegalStateException</ExpectedException>");
        }
        assertNull("Partitioned Region ref null", partitionedregion);
        assertNull("Cache contains PR " + rName + "!!", cache.getRegion(rName));
      }
    });

    vm1.invoke(new CacheSerializableRunnable("validatePRCreationException") {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region partitionedregion = null;
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(3).create());
        try {
          cache.getLogger().info("<ExpectedException action=add>" + 
              "IllegalStateException</ExpectedException>");
          partitionedregion = cache.createRegion(rName, attr
              .create());
          fail("Expected exception upon creation with invalid redundancy");
        }
        catch (IllegalStateException expected) {
        }
        finally {
          cache.getLogger().info("<ExpectedException action=remove>" + 
              "IllegalStateException</ExpectedException>");
        }
        assertNull("Partitioned Region ref null", partitionedregion);
        assertNull("Cache contains PR " + rName + "!!", cache.getRegion(rName));
      }
    });
  }

  /**
   * This test creates partition region with scope = DISTRIBUTED_ACK and tests
   * whether all the attributes of partiotion region are properlt initialized
   * 
   * @throws Exception
   */
  public void testPartitionRegionInitialization() throws Throwable
  {
    final String name = getUniqueName();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    getLogWriter().info("*****INITIALIZATION TEST STARTED*****");
    int AsyncInvocationArrSize = 8;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm0.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));
    async[1] = vm1.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));
    async[2] = vm2.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));
    async[3] = vm3.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < 4; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }

    for (int count = 0; count < 4; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during " + count, async[count].getException());
      }
    }
    
    async[4] = vm0.invokeAsync(getCacheSerializableRunnableForPRInitialize());
    async[5] = vm1.invokeAsync(getCacheSerializableRunnableForPRInitialize());
    async[6] = vm2.invokeAsync(getCacheSerializableRunnableForPRInitialize());
    async[7] = vm3.invokeAsync(getCacheSerializableRunnableForPRInitialize());
    
    /** main thread is waiting for the other threads to complete */
    for (int count = 4; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
  
    for (int count = 4; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during " + count, async[count].getException());
      }
    }
    getLogWriter().info("*****INITIALIZATION TEST ENDED*****");
  }

  /**
   * This tests registration of partition region is happened in allpartition
   * region
   * 
   * @throws Exception
   */
  public void testPartitionRegionRegistration() throws Throwable
  {
    final String name = getUniqueName();
    // Cache cache = getCache();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    getLogWriter().info("*****REGISTRATION TEST STARTED*****");
    int AsyncInvocationArrSize = 8;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm0.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));
    async[1] = vm1.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));
    async[2] = vm2.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));
    async[3] = vm3.invokeAsync(getCacheSerializableRunnableForPRCreate(name,
        MAX_REGIONS, 0, "NONE"));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < 4; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }

    for (int count = 0; count < 4; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during " + count, async[count].getException());
      }
    }
    
    async[4] = vm0
        .invokeAsync(getCacheSerializableRunnableForPRRegistration(name));
    async[5] = vm1
        .invokeAsync(getCacheSerializableRunnableForPRRegistration(name));
    async[6] = vm2
        .invokeAsync(getCacheSerializableRunnableForPRRegistration(name));
    async[7] = vm3
        .invokeAsync(getCacheSerializableRunnableForPRRegistration(name));

    /** main thread is waiting for the other threads to complete */
    for (int count = 4; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
  
    for (int count = 4; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during " + count, async[count].getException());
      }
    }
    getLogWriter().info("*****REGISTRATION TEST ENDED*****");
  }
  
  /**
   * This tests persistence conflicts btw members of partition region
   * 
   * @throws Exception
   */
  public void testPartitionRegionPersistenceConflicts() throws Throwable
  {
    addExpectedException("IllegalStateException");
    final String name = getUniqueName();
    // Cache cache = getCache();
    Host host = Host.getHost(0);
    VM dataStore0 = host.getVM(0);
    VM dataStore1 = host.getVM(1);
    VM accessor0 = host.getVM(2);
    VM accessor1 = host.getVM(3);
    getLogWriter().info("*****PERSISTENCE CONFLICTS TEST STARTED*****");
    accessor0.invoke(getCacheSerializableRunnableForPRPersistence(name, 0, false, false));
    accessor1.invoke(getCacheSerializableRunnableForPRPersistence(name, 0, true, true));
    dataStore0.invoke(getCacheSerializableRunnableForPRPersistence(name, 100, true, false));
    dataStore1.invoke(getCacheSerializableRunnableForPRPersistence(name, 100, false, true));

     getLogWriter().info("*****PERSISTENCE CONFLICTS TEST ENDED*****");
  }

  /**
   * This function tests root, allpartition region and their scope and
   * mirrortype attribute.
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPRInitialize()
  {
    SerializableRunnable initializePrRegion;
    initializePrRegion = new CacheSerializableRunnable("initialize") {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region root = cache
            .getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
        if (root == null)
          fail("PartionedRegionInitializationDUnitTest() - the "
              + PartitionedRegionHelper.PR_ROOT_REGION_NAME + " do not exists");
        RegionAttributes regionAttribs = root.getAttributes();
        Scope scope = regionAttribs.getScope();
        if (!scope.isDistributedAck())
          fail("PartionedRegionInitializationDUnitTest() - the "
              + PartitionedRegionHelper.PR_ROOT_REGION_NAME
              + " scope is not distributedAck");
        assertEquals("PartionedRegionInitializationTest() - the "
            + PartitionedRegionHelper.PR_ROOT_REGION_NAME
            + " does not have the proper data policy" + DataPolicy.REPLICATE, DataPolicy.REPLICATE, regionAttribs.getDataPolicy());
//        Region allPartitionedRegions = root
//            .getSubregion(PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME);
//        if (allPartitionedRegions == null)
//          fail("PartionedRegionInitializationTest() - the "
//              + PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME
//              + " do not exists");
//        regionAttribs = allPartitionedRegions.getAttributes();
//        scope = regionAttribs.getScope();
//        if (!scope.isDistributedAck())
//          fail("PartionedRegionInitializationTest() - the "
//              + PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME
//              + " scope is not global");
//        DataPolicy datapolicy = regionAttribs.getDataPolicy();
//        if (! DataPolicy.REPLICATE.equals(datapolicy)) 
//          fail("PartionedRegionInitializationTest() - the "
//              + PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME
//              + " data policy is not " + DataPolicy.REPLICATE);
      }
    };
    return (CacheSerializableRunnable)initializePrRegion;
    
  }

  /**
   * this function tests psConfig for the regions
   * 
   * @param rgionName
   * @return
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPRRegistration(
      final String rgionName)
  {
    SerializableRunnable registerPrRegion;
    registerPrRegion = new CacheSerializableRunnable("register") {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region root = PartitionedRegionHelper.getPRRoot(cache);
//        Region allPartitionedRegions = PartitionedRegionHelper
//            .getPRConfigRegion(root, cache);
        for (int i = 0; i < MAX_REGIONS; i++) {
          Region region = cache.getRegion("/" + rgionName + String.valueOf(i));
          String name = ((PartitionedRegion)region).getRegionIdentifier();
          PartitionRegionConfig prConfig = (PartitionRegionConfig)root
              .get(name);
          if (prConfig == null)
            fail("PartionedRegionRegistrationTest() - PartionedRegion - "
                + name + " configs do not exists in  region - "
                + root.getName());
        }
        getLogWriter().info(" PartitionedRegionCreationTest PartionedRegionRegistrationTest() Successfully Complete ..  ");
      }
    };
    return (CacheSerializableRunnable)registerPrRegion;
  }

//  /**
//   * This function tests bucket2node for the regions
//   * 
//   * @param regionName
//   * @return
//   */
//  public CacheSerializableRunnable getCacheSerializableRunnableForPRBucket2NodeCreation(
//      final String regionName)
//  {
//    SerializableRunnable bucket2NodePrRegion;
//    bucket2NodePrRegion = new CacheSerializableRunnable("register") {
//      public void run2() throws CacheException
//      {
//        Cache cache = getCache();
//        Region root = PartitionedRegionHelper.getPRRoot(cache);
//        for (int i = 0; i < MAX_REGIONS; i++) {
//          PartitionedRegion region = (PartitionedRegion) cache.getRegion("/" + regionName + i);
//          String bucketToNodeName = PartitionedRegionHelper.BUCKET_2_NODE_TABLE_PREFIX
//              + region.getPRId();
//          Region bucketRegion = root.getSubregion(bucketToNodeName);
//          if (bucketRegion == null)
//            fail("PartionedRegionBucketToNodeCreateTest() - BucketToNode Region do not exist for PartitionedRegion - "
//                + region);
//        }
//        getLogWriter().info(" PartitionedRegionCreationTest PartionedRegionBucketToNodeCreateTest() Successfully Complete ..  ");
//      }
//    };
//    return (CacheSerializableRunnable)bucket2NodePrRegion;
//  }

  /**
   * this function creates partion region with the given name and throws
   * appropriate exception
   * @param regionName
   * @param cnt
   * @param redundancy
   * @param exceptionType
   * @param objName
   * 
   * @return
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPRCreate(
      final String regionName, final int cnt, final int redundancy,
      final String exceptionType)
  {
    SerializableRunnable createPrRegion1;
    if (cnt == 0) {
      createPrRegion1 = new CacheSerializableRunnable(regionName) {
        @Override
        public void run2() throws CacheException
        {
          Cache cache = getCache();
          Region partitionedregion = null;
          try {
            AttributesFactory attr = new AttributesFactory();
            PartitionAttributesFactory paf = new PartitionAttributesFactory();
            if (redundancy != 0)
              paf.setRedundantCopies(redundancy);
            PartitionAttributes prAttr = paf.create();
            attr.setPartitionAttributes(prAttr);
            partitionedregion = cache.createRegion(regionName, attr
                .create());
          }
          catch (IllegalStateException ex) {
            getCache().getLogger().warning(
                "Creation caught IllegalStateException", ex);
            if (exceptionType.equals("GLOBAL"))
              getLogWriter().info("PartitionedRegionCreationDUnitTest:testPartitionedRegionCreationExceptions()  Got a Correct exception for scope = GLOBAL");
            if (exceptionType.equals("REDUNDANCY"))
              getLogWriter().info("PartitionedRegionCreationDUnitTest:testPartitionedRegionCreationExceptions()  Got a Correct exception for 0 > redundancy  > 3  ");
            if (exceptionType.equals("DIFFREG"))
              getLogWriter().info("PartitionedRegionCreationDUnitTest:testPartitionedRegionCreationExceptions()  Got a Correct exception for regions with diff scope ");
          }
          assertNotNull("Partitioned Region " + regionName + " not in cache",
              cache.getRegion(regionName));
          assertNotNull("Partitioned Region ref null", partitionedregion);
          assertTrue("Partitioned Region ref claims to be destroyed",
              !partitionedregion.isDestroyed());
        }
      };
    }
    else {
      createPrRegion1 = new CacheSerializableRunnable(regionName) {
        @Override
        public void run2() throws CacheException
        {
          Cache cache = getCache();
          AttributesFactory attr = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          if (redundancy != 0)
            paf.setRedundantCopies(redundancy);
          PartitionAttributes prAttr = paf.create();
          attr.setPartitionAttributes(prAttr);
          Region partitionedregion = null;
          String rName = null;
          for (int i = 0; i < cnt; i++) {
            try {
              rName = regionName + i;
              partitionedregion = cache.createRegion(rName, attr
                  .create());
            }
            catch (IllegalStateException ex) {
              getCache().getLogger().warning(
                  "Creation caught IllegalStateException", ex);
              if (exceptionType.equals("GLOBAL"))
                getLogWriter().info("PartitionedRegionCreationDUnitTest:testPartitionedRegionCreationExceptions()  Got a Correct exception for scope = GLOBAL");
              if (exceptionType.equals("REDUNDANCY"))
                getLogWriter().info("PartitionedRegionCreationDUnitTest:testPartitionedRegionCreationExceptions()  Got a Correct exception for 0 > redundancy  > 3  ");
              if (exceptionType.equals("DIFFREG"))
                getLogWriter().info("PartitionedRegionCreationDUnitTest:testPartitionedRegionCreationExceptions()  Got a Correct exception for regions with diff scope ");
            }
            assertNotNull("Partitioned Region " + rName + " not in cache",
                cache.getRegion(rName));
            assertNotNull("Partitioned Region ref null", partitionedregion);
            assertTrue("Partitioned Region ref claims to be destroyed",
                !partitionedregion.isDestroyed());
          }
        }
      };
    }
    return (CacheSerializableRunnable)createPrRegion1;
  }
  
  /**
   * this function creates partition region with the specified persistence and 
   * throws appropriate exception
   * @param regionName
   * @param cnt
   * @param redundancy
   * @param exceptionType
   * @param objName
   * 
   * @return
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPRPersistence(
      final String regionName, final int localMaxMemory, final boolean isPersistent, final boolean expectException)
  {
    SerializableRunnable createPrRegion1;
    createPrRegion1 = new CacheSerializableRunnable(regionName) {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region partitionedregion = null;
        try {
          AttributesFactory attr = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setLocalMaxMemory(localMaxMemory); // 0: accessor
          PartitionAttributes prAttr = paf.create();
          attr.setPartitionAttributes(prAttr);
          if (isPersistent) {
            attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          } else {
            attr.setDataPolicy(DataPolicy.PARTITION);
          }
          partitionedregion = cache.createRegion(regionName, attr.create());
          if (expectException) {
            fail("Expect exception but it did not");
          }
          assertNotNull("Partitioned Region " + regionName + " not in cache",
              cache.getRegion(regionName));
          assertNotNull("Partitioned Region ref null", partitionedregion);
          assertTrue("Partitioned Region ref claims to be destroyed",
              !partitionedregion.isDestroyed());
        }
        catch (IllegalStateException ex) {
          if (localMaxMemory>0) {
            // datastore
            assertTrue(ex.getMessage().contains("DataPolicy for Datastore members should all be persistent or not."));
          } else {
            assertTrue(ex.getMessage().contains("Persistence is not allowed when local-max-memory is zero."));
          }
        }
      }
    };
    return (CacheSerializableRunnable)createPrRegion1;
  }

  /**
   * this function validates partition regions
   * 
   * @param regionName
   * @return
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPRValidate(
      final String regionName)
  {
    SerializableRunnable validatePrRegion = new CacheSerializableRunnable(
        "validateRegionCreation") {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        String n;
        for (int i = 0; i < MAX_REGIONS; i++) {
          n = Region.SEPARATOR + regionName + String.valueOf(i);
          assertNotNull(n + " not created successfully", cache.getRegion(n));
        }
      }
    };
    return (CacheSerializableRunnable)validatePrRegion;
  }
  
  /**
   * This creates a PR with data store for a specified number of buckets
   */
  SerializableRunnable createPrRegion = new CacheSerializableRunnable(
      "createPrRegion") {

    @Override
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      AttributesFactory attr = new AttributesFactory();
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      PartitionAttributes prAttr = paf.create();
      attr.setPartitionAttributes(prAttr);
      RegionAttributes regionAttribs = attr.create();
      cache.createRegion("PR1", regionAttribs);

      paf.setTotalNumBuckets(totalNumBuckets);
      prAttr = paf.create();
      attr.setPartitionAttributes(prAttr);
      regionAttribs = attr.create();
      cache.createRegion("PR2", regionAttribs);

    }
  };

  /**
   * SerializableRunnable object to create PR with scope = D_ACK with only
   * Accessor(no data store)
   */

  SerializableRunnable createPrRegionOnlyAccessor = new CacheSerializableRunnable(
      "createPrRegionOnlyAccessor") {

    @Override
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      AttributesFactory attr = new AttributesFactory();
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      PartitionAttributes prAttr = paf.setLocalMaxMemory(0)
          .create();
      attr.setPartitionAttributes(prAttr);
      RegionAttributes regionAttribs = attr.create();
      PartitionedRegion accessor = (PartitionedRegion)cache.createRegion(
          "PR1", regionAttribs);
      getLogWriter().info("Region created in VM1.");
      assertEquals(accessor.getTotalNumberOfBuckets(),
          PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT);
      try {
        cache.getLogger().info("<ExpectedException action=add>" + 
            "IllegalStateException</ExpectedException>");
        accessor = (PartitionedRegion)cache.createRegion("PR2", regionAttribs);
        fail("Creation of a Partitioned Region was allowed with incompatible GLOBAL_MAX_BUCKETS setting");
      } catch (IllegalStateException expected) {
        
      } finally {
        cache.getLogger().info("<ExpectedException action=remove>" + 
            "IllegalStateException</ExpectedException>");
      }
      
      Properties globalProps = new Properties();
      globalProps.setProperty(
          PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY, ""
              + totalNumBuckets);
      paf.setGlobalProperties(globalProps);
      attr.setPartitionAttributes(paf.create());
      accessor = (PartitionedRegion)cache.createRegion("PR2", attr.create());
      assertEquals(accessor.getTotalNumberOfBuckets(), totalNumBuckets);

    }
  };

  /**
   * This method validates that TotalNumberOfBuckets are getting set properly.
   * 
   * @throws Exception
   */
  public void testTotalNumberOfBuckets() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegionOnlyAccessor);
  }  
}
