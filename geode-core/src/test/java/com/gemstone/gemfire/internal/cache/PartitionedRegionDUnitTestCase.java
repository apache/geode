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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.internal.logging.PureLogWriter;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.standalone.DUnitLauncher;

/**
 * This class is extended by some PartitionedRegion related DUnit test cases 
 *
 */
public class PartitionedRegionDUnitTestCase extends CacheTestCase
{
  static int oldLogLevel;
  public void setVMInfoLogLevel() {
    SerializableRunnable runnable = new SerializableRunnable() {
      public void run() {
        oldLogLevel = setLogLevel(getCache().getLogger(), InternalLogWriter.INFO_LEVEL);
      }
    };
    for (int i=0; i<4; i++) {
      Host.getHost(0).getVM(i).invoke(runnable);
    }
  }
    
  public void resetVMLogLevel() {
    SerializableRunnable runnable = new SerializableRunnable() {
      public void run() {
        setLogLevel(getCache().getLogger(), oldLogLevel);
      }
    };
    for (int i=0; i<4; i++) {
      Host.getHost(0).getVM(i).invoke(runnable);
    }
  }
  /**
   * Sets the loglevel for the provided log writer
   * @param l  the {@link LogWriter}
   * @param logLevl the new log level as specified in {@link LogWriterImpl}
   * @return the old log level
   */
  public static int setLogLevel(LogWriter l, int logLevl)
  {
    int ret = -1;
    l.config("PartitionedRegionDUnitTest attempting to set log level on LogWriter instance class:" + l.getClass().getName());
    if (l instanceof PureLogWriter) {
      
        PureLogWriter pl = (PureLogWriter) l;
        ret = pl.getLogWriterLevel();
        l.config("PartitiionedRegionDUnitTest forcing log level to " + LogWriterImpl.levelToString(logLevl) 
            + " from "  +  LogWriterImpl.levelToString(ret));
        pl.setLevel(logLevl);
    }
    return ret;
  }

  public PartitionedRegionDUnitTestCase(String name) {
    super(name);
  }

  /**
   * Tear down a PartitionedRegionTestCase by cleaning up the existing cache (mainly
   * because we want to destroy any existing PartitionedRegions)
   */
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    preTearDownPartitionedRegionDUnitTest();
    closeCache();
    Invoke.invokeInEveryVM(CacheTestCase.class, "closeCache");
  }
  
  protected void preTearDownPartitionedRegionDUnitTest() throws Exception {
  }
  
  public static void caseSetUp() {
    DUnitLauncher.launchIfNeeded();
    // this makes sure we don't have any connection left over from previous tests
    disconnectAllFromDS();
  }
  public static void caseTearDown() {
    // this makes sure we don't leave anything for the next tests
    disconnectAllFromDS();
  }
  
  /**
   * This function creates multiple partition regions in a VM. The name of the
   * Partition Region will be PRPrefix+index (index starts from
   * startIndexForRegion and ends to endIndexForRegion)
   * 
   * @param PRPrefix :
   *          Used in the name of the Partition Region
   * 
   * These indices Represents range of the Partition Region
   */
  public CacheSerializableRunnable createMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForRegion,
      final int endIndexForRegion, final int redundancy, final int localmaxMemory) {
    return createMultiplePartitionRegion(PRPrefix, startIndexForRegion, endIndexForRegion, redundancy, localmaxMemory, false);
  }

  /**
   * This function creates multiple partition regions in a VM. The name of the
   * Partition Region will be PRPrefix+index (index starts from
   * startIndexForRegion and ends to endIndexForRegion)
   * 
   * @param PRPrefix :
   *          Used in the name of the Partition Region
   * 
   * These indices Represents range of the Partition Region
   * @param startIndexForRegion :
   * @param endIndexForRegion
   * @param redundancy
   * @param localmaxMemory
   * @param evict 
   * @return
   */
  public CacheSerializableRunnable createMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForRegion,
      final int endIndexForRegion, final int redundancy, final int localmaxMemory, final boolean evict)
  {
    return new CacheSerializableRunnable(
        "createPrRegions_" + PRPrefix) {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      int innerRedundancy = redundancy;

      int innerlocalmaxMemory = localmaxMemory;

      public void run2() throws CacheException
      {
        System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, 
            "20000");
        EvictionAttributes evictionAttrs = evict ? EvictionAttributes
            .createLRUEntryAttributes(Integer.MAX_VALUE,
                EvictionAction.LOCAL_DESTROY) : null;
        for (int i = startIndexForRegion; i < endIndexForRegion; i++) {
          Region partitionedregion = getCache().createRegion(innerPRPrefix + i,
              createRegionAttrsForPR(innerRedundancy,
        innerlocalmaxMemory, PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, evictionAttrs));
          getCache().getLogger().info("Successfully created PartitionedRegion = " + partitionedregion);
        }
        System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, 
            Integer.toString(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
        getCache().getLogger().info("createMultiplePartitionRegion() - Partition Regions Successfully Completed ");
      }
    };
  }
  
  protected RegionAttributes<?, ?> createRegionAttrsForPR(int red, int localMaxMem, long recoveryDelay, EvictionAttributes evictionAttrs) {
    return PartitionedRegionTestHelper.createRegionAttrsForPR(
        red, localMaxMem, recoveryDelay, evictionAttrs, null);
  }

  public CacheSerializableRunnable getCreateMultiplePRregion(
      final String prPrefix, final int maxIndex, final int redundancy, final int localmaxMemory, final long recoveryDelay)
  {
    return new CacheSerializableRunnable("getCreateMultiplePRregion") {
      public void run2() throws CacheException
      {
        // final Random ra = new Random();
        for (int i = 0; i < maxIndex; i++) {
          // final int rind = ra.nextInt(maxIndex);
          try {
            getCache().createRegion(
                prPrefix + i,
                PartitionedRegionTestHelper.createRegionAttrsForPR(redundancy,
                    localmaxMemory, recoveryDelay));
            com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Created Region  new  --- " + prPrefix + i);
          } catch (RegionExistsException ignore) {}
        }
        com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("getCreateMultiplePRregion() - Partition Regions Successfully Completed ");
      }
    };
  }

  /**
   * This function performs following checks on allPartitionRegion and
   * bucket2Node region of Partition Region 1. allPartitionRegion should not be
   * null 2. size of allPartitionRegion should be no. of regions + 1.
   * 3.Bucket2Node should not be null and size should = no. of regions 4. Name
   * of the Bucket2Node should be
   * PartitionedRegionHelper.BUCKET_2_NODE_TABLE_PREFIX + pr.getName().
   */
  public CacheSerializableRunnable validateMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForRegion,
      final int endIndexForRegion)
  {
    CacheSerializableRunnable validateAllPRs;
    validateAllPRs = new CacheSerializableRunnable("validateAllPRs") {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      public void run2() throws CacheException
      {
        Region rootRegion = getCache().getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
        assertNotNull(rootRegion);
        assertEquals("PR root size is not correct", innerEndIndexForRegion,
          rootRegion.size());
//        Region allPR = rootRegion.getSubregion(PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME);
//        assertNotNull(allPR);
//        assertEquals("allPR size is not correct", innerEndIndexForRegion,
//      allPR.size());
        assertEquals("prIdToPR size is not correct", innerEndIndexForRegion,
            PartitionedRegion.prIdToPR.size());
        getCache().getLogger().info("validateMultiplePartitionRegion() - Partition Regions Successfully Validated ");
      }
    };
    return validateAllPRs;
  }

  /**
   * This function performs put() operations in multiple Partition Regions
   */
  public CacheSerializableRunnable putInMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForKey,
      final int endIndexForKey, final int startIndexNumOfRegions,
      final int endIndexNumOfRegions)
  {
    CacheSerializableRunnable putInPRs = new CacheSerializableRunnable(
        "doPutOperations") {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForKey = startIndexForKey;

      int innerEndIndexForKey = endIndexForKey;

      int innerStartIndexNumOfRegions = startIndexNumOfRegions;

      int innerEndIndexNumOfRegions = endIndexNumOfRegions;

      public void run2() throws CacheException
      {
        for (int j = innerStartIndexNumOfRegions; j < innerEndIndexNumOfRegions; j++) {
          Region pr = getCache().getRegion(Region.SEPARATOR + innerPRPrefix + j);
          assertNotNull(pr);
          for (int k = innerStartIndexForKey; k < innerEndIndexForKey; k++) {
            pr.put(j + innerPRPrefix + k, innerPRPrefix + k);
          }
          getCache().getLogger().info("putInMultiplePartitionRegion() - Put() done Successfully in Partition Region "+ pr.getName());
        }
      }
    };
    return putInPRs;
  }

  /**
   * This function performs get() operations in multiple Partitions Regions and
   * checks return values
   */
  public CacheSerializableRunnable getInMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForKey,
      final int endIndexForKey, final int startIndexNumOfRegions,
      final int endIndexNumOfRegions)
  {
    CacheSerializableRunnable getInPRs = new CacheSerializableRunnable(
        "doGetOperations") {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForKey = startIndexForKey;

      int innerEndIndexForKey = endIndexForKey;

      int innerStartIndexNumOfRegions = startIndexNumOfRegions;

      int innerEndIndexNumOfRegions = endIndexNumOfRegions;

      public void run2() throws CacheException
      {
        for (int j = innerStartIndexNumOfRegions; j < innerEndIndexNumOfRegions; j++) {
          Region pr = getCache().getRegion(Region.SEPARATOR + innerPRPrefix + j);
          assertNotNull(pr);
          for (int k = innerStartIndexForKey; k < innerEndIndexForKey; k++) {
            Object Obj = pr.get(j + innerPRPrefix + k);
            assertNotNull(Obj);
            assertEquals("Values are not equal", Obj,
                (innerPRPrefix + k));
          }
          getCache().getLogger().info("putInMultiplePartitionRegion() - Get() done Successfully in Partition Region "+ pr.getName());
        }
      }
    };
    return getInPRs;
  }

  /**
   * This function performs put() operations in multiple Partition Regions after
   * the region is destroyed.
   */
  public CacheSerializableRunnable putAfterDestroyInMultiplePartitionedRegion(
      final String PRPrefix, final int startIndexForKey,
      final int endIndexForKey, final int startIndexNumOfRegions,
      final int endIndexNumOfRegions)
  {
    CacheSerializableRunnable putInPRs = new CacheSerializableRunnable(
        "doPutAfterDestroyOperations") {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForKey = startIndexForKey;

      int innerEndIndexForKey = endIndexForKey;

      int innerStartIndexNumOfRegions = startIndexNumOfRegions;

      int innerEndIndexNumOfRegions = endIndexNumOfRegions;

      public void run2() throws CacheException
      {
        for (int j = innerStartIndexNumOfRegions; j < innerEndIndexNumOfRegions; j++) {
          Region pr = getCache().getRegion(Region.SEPARATOR + innerPRPrefix + j);
          for (int k = innerStartIndexForKey; k < innerEndIndexForKey; k++) {
            try {
              pr.put(j + innerPRPrefix + k, innerPRPrefix + k);
              fail("You can not put after the region is destroyed ");
            }
            catch (RegionDestroyedException e) {
              // do nothing It's a valid exception
            }
          }
        }
      }
    };
    return putInPRs;
  }

  /**
   * this function performs get() operations for the removed and destroyed
   * entries
   */
  public CacheSerializableRunnable getRemovedOrDestroyedInMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForKey,
      final int endIndexForKey, final int startIndexNumOfRegions,
      final int endIndexNumOfRegions, final int afterPutFlag)
  {
    CacheSerializableRunnable getInPRs = new CacheSerializableRunnable(
        "doDetroyedGetOperations") {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForKey = startIndexForKey;

      int innerEndIndexForKey = endIndexForKey;

      int innerStartIndexNumOfRegions = startIndexNumOfRegions;

      int innerEndIndexNumOfRegions = endIndexNumOfRegions;

      int innerAfterPutFlag = afterPutFlag;

      public void run2() throws CacheException
      {
        for (int j = innerStartIndexNumOfRegions; j < innerEndIndexNumOfRegions; j++) {
          Region pr = getCache().getRegion(Region.SEPARATOR + innerPRPrefix + j);
          assertNotNull(pr);
          for (int k = innerStartIndexForKey; k < innerEndIndexForKey; k++) {
            Object Obj = pr.get(j + innerPRPrefix + k);
            if (innerAfterPutFlag == 0)
              assertNull(Obj);
            else
              assertNotNull(Obj);
          }
          getCache().getLogger().info("getRmovedOrDestroyedInMultiplePartitionRegion() - Get() of Destroy Keys done Successfully in Partition Region " + pr.getName());
        }
      }
    };
    return getInPRs;
  }

  /** this functions destroys regions in a node */
  public CacheSerializableRunnable destroyRegionInMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForRegion,
      final int endIndexForRegion)
  {

    CacheSerializableRunnable getInPRs = new CacheSerializableRunnable(
        "destroyRegionInMultiplePartitionRegion") {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      public void run2() throws CacheException
      {
        for (int j = innerStartIndexForRegion; j < innerEndIndexForRegion; j++) {
          Region pr = getCache().getRegion(Region.SEPARATOR + innerPRPrefix + j);
          assertNotNull(pr);
          getCache().getLogger().info("region going to destroy is : " + pr);
          pr.destroyRegion();
        }
      }
    };
    return getInPRs;
  }

  /**
   * This function performs destroy() operations in multiple partitions for
   * destroying key-value pair so that entry corresponding to that key becomes
   * null
   */
  public CacheSerializableRunnable destroyInMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForKey,
      final int endIndexForKey, final int startIndexNumOfRegions,
      final int endIndexNumOfRegions)
  {
    CacheSerializableRunnable destroyInPRs = new CacheSerializableRunnable(
        "doDestroyKeyOperations") {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForKey = startIndexForKey;

      int innerEndIndexForKey = endIndexForKey;

      int innerStartIndexNumOfRegions = startIndexNumOfRegions;

      int innerEndIndexNumOfRegions = endIndexNumOfRegions;

      public void run2() throws CacheException
      {
        for (int j = innerStartIndexNumOfRegions; j < innerEndIndexNumOfRegions; j++) {
          Region pr = getCache().getRegion(Region.SEPARATOR + innerPRPrefix + j);
          assertNotNull(pr);
          for (int k = startIndexForKey; k < endIndexForKey; k++) {
            try {
              pr.destroy(j + innerPRPrefix + k);
            }
            catch (Exception e) {
              fail("destroyInMultiplePartitionRegion()- Entry not found in the Partition region "
                  + pr.getName());
            }
          }
          getCache().getLogger().info("destroyInMultiplePartitionRegion() - destroy() done Successfully in Partition Region "+ pr.getName());
        }
      }
    };
    return destroyInPRs;
  }

  /**
   * This function performs invalidate() operation in multiple partition
   * regions.
   */
  public CacheSerializableRunnable invalidatesInMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForKey,
      final int endIndexForKey, final int startIndexNumOfRegions,
      final int endIndexNumOfRegions)
  {
    CacheSerializableRunnable invalidateInPRs = new CacheSerializableRunnable(
        "doInvalidateKeyOperations") {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForKey = startIndexForKey;

      int innerEndIndexForKey = endIndexForKey;

      int innerStartIndexNumOfRegions = startIndexNumOfRegions;

      int innerEndIndexNumOfRegions = endIndexNumOfRegions;

      public void run2() throws CacheException
      {
        for (int j = innerStartIndexNumOfRegions; j < innerEndIndexNumOfRegions; j++) {
          Region pr = getCache().getRegion(Region.SEPARATOR + innerPRPrefix + j);
          assertNotNull(pr);
          for (int k = startIndexForKey; k < endIndexForKey; k++) {
            try {
              pr.invalidate(j + innerPRPrefix + k);
            }
            catch (Exception e) {
              fail("invalidateInMultiplePartitionRegion()- Entry not found in the Partition region "
                  + pr.getName());
            }
          }
          getCache().getLogger().info("invalidateInMultiplePartitionRegion() - invalidate() done Successfully in Partition Region "+ pr.getName());
        }
      }
    };
    return invalidateInPRs;
  }

  public CacheSerializableRunnable disconnectVM()
  {
    CacheSerializableRunnable csr = new CacheSerializableRunnable(
        "disconnectVM") {
      public void run2() throws CacheException
      {
        getCache();
//        DistributedMember dsMember = ((InternalDistributedSystem)getCache()
//            .getDistributedSystem()).getDistributionManager().getId();
        getCache().getDistributedSystem().disconnect();
        getCache().getLogger().info("disconnectVM() completed ..");
      }
    };
    return csr;
  }
}