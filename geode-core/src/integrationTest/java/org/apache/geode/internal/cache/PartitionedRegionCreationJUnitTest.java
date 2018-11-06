/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;

/**
 * This is a test for creation of Partition region(PR).
 * <p>
 * Following tests are included in PartitionedRegionCreationJUnitTest :
 * </p>
 * <p>
 * 1) testpartionedRegionCreate - Tests the PR creation.
 * </p>
 * <p>
 * 2) testpartionedRegionInitialization - Tests the PR initialization
 * </p>
 * <p>
 * 3) testpartionedRegionRegistration - Tests the PR registration
 * </p>
 * <p>
 * 4) testpartionedRegionBucketToNodeCreate - Tests the PR's BUCKET_2_NODE region creation
 * </p>
 *
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionedRegionCreationJUnitTest {
  static volatile int PRNumber = 0;

  static Region root = null;

  static volatile boolean increamentFlag = false;

  static final int TOTAL_THREADS = 10;

  static volatile int TOTAL_PR_CREATED = 0;

  static volatile int TOTAL_RETURNS = 0;

  static volatile int TOTAL_PR_CREATION_FAIL = 0;

  static final Object PR_CREATE = new Object();

  static final Object PR_CREATE_FAIL = new Object();

  static final Object PR_INCREMENT = new Object();

  static final Object PR_TOTAL_RETURNS = new Object();

  public boolean PRCreateDone = false;

  List PRRegionList = new ArrayList();

  LogWriter logger = null;

  private Object CREATE_COMPLETE_LOCK = new Object();

  private volatile boolean createComplete = false;

  @Before
  public void setUp() throws Exception {
    TOTAL_RETURNS = 0;
    if (logger == null)
      logger = PartitionedRegionTestHelper.getLogger();
  }

  /*
   * 1)Create 10 thread each. Each thread will try to create PartionedRegion total of 5 partitioned
   * region will be created. 5 threads should throw RegionExistException. 2) Tests for PR scope =
   * GLOBAL and PR scope = LOCAL </p> 3) Test for redundancy < 0 </p> 4) Test for redundancy > 3
   * </p> 5) Test for localMaxMemory < 0 </p>
   */
  @Test
  public void test000PartitionedRegionCreate() {
    createMultiplePartitionedRegions();
    verifyCreateResults();
    if (logger.fineEnabled()) {
      logger.fine(
          " PartitionedRegionCreationTest-testpartionedRegionCreate() Successfully Complete ..  ");
    }

    final String regionname = "testPartionedRegionCreate";
    int localMaxMemory = 0;
    PartitionedRegion pr = null;

    // Test vanilla creation of a Partitioned Region w/o Scope
    try {
      AttributesFactory af = new AttributesFactory();
      af.setDataPolicy(DataPolicy.PARTITION);
      RegionAttributes ra = af.create();
      Cache cache = PartitionedRegionTestHelper.createCache();
      pr = (PartitionedRegion) cache.createRegion(regionname, ra);
    } finally {
      pr.destroyRegion();
    }

    // Assert that setting any scope throws IllegalStateException
    final Scope[] scopes =
        {Scope.LOCAL, Scope.DISTRIBUTED_ACK, Scope.DISTRIBUTED_NO_ACK, Scope.GLOBAL};
    for (int i = 0; i < scopes.length; i++) {
      try {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        af.setScope(scopes[i]);
        RegionAttributes ra = af.create();
        Cache cache = PartitionedRegionTestHelper.createCache();
        pr = (PartitionedRegion) cache.createRegion(regionname, ra);
        fail("testpartionedRegionCreate() Expected IllegalStateException not thrown for Scope "
            + scopes[i]);
      } catch (IllegalStateException expected) {
      } finally {
        if (pr != null && !pr.isDestroyed()) {
          pr.destroyRegion();
        }
      }
    }

    // test for redundancy > 3
    int redundancy = 10;
    try {
      pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion(regionname,
          String.valueOf(localMaxMemory), redundancy);
    } catch (IllegalStateException illex) {
      if (logger.fineEnabled()) {
        logger.fine(
            "testpartionedRegionCreate() Got a correct exception-IllegalStateException for  redundancy > 3 ");
      }
    }

    // test for redundancy < 0
    if (pr != null && !pr.isDestroyed())
      pr.destroyRegion();
    redundancy = -5;
    try {
      pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion(regionname,
          String.valueOf(200), redundancy);
      fail(
          "testpartionedRegionCreate() Expected IllegalStateException not thrown for redundancy < 0 ");
    } catch (IllegalStateException illex) {
      if (logger.fineEnabled()) {
        logger.fine(
            "testpartionedRegionCreate() Got a correct exception-IllegalStateException for  redundancy < 0 ");
      }
    }

    // test for localMaxMemory < 0
    /*
     * if (pr!= null && !pr.isDestroyed()) pr.destroyRegion(); ; localMaxMemory = -5; try { pr =
     * (PartitionedRegion)PartitionedRegionTestHelper .createPartitionedRegion(regionname,
     * String.valueOf(localMaxMemory), 2, Scope.DISTRIBUTED_ACK);
     * fail("testpartionedRegionCreate() Expected IllegalStateException not thrown for localMaxMemory < 0 "
     * ); } catch (IllegalStateException illex) { if (logger.fineEnabled()) { logger
     * .fine("testpartionedRegionCreate() Got a correct exception-IllegalStateException for  localMaxMemory < 0  "
     * ); } }
     */
  }

  @Test
  public void test001PersistentPartitionedRegionCreate() {
    final String regionname = "testPersistentPartionedRegionCreate";
    PartitionedRegion pr = null;

    // Test vanilla creation of a Partitioned Region w/o Scope
    try {
      AttributesFactory af = new AttributesFactory();
      af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      RegionAttributes ra = af.create();
      Cache cache = PartitionedRegionTestHelper.createCache();
      pr = (PartitionedRegion) cache.createRegion(regionname, ra);
    } finally {
      if (pr != null) {
        pr.destroyRegion();
      }
    }

    // Assert that an accessor (localMaxMem == 0) can't be persistent
    try {
      AttributesFactory af = new AttributesFactory();
      af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      af.setPartitionAttributes(new PartitionAttributesFactory().setLocalMaxMemory(0).create());

      RegionAttributes ra = af.create();
      Cache cache = PartitionedRegionTestHelper.createCache();
      pr = (PartitionedRegion) cache.createRegion(regionname, ra);
      fail("testpartionedRegionCreate() Expected IllegalStateException not thrown");
    } catch (IllegalStateException expected) {
      assertEquals("Persistence is not allowed when local-max-memory is zero.",
          expected.getMessage());
    }

    // Assert that a region can't be created
    // if configured with a diskStoreName and the disk store has not be created.
    try {
      AttributesFactory af = new AttributesFactory();
      af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      af.setDiskStoreName("nonexistentDiskStore");
      RegionAttributes ra = af.create();
      Cache cache = PartitionedRegionTestHelper.createCache();
      pr = (PartitionedRegion) cache.createRegion(regionname, ra);
      fail("testpartionedRegionCreate() Expected IllegalStateException not thrown");
    } catch (RuntimeException expected) {
      assertTrue(expected.getMessage().contains(String.format("Disk store %s not found",
          "nonexistentDiskStore")));
    }

    // Assert that you can't have a diskStoreName unless you are persistent or overflow.
    try {
      Cache cache = PartitionedRegionTestHelper.createCache();
      cache.createDiskStoreFactory().create("existentDiskStore");
      AttributesFactory af = new AttributesFactory();
      af.setDataPolicy(DataPolicy.PARTITION);
      af.setDiskStoreName("existentDiskStore");
      RegionAttributes ra = af.create();
      pr = (PartitionedRegion) cache.createRegion(regionname, ra);
      fail("testpartionedRegionCreate() Expected IllegalStateException not thrown");
    } catch (IllegalStateException expected) {
      assertEquals("Only regions with persistence or overflow to disk can specify DiskStore",
          expected.getMessage());
    }

    // Assert that setting any scope throws IllegalStateException
    final Scope[] scopes =
        {Scope.LOCAL, Scope.DISTRIBUTED_ACK, Scope.DISTRIBUTED_NO_ACK, Scope.GLOBAL};
    for (int i = 0; i < scopes.length; i++) {
      try {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setScope(scopes[i]);
        RegionAttributes ra = af.create();
        Cache cache = PartitionedRegionTestHelper.createCache();
        pr = (PartitionedRegion) cache.createRegion(regionname, ra);
        fail("testpartionedRegionCreate() Expected IllegalStateException not thrown for Scope "
            + scopes[i]);
      } catch (IllegalStateException expected) {
      }
    }

    // test for redundancy > 3
    try {
      pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion(regionname,
          String.valueOf(0), 4);
      fail(
          "testpartionedRegionCreate() Expected IllegalStateException not thrown for redundancy > 3 ");
    } catch (IllegalStateException illex) {
      if (logger.fineEnabled()) {
        logger.fine(
            "testpartionedRegionCreate() Got a correct exception-IllegalStateException for  redundancy > 3 ");
      }
    }

    // test for redundancy < 0
    try {
      pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion(regionname,
          String.valueOf(200), -1);
      fail(
          "testpartionedRegionCreate() Expected IllegalStateException not thrown for redundancy < 0 ");
    } catch (IllegalStateException illex) {
      if (logger.fineEnabled()) {
        logger.fine(
            "testpartionedRegionCreate() Got a correct exception-IllegalStateException for  redundancy < 0 ");
      }
    }
  }

  /**
   * Test for initialization of PartitionedRegion. Following are tested for the PartitionedRegion:
   * <p>
   * (1) Test for Root == null
   * <p>
   * (2) Test for Root region scope is DIST_ACK
   * <p>
   * (3) Test if MirrorType.NONE for root region.
   * <p>
   * (4) Test if PARTITIONED_REGION_CONFIG_NAME exist and isDistributedAck and the mirror type is
   * MirrorType.KEYS_VALUES.
   *
   */
  @Test
  public void test002PartionedRegionInitialization() throws RegionExistsException {
    String PRName = "testpartionedRegionInitialization";
    PartitionedRegionTestHelper.createPartionedRegion(PRName);

    Region root = (PartitionedRegionTestHelper
        .getExistingRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME));
    if (root == null)
      fail("testpartionedRegionInitialization() - the "
          + PartitionedRegionHelper.PR_ROOT_REGION_NAME + " do not exists");
    RegionAttributes regionAttribs = root.getAttributes();
    Scope scope = regionAttribs.getScope();
    if (!scope.isDistributedAck())
      fail("testpartionedRegionInitialization() - the "
          + PartitionedRegionHelper.PR_ROOT_REGION_NAME + " scope is not distributed_ack");
    assertEquals(DataPolicy.REPLICATE, regionAttribs.getDataPolicy());

    // Region allPartitionedRegions = root
    // .getSubregion(PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME);
    // if (allPartitionedRegions == null)
    // fail("testpartionedRegionInitialization() - the "
    // + PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME
    // + " do not exists");
    //
    // regionAttribs = allPartitionedRegions.getAttributes();
    // scope = regionAttribs.getScope();
    // if (!scope.isDistributedAck())
    // fail("testpartionedRegionInitialization() - the "
    // + PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME
    // + " scope is not distributed_ack");
    // MirrorType mirrortype = regionAttribs.getMirrorType();
    // if (mirrortype != MirrorType.KEYS_VALUES)
    // fail("testpartionedRegionInitialization() - the "
    // + PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME
    // + " mirror type is not KEYS_VALUES ");

    if (logger.fineEnabled()) {
      logger.fine("testpartionedRegionInitialization() Successfully Complete ..  ");
    }
    // System.out.println("testpartionedRegionInitialization");
  }

  /**
   * Test for partitioned Region registration. All partitioned regions created must have a entry in
   * PARTITIONED_REGION_CONFIG_NAME. Every PR has PR name / PartitionRegionConfig entry in region
   * PARTITIONED_REGION_CONFIG_NAME
   *
   */
  @Test
  public void test003partionedRegionRegistration() {
    createMultiplePartitionedRegions();
    Region root = (PartitionedRegionTestHelper
        .getExistingRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME));
    //
    // Region allPartitionedRegions = root
    // .getSubregion(PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME);

    Iterator itr = PRRegionList.iterator();
    while (itr.hasNext()) {
      Region region = (Region) itr.next();
      String name = ((PartitionedRegion) region).getRegionIdentifier();
      PartitionRegionConfig prConfig = (PartitionRegionConfig) root.get(name);
      if (prConfig == null)
        fail("testpartionedRegionRegistration() - PartionedRegion - " + name
            + " configs do not exists in  region - " + root.getName());
    }

    if (logger.fineEnabled()) {
      logger.fine(" testpartionedRegionRegistration() Successfully Complete ..  ");
    }
    // System.out.println("testpartionedRegionRegistration");
  }

  /**
   * creates multiple partitioned region from different threads.
   *
   */
  private void createMultiplePartitionedRegions() {
    if (PRCreateDone)
      return;
    int numthread = 0;
    while (numthread < TOTAL_THREADS) {
      PartionedRegionCreateThread pregionThread = new PartionedRegionCreateThread();
      pregionThread.start();
      numthread++;
    }
    while (!createComplete) {
      synchronized (CREATE_COMPLETE_LOCK) {
        if (!createComplete) {
          try {
            CREATE_COMPLETE_LOCK.wait();
          } catch (Exception ex) { // no action }
          }
        }
      }
    }
    PRCreateDone = true;
  }

  /**
   * Verifies creation of partitioned region.
   *
   */
  private void verifyCreateResults() {
    if (TOTAL_RETURNS != TOTAL_THREADS)
      fail("Failed -- Total thread returned is not same as number of threads created");

    if (TOTAL_PR_CREATED != (TOTAL_THREADS / 2))
      fail("Failed -- Total Partioned Region created is not correct");

    if (TOTAL_PR_CREATION_FAIL != (TOTAL_THREADS / 2))
      fail("Failed -- Total Partioned Region creation failures is not correct");
  }

  /**
   * Thread to create the partitioned region.
   *
   */
  public class PartionedRegionCreateThread extends Thread {
    public void run() {
      String prName = "PartitionedRegionCreationJUnitTest_" + getPRNumber();
      try {
        Region region = PartitionedRegionTestHelper.createPartionedRegion(prName);
        PRRegionList.add(region);
        if (logger.fineEnabled()) {
          logger.fine(
              "PartitionedRegionCreationJUnitTest - partitioned region -" + prName + "Created");
        }
        updatePRCreate();
      } catch (RegionExistsException rex) {
        if (logger.fineEnabled()) {
          logger
              .fine("PartitionedRegionCreationTest -  Thread - " + Thread.currentThread().getName()
                  + " Failed to create a PartitionedRegion. Region already exists");
        }
        updatePRCreateFail();
      }

      updateTotalReturns();
    }
  }

  /**
   * Increments and returns the PR nmber used for PR creation. Thread safe function.
   *
   * @return the PR number
   */
  protected int getPRNumber() {
    int retNum = 0;
    synchronized (PR_INCREMENT) {
      if (increamentFlag) {
        retNum = PRNumber;
        PRNumber++;
        increamentFlag = false;
      } else {
        increamentFlag = true;
      }
    }
    return retNum;
  }

  protected void updatePRCreate() {
    synchronized (PR_CREATE) {
      TOTAL_PR_CREATED++;
    }
  }

  protected void updatePRCreateFail() {
    synchronized (PR_CREATE_FAIL) {
      TOTAL_PR_CREATION_FAIL++;
    }
  }

  /**
   * Increments total creation thread returns.
   *
   */
  protected void updateTotalReturns() {
    synchronized (PR_TOTAL_RETURNS) {
      TOTAL_RETURNS++;
      System.out.println("TOTAL_RETURNS is " + TOTAL_RETURNS);
    }
    if (TOTAL_RETURNS == TOTAL_THREADS) {
      synchronized (CREATE_COMPLETE_LOCK) {
        createComplete = true;
        CREATE_COMPLETE_LOCK.notifyAll();
      }
    }
  }
}
