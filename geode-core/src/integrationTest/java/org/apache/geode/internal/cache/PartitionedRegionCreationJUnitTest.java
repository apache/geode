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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class PartitionedRegionCreationJUnitTest {
  private volatile int PRNumber = 0;

  private volatile boolean incrementFlag = false;

  private final int TOTAL_THREADS = 10;

  private volatile int TOTAL_PR_CREATED = 0;

  private volatile int TOTAL_RETURNS = 0;

  private volatile int TOTAL_PR_CREATION_FAIL = 0;

  private final Object PR_CREATE = new Object();

  private final Object PR_CREATE_FAIL = new Object();

  private final Object PR_INCREMENT = new Object();

  private final Object PR_TOTAL_RETURNS = new Object();

  private boolean PRCreateDone = false;

  private List PRRegionList = new ArrayList();

  private Object CREATE_COMPLETE_LOCK = new Object();

  private volatile boolean createComplete = false;

  private List<Thread> regionCreationThreads = new ArrayList<>(20);

  @Before
  public void setUp() throws Exception {
    TOTAL_RETURNS = 0;
  }

  @After
  public void tearDown() throws Exception {
    PartitionedRegionTestHelper.closeCache();
    long numThreads = regionCreationThreads.size();
    int numAlive = 0;
    for (Thread thread : regionCreationThreads) {
      thread.interrupt();
      thread.join(GeodeAwaitility.getTimeout().toMillis() / numThreads);
      if (thread.isAlive()) {
        numAlive++;
      }
    }
    if (numAlive > 0) {
      throw new IllegalStateException("Test is leaving behind " + numAlive + " live threads");
    }
  }

  /*
   * 1)Create 10 thread each. Each thread will try to create PartionedRegion total of 5 partitioned
   * region will be created. 5 threads should throw RegionExistException. 2) Tests for PR scope =
   * GLOBAL and PR scope = LOCAL </p> 3) Test for redundancy < 0 </p> 4) Test for redundancy > 3
   * </p> 5) Test for localMaxMemory < 0 </p>
   */
  @Test
  public void testPartitionedRegionCreate() {
    createMultiplePartitionedRegions();
    verifyCreateResults();

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
      fail("expected redundancy of 10 to cause an exception");
    } catch (IllegalStateException illex) {
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
    }

  }

  @Test
  public void testPersistentPartitionedRegionCreate() {
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
      cache.createRegion(regionname, ra);
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
        cache.createRegion(regionname, ra);
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
    }

    // test for redundancy < 0
    try {
      pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion(regionname,
          String.valueOf(200), -1);
      fail(
          "testpartionedRegionCreate() Expected IllegalStateException not thrown for redundancy < 0 ");
    } catch (IllegalStateException illex) {
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
  public void testPartionedRegionInitialization() throws RegionExistsException {
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

  }

  /**
   * Test for partitioned Region registration. All partitioned regions created must have a entry in
   * PARTITIONED_REGION_CONFIG_NAME. Every PR has PR name / PartitionRegionConfig entry in region
   * PARTITIONED_REGION_CONFIG_NAME
   *
   */
  @Test
  public void testPartionedRegionRegistration() {
    createMultiplePartitionedRegions();
    Region root = (PartitionedRegionTestHelper
        .getExistingRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME));

    Iterator itr = PRRegionList.iterator();
    while (itr.hasNext()) {
      Region region = (Region) itr.next();
      String name = ((PartitionedRegion) region).getRegionIdentifier();
      PartitionRegionConfig prConfig = (PartitionRegionConfig) root.get(name);
      if (prConfig == null)
        fail("testpartionedRegionRegistration() - PartionedRegion - " + name
            + " configs do not exists in  region - " + root.getName());
    }

  }

  /**
   * creates multiple partitioned region from different threads.
   *
   */
  private void createMultiplePartitionedRegions() {
    if (PRCreateDone)
      return;
    int numthread = 0;
    long giveupTime = System.currentTimeMillis() + GeodeAwaitility.getTimeout().toMillis();
    while (numthread < TOTAL_THREADS && giveupTime > System.currentTimeMillis()) {
      PartionedRegionCreateThread pregionThread = new PartionedRegionCreateThread();
      pregionThread.start();
      regionCreationThreads.add(pregionThread);
      numthread++;
    }
    assertTrue(numthread >= TOTAL_THREADS);
    GeodeAwaitility.await().until(() -> {
      synchronized (CREATE_COMPLETE_LOCK) {
        return createComplete;
      }
    });
    PRCreateDone = true;
  }

  /**
   * Verifies creation of partitioned region.
   *
   */
  private void verifyCreateResults() {
    GeodeAwaitility.await().untilAsserted(() -> {
      if (TOTAL_RETURNS != TOTAL_THREADS)
        fail("Failed -- Total thread returned is not same as number of threads created");

      if (TOTAL_PR_CREATED != (TOTAL_THREADS / 2))
        fail("Failed -- Total Partioned Region created is not correct.  Total created = "
            + TOTAL_PR_CREATED + " but expected " + (TOTAL_THREADS / 2));

      if (TOTAL_PR_CREATION_FAIL != (TOTAL_THREADS / 2))
        fail("Failed -- Total Partioned Region creation failures is not correct");
    });
  }

  /**
   * Thread to create the partitioned region.
   *
   */
  public class PartionedRegionCreateThread extends Thread {
    @Override
    public void run() {
      String prName = "PartitionedRegionCreationJUnitTest_" + getPRNumber();
      try {
        Region region = PartitionedRegionTestHelper.createPartionedRegion(prName);
        PRRegionList.add(region);
        updatePRCreate();
      } catch (RegionExistsException rex) {
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
      if (incrementFlag) {
        retNum = PRNumber;
        PRNumber++;
        incrementFlag = false;
      } else {
        incrementFlag = true;
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
    }
    if (TOTAL_RETURNS == TOTAL_THREADS) {
      synchronized (CREATE_COMPLETE_LOCK) {
        createComplete = true;
        CREATE_COMPLETE_LOCK.notifyAll();
      }
    }
  }
}
