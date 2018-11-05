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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.ColocationLogger;
import org.apache.geode.internal.cache.InitialImageOperation.RequestImageMessage;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.RegionsTest;

@Category({RegionsTest.class})
public class PersistentColocatedPartitionedRegionDUnitTest
    extends PersistentPartitionedRegionTestBase {

  private static final String PATTERN_FOR_MISSING_CHILD_LOG =
      "(?s)Persistent data recovery for region .*is prevented by offline colocated region.*";
  private static final int NUM_BUCKETS = 15;
  private static final int MAX_WAIT = 60 * 1000;
  private static final int DEFAULT_NUM_EXPECTED_LOG_MESSAGES = 1;
  private static int numExpectedLogMessages = DEFAULT_NUM_EXPECTED_LOG_MESSAGES;
  private static int numChildPRs = 1;
  private static int numChildPRGenerations = 2;
  // Default region creation delay long enough for the initial cycle of logger warnings
  private static int delayForChildCreation = MAX_WAIT * 2 / 3;

  public PersistentColocatedPartitionedRegionDUnitTest() {
    super();
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    FileUtils.deleteDirectory(getBackupDir());
  }

  @Test
  public void testColocatedPRAttributes() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(1);

    vm0.invoke(new SerializableRunnable("create") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }

        // Create Persistent region
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion("persistentLeader", af.create());

        af.setDataPolicy(DataPolicy.PARTITION);
        af.setDiskStoreName(null);
        cache.createRegion("nonPersistentLeader", af.create());


        // Create a non persistent PR
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        paf.setColocatedWith("nonPersistentLeader");
        af.setPartitionAttributes(paf.create());

        // Try to colocate a persistent PR with the non persistent PR. This should fail.
        IgnoredException exp = IgnoredException.addIgnoredException("IllegalStateException");
        try {
          cache.createRegion("colocated", af.create());
          fail(
              "should not have been able to create a persistent region colocated with a non persistent region");
        } catch (IllegalStateException expected) {
          // do nothing
        } finally {
          exp.remove();
        }

        // Try to colocate a persistent PR with another persistent PR. This should work.
        paf.setColocatedWith("persistentLeader");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("colocated", af.create());

        // We should also be able to colocate a non persistent region with a persistent region.
        af.setDataPolicy(DataPolicy.PARTITION);
        af.setDiskStoreName(null);
        paf.setColocatedWith("persistentLeader");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("colocated2", af.create());
      }
    });
  }

  /**
   * Testing that we can colocate persistent PRs
   */
  @Test
  public void testColocatedPRs() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPRs = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());

        paf.setColocatedWith(getPartitionedRegionName());
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
        paf.setColocatedWith("region2");
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PARTITION);
        af.setDiskStoreName(null);
        cache.createRegion("region3", af.create());
      }
    };
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    vm2.invoke(createPRs);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");
    createData(vm0, 0, NUM_BUCKETS, "c", "region3");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    assertEquals(vm0Buckets, getBucketList(vm0, "region3"));
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    assertEquals(vm1Buckets, getBucketList(vm1, "region3"));
    Set<Integer> vm2Buckets = getBucketList(vm2, getPartitionedRegionName());
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));
    assertEquals(vm2Buckets, getBucketList(vm2, "region3"));

    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);

    AsyncInvocation async0 = vm0.invokeAsync(createPRs);
    AsyncInvocation async1 = vm1.invokeAsync(createPRs);
    AsyncInvocation async2 = vm2.invokeAsync(createPRs);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);


    // The secondary buckets can be recovered asynchronously,
    // so wait for them to come back.
    waitForBuckets(vm0, vm0Buckets, getPartitionedRegionName());
    waitForBuckets(vm0, vm0Buckets, "region2");
    waitForBuckets(vm1, vm1Buckets, getPartitionedRegionName());
    waitForBuckets(vm1, vm1Buckets, "region2");

    checkData(vm0, 0, NUM_BUCKETS, "a");
    checkData(vm0, 0, NUM_BUCKETS, "b", "region2");

    // region 3 didn't have persistent data, so it nothing should be recovered
    checkData(vm0, 0, NUM_BUCKETS, null, "region3");

    // Make sure can do a put in all of the buckets in region 3
    createData(vm0, 0, NUM_BUCKETS, "c", "region3");
    // Now all of those buckets should exist.
    checkData(vm0, 0, NUM_BUCKETS, "c", "region3");
    // The region 3 buckets should be restored in the appropriate places.
    assertEquals(vm0Buckets, getBucketList(vm0, "region3"));
    assertEquals(vm1Buckets, getBucketList(vm1, "region3"));
    assertEquals(vm2Buckets, getBucketList(vm2, "region3"));

  }

  private void createPR(String regionName, boolean persistent) {
    createPR(regionName, null, persistent, "disk");
  }

  private void createPR(String regionName, String colocatedWith, boolean persistent) {
    createPR(regionName, colocatedWith, persistent, "disk");
  }

  private void createPR(String regionName, String colocatedRegionName, boolean persistent,
      String diskName) {
    Cache cache = getCache();

    DiskStore ds = cache.findDiskStore(diskName);
    if (ds == null) {
      ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create(diskName);
    }
    AttributesFactory af = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(0);
    if (colocatedRegionName != null) {
      paf.setColocatedWith(colocatedRegionName);
    }
    af.setPartitionAttributes(paf.create());
    if (persistent) {
      af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      af.setDiskStoreName(diskName);
    } else {
      af.setDataPolicy(DataPolicy.PARTITION);
      af.setDiskStoreName(null);
    }
    cache.createRegion(regionName, af.create());
  }

  private SerializableRunnable createPRsColocatedPairThread =
      new SerializableRunnable("create2PRs") {
        public void run() {
          createPR(getPartitionedRegionName(), true);
          createPR("region2", getPartitionedRegionName(), true);
        }
      };

  private SerializableRunnable createMultipleColocatedChildPRs =
      new SerializableRunnable("create multiple child PRs") {
        @Override
        public void run() throws Exception {
          createPR(getPartitionedRegionName(), true);
          for (int i = 2; i < numChildPRs + 2; ++i) {
            createPR("region" + i, getPartitionedRegionName(), true);
          }
        }
      };

  private SerializableRunnable createPRColocationHierarchy =
      new SerializableRunnable("create PR colocation hierarchy") {
        @Override
        public void run() throws Exception {
          createPR(getPartitionedRegionName(), true);
          createPR("region2", getPartitionedRegionName(), true);
          for (int i = 3; i < numChildPRGenerations + 2; ++i) {
            createPR("region" + i, "region" + (i - 1), true);
          }
        }
      };

  private SerializableCallable createPRsMissingParentRegionThread =
      new SerializableCallable("createPRsMissingParentRegion") {
        public Object call() throws Exception {
          String exClass = "";
          Exception ex = null;
          try {
            // Skip creation of first region - expect region2 creation to fail
            // createPR(PR_REGION_NAME, true);
            createPR("region2", getPartitionedRegionName(), true);
          } catch (Exception e) {
            ex = e;
            exClass = e.getClass().toString();
          } finally {
            return ex;
          }
        }
      };

  private SerializableCallable delayedCreatePRsMissingParentRegionThread =
      new SerializableCallable("delayedCreatePRsMissingParentRegion") {
        public Object call() throws Exception {
          String exClass = "";
          Exception ex = null;
          // To ensure that the targeted code paths in ColocationHelper.getColocatedRegion is taken,
          // this
          // thread delays the attempted creation on the local member of colocated child region when
          // parent doesn't exist.
          // The delay is so that both parent and child regions will be created on another member
          // and the PR root config
          // will have an entry for the parent region.
          Thread.sleep(100);
          try {
            // Skip creation of first region - expect region2 creation to fail
            // createPR(PR_REGION_NAME, true);
            createPR("region2", getPartitionedRegionName(), true);
          } catch (Exception e) {
            ex = e;
            exClass = e.getClass().toString();
          } finally {
            return ex;
          }
        }
      };

  private SerializableCallable createPRsMissingChildRegionThread =
      new SerializableCallable("createPRsMissingChildRegion") {

        public Object call() throws Exception {

          try (MockAppender mockAppender = new MockAppender(ColocationLogger.class)) {
            createPR(getPartitionedRegionName(), true);
            // Let this thread continue running long enough for the missing region to be logged a
            // couple times.
            // Child regions do not get created by this thread.
            await().untilAsserted(
                () -> assertEquals(numExpectedLogMessages, mockAppender.getLogs().size()));

            return mockAppender.getLogs().get(0).getMessage().getFormattedMessage();
          } finally {
            numExpectedLogMessages = 1;
          }
        }
      };

  private SerializableCallable createPRsMissingChildRegionDelayedStartThread =
      new SerializableCallable("createPRsMissingChildRegionDelayedStart") {

        public Object call() throws Exception {

          try (MockAppender mockAppender = new MockAppender(ColocationLogger.class)) {
            createPR(getPartitionedRegionName(), true);
            // Delay creation of second (i.e child) region to see missing colocated region log
            // message (logInterval/2 < delay < logInterval)
            await().untilAsserted(
                () -> assertEquals(numExpectedLogMessages, mockAppender.getLogs().size()));
            createPR("region2", getPartitionedRegionName(), true);
            // Another delay before exiting the thread to make sure that missing region logging
            // doesn't continue after missing region is created (delay > logInterval)
            Thread.sleep(ColocationLogger.getLogInterval() + 10000);
            return mockAppender.getLogs().get(0).getMessage().getFormattedMessage();
          } finally {
            numExpectedLogMessages = 1;
          }
        }
      };

  private SerializableCallable createPRsSequencedChildrenCreationThread =
      new SerializableCallable("createPRsSequencedChildrenCreation") {

        public Object call() throws Exception {

          try (MockAppender mockAppender = new MockAppender(ColocationLogger.class)) {
            createPR(getPartitionedRegionName(), true);
            // Delay creation of child generation regions to see missing colocated region log
            // message
            // parent region is generation 1, child region is generation 2, grandchild is 3, etc.
            for (int generation = 2; generation < (numChildPRGenerations + 2); ++generation) {
              String childPRName = "region" + generation;
              String colocatedWithRegionName =
                  generation == 2 ? getPartitionedRegionName() : "region" + (generation - 1);

              // delay between starting generations of child regions until the expected missing
              // colocation messages are logged
              int n = (generation - 1) * generation / 2;
              await().untilAsserted(() -> assertEquals(n, mockAppender.getLogs().size()));

              // Start the child region
              createPR(childPRName, colocatedWithRegionName, true);
            }
            assertEquals(numExpectedLogMessages, mockAppender.getLogs().size());

            // Another delay before exiting the thread to make sure that missing region logging
            // doesn't continue after all regions are created (delay > logInterval)
            verify(mockAppender.getMock(), atLeastOnce()).getName();
            verify(mockAppender.getMock(), atLeastOnce()).isStarted();
            Thread.sleep(ColocationLogger.getLogInterval() + 10000);
            verifyNoMoreInteractions(mockAppender.getMock());
            return mockAppender.getLogs().get(0).getMessage().getFormattedMessage();
          } finally {
            numExpectedLogMessages = 1;
          }
        }
      };

  private SerializableCallable createMultipleColocatedChildPRsWithSequencedStart =
      new SerializableCallable("createPRsMultipleSequencedChildrenCreation") {

        public Object call() throws Exception {


          try (MockAppender mockAppender = new MockAppender(ColocationLogger.class)) {
            createPR(getPartitionedRegionName(), true);
            // Delay creation of child generation regions to see missing colocated region log
            // message
            for (int regionNum = 2; regionNum < (numChildPRs + 2); ++regionNum) {
              String childPRName = "region" + regionNum;

              // delay between starting generations of child regions until the expected missing
              // colocation messages are logged
              int n = regionNum - 1;
              await().untilAsserted(() -> assertEquals(n, mockAppender.getLogs().size()));
              int numLogEvents = mockAppender.getLogs().size();
              assertEquals("Expected warning messages to be logged.", regionNum - 1, numLogEvents);

              // Start the child region
              createPR(childPRName, getPartitionedRegionName(), true);
            }
            String logMsg = "";
            assertEquals(String.format("Expected warning messages to be logged."),
                numExpectedLogMessages, mockAppender.getLogs().size());
            logMsg = mockAppender.getLogs().get(0).getMessage().getFormattedMessage();

            // Another delay before exiting the thread to make sure that missing region logging
            // doesn't continue after all regions are created (delay > logInterval)
            verify(mockAppender.getMock(), atLeastOnce()).getName();
            verify(mockAppender.getMock(), atLeastOnce()).isStarted();
            Thread.sleep(ColocationLogger.getLogInterval() * 2);
            verifyNoMoreInteractions(mockAppender.getMock());
            return logMsg;
          } finally {

            numExpectedLogMessages = 1;
          }
        }
      };

  private class ColocationLoggerIntervalSetter extends SerializableRunnable {
    private int logInterval;

    ColocationLoggerIntervalSetter(int newInterval) {
      this.logInterval = newInterval;
    }

    @Override
    public void run() throws Exception {
      ColocationLogger.testhookSetLogInterval(logInterval);
    }
  }

  private class ColocationLoggerIntervalResetter extends SerializableRunnable {
    private int logInterval;

    @Override
    public void run() throws Exception {
      ColocationLogger.testhookResetLogInterval();
    }
  }

  private class ExpectedNumLogMessageSetter extends SerializableRunnable {
    private int numMsgs;

    ExpectedNumLogMessageSetter(int num) {
      this.numMsgs = num;
    }

    @Override
    public void run() throws Exception {
      numExpectedLogMessages = numMsgs;
    }
  }

  private class ExpectedNumLogMessageResetter extends SerializableRunnable {
    private int numMsgs;

    ExpectedNumLogMessageResetter() {
      this.numMsgs = DEFAULT_NUM_EXPECTED_LOG_MESSAGES;
    }

    @Override
    public void run() throws Exception {
      numExpectedLogMessages = numMsgs;
    }
  }

  private class NumChildPRsSetter extends SerializableRunnable {
    private int numChildren;

    NumChildPRsSetter(int num) {
      this.numChildren = num;
    }

    @Override
    public void run() throws Exception {
      numChildPRs = numChildren;
    }
  }

  private class NumChildPRGenerationsSetter extends SerializableRunnable {
    private int numGenerations;

    NumChildPRGenerationsSetter(int num) {
      this.numGenerations = num;
    }

    @Override
    public void run() throws Exception {
      numChildPRGenerations = numGenerations;
    }
  }

  private class DelayForChildCreationSetter extends SerializableRunnable {
    private int delay;

    DelayForChildCreationSetter(int millis) {
      this.delay = millis;
    }

    @Override
    public void run() throws Exception {
      delayForChildCreation = delay;
    }
  }

  /**
   * Testing that missing colocated persistent PRs are logged as warning
   */
  @Test
  public void testMissingColocatedParentPR() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(createPRsColocatedPairThread);
    vm1.invoke(createPRsColocatedPairThread);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertFalse(vm0Buckets.isEmpty());
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));

    closeCache(vm0);
    closeCache(vm1);

    // The following should fail immediately with ISE on vm0, it's not necessary to also try the
    // operation on vm1.
    Object remoteException = null;
    remoteException = vm0.invoke(createPRsMissingParentRegionThread);

    assertEquals("Expected IllegalState Exception for missing colocated parent region",
        IllegalStateException.class, remoteException.getClass());
    assertTrue("Expected IllegalState Exception for missing colocated parent region",
        remoteException.toString()
            .matches("java.lang.IllegalStateException: Region specified in 'colocated-with'.*"));
  }

  /**
   * Testing that parent colocated persistent PRs only missing on local member throws exception
   */
  @Test
  public void testMissingColocatedParentPRWherePRConfigExists() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(createPRsColocatedPairThread);
    vm1.invoke(createPRsColocatedPairThread);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertFalse(vm0Buckets.isEmpty());
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));

    closeCache(vm0);
    closeCache(vm1);

    AsyncInvocation async0 = null;
    AsyncInvocation async1a = null;
    AsyncInvocation async1b = null;
    try {
      async0 = vm0.invokeAsync(createPRsColocatedPairThread);

      Object logMsg = "";
      Object remoteException = null;
      async1a = vm1.invokeAsync(delayedCreatePRsMissingParentRegionThread);
      remoteException = async1a.get(MAX_WAIT, TimeUnit.MILLISECONDS);

      assertEquals("Expected IllegalState Exception for missing colocated parent region",
          IllegalStateException.class, remoteException.getClass());
      assertTrue("Expected IllegalState Exception for missing colocated parent region",
          remoteException.toString()
              .matches("java.lang.IllegalStateException: Region specified in 'colocated-with'.*"));
    } finally {
      // The real test is done now (either passing or failing) but there's some cleanup in this test
      // that needs to be done.
      //
      // The vm0 invokeAsync thread is still alive after the expected exception on vm1. Cleanup by
      // first re-creating both regions
      // on vm1, vm0 thread should now complete. Then wait (i.e. join() on the thread) for the new
      // vm1 thread and the vm0 thread to
      // verify they terminated without timing out, and close the caches.
      async1b = vm1.invokeAsync(createPRsColocatedPairThread);
      async1b.join(MAX_WAIT);
      async0.join(MAX_WAIT);
      closeCache(vm1);
      closeCache(vm0);
    }
  }

  /**
   * Testing that missing colocated child persistent PRs are logged as warning
   */
  @Test
  public void testMissingColocatedChildPRDueToDelayedStart() throws Throwable {
    int loggerTestInterval = 4000; // millis
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(createPRsColocatedPairThread);
    vm1.invoke(createPRsColocatedPairThread);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertFalse(vm0Buckets.isEmpty());
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));

    closeCache(vm0);
    closeCache(vm1);

    vm0.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm1.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm0.invoke(new ExpectedNumLogMessageSetter(1));
    vm1.invoke(new ExpectedNumLogMessageSetter(1));

    Object logMsg = "";
    AsyncInvocation async0 = vm0.invokeAsync(createPRsMissingChildRegionDelayedStartThread);
    AsyncInvocation async1 = vm1.invokeAsync(createPRsMissingChildRegionDelayedStartThread);
    logMsg = async1.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    async0.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    vm0.invoke(new ExpectedNumLogMessageResetter());
    vm1.invoke(new ExpectedNumLogMessageResetter());
    vm0.invoke(new ColocationLoggerIntervalResetter());
    vm1.invoke(new ColocationLoggerIntervalResetter());

    assertTrue(
        "Expected missing colocated region warning on remote. Got message \"" + logMsg + "\"",
        logMsg.toString().matches(PATTERN_FOR_MISSING_CHILD_LOG));
  }

  /**
   * Testing that missing colocated child persistent PRs are logged as warning
   */
  @Test
  public void testMissingColocatedChildPR() throws Throwable {
    int loggerTestInterval = 4000; // millis
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(createPRsColocatedPairThread);
    vm1.invoke(createPRsColocatedPairThread);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertFalse(vm0Buckets.isEmpty());
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));

    closeCache(vm0);
    closeCache(vm1);

    vm0.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm1.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm0.invoke(new ExpectedNumLogMessageSetter(2));
    vm1.invoke(new ExpectedNumLogMessageSetter(2));

    Object logMsg = "";
    AsyncInvocation async0 = vm0.invokeAsync(createPRsMissingChildRegionThread);
    AsyncInvocation async1 = vm1.invokeAsync(createPRsMissingChildRegionThread);
    logMsg = async1.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    async0.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    vm0.invoke(new ExpectedNumLogMessageResetter());
    vm1.invoke(new ExpectedNumLogMessageResetter());
    vm0.invoke(new ColocationLoggerIntervalResetter());
    vm1.invoke(new ColocationLoggerIntervalResetter());

    assertTrue(
        "Expected missing colocated region warning on remote. Got message \"" + logMsg + "\"",
        logMsg.toString().matches(PATTERN_FOR_MISSING_CHILD_LOG));
  }

  /**
   * Test that when there is more than one missing colocated child persistent PRs for a region all
   * missing regions are logged in the warning.
   */
  @Test
  public void testMultipleColocatedChildPRsMissing() throws Throwable {
    int loggerTestInterval = 4000; // millis
    int numChildPRs = 2;
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(new NumChildPRsSetter(numChildPRs));
    vm1.invoke(new NumChildPRsSetter(numChildPRs));
    vm0.invoke(createMultipleColocatedChildPRs);
    vm1.invoke(createMultipleColocatedChildPRs);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertFalse(vm0Buckets.isEmpty());
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertFalse(vm1Buckets.isEmpty());
    for (int i = 2; i < numChildPRs + 2; ++i) {
      String childName = "region" + i;
      assertEquals(vm0Buckets, getBucketList(vm0, childName));
      assertEquals(vm1Buckets, getBucketList(vm1, childName));
    }

    closeCache(vm0);
    closeCache(vm1);

    vm0.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm1.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm0.invoke(new ExpectedNumLogMessageSetter(2));
    vm1.invoke(new ExpectedNumLogMessageSetter(2));

    Object logMsg = "";
    AsyncInvocation async0 = vm0.invokeAsync(createPRsMissingChildRegionThread);
    AsyncInvocation async1 = vm1.invokeAsync(createPRsMissingChildRegionThread);
    logMsg = async1.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    async0.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    vm0.invoke(new ExpectedNumLogMessageResetter());
    vm1.invoke(new ExpectedNumLogMessageResetter());
    vm0.invoke(new ColocationLoggerIntervalResetter());
    vm1.invoke(new ColocationLoggerIntervalResetter());

    assertTrue(
        "Expected missing colocated region warning on remote. Got message \"" + logMsg + "\"",
        logMsg.toString().matches(PATTERN_FOR_MISSING_CHILD_LOG));
  }

  /**
   * Test that when there is more than one missing colocated child persistent PRs for a region all
   * missing regions are logged in the warning. Verifies that as regions are created they no longer
   * appear in the warning.
   */
  @Test
  public void testMultipleColocatedChildPRsMissingWithSequencedStart() throws Throwable {
    int loggerTestInterval = 4000; // millis
    int numChildPRs = 2;
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(new NumChildPRsSetter(numChildPRs));
    vm1.invoke(new NumChildPRsSetter(numChildPRs));
    vm0.invoke(createMultipleColocatedChildPRs);
    vm1.invoke(createMultipleColocatedChildPRs);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertFalse(vm0Buckets.isEmpty());
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertFalse(vm1Buckets.isEmpty());
    for (int i = 2; i < numChildPRs + 2; ++i) {
      String childName = "region" + i;
      assertEquals(vm0Buckets, getBucketList(vm0, childName));
      assertEquals(vm1Buckets, getBucketList(vm1, childName));
    }

    closeCache(vm0);
    closeCache(vm1);

    vm0.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm1.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm0.invoke(new ExpectedNumLogMessageSetter(2));
    vm1.invoke(new ExpectedNumLogMessageSetter(2));
    vm0.invoke(new DelayForChildCreationSetter((int) (loggerTestInterval)));
    vm1.invoke(new DelayForChildCreationSetter((int) (loggerTestInterval)));

    Object logMsg = "";
    AsyncInvocation async0 = vm0.invokeAsync(createMultipleColocatedChildPRsWithSequencedStart);
    AsyncInvocation async1 = vm1.invokeAsync(createMultipleColocatedChildPRsWithSequencedStart);
    logMsg = async1.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    async0.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    vm0.invoke(new ExpectedNumLogMessageResetter());
    vm1.invoke(new ExpectedNumLogMessageResetter());
    vm0.invoke(new ColocationLoggerIntervalResetter());
    vm1.invoke(new ColocationLoggerIntervalResetter());

    assertTrue(
        "Expected missing colocated region warning on remote. Got message \"" + logMsg + "\"",
        logMsg.toString().matches(PATTERN_FOR_MISSING_CHILD_LOG));
  }

  /**
   * Testing that all missing persistent PRs in a colocation hierarchy are logged as warnings
   */
  @Test
  public void testHierarchyOfColocatedChildPRsMissing() throws Throwable {
    int loggerTestInterval = 4000; // millis
    int numChildGenerations = 2;
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(new NumChildPRGenerationsSetter(numChildGenerations));
    vm1.invoke(new NumChildPRGenerationsSetter(numChildGenerations));
    vm0.invoke(createPRColocationHierarchy);
    vm1.invoke(createPRColocationHierarchy);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");
    createData(vm0, 0, NUM_BUCKETS, "c", "region3");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertFalse(vm0Buckets.isEmpty());
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertFalse(vm1Buckets.isEmpty());
    for (int i = 2; i < numChildGenerations + 2; ++i) {
      String childName = "region" + i;
      assertEquals(vm0Buckets, getBucketList(vm0, childName));
      assertEquals(vm1Buckets, getBucketList(vm1, childName));
    }

    closeCache(vm0);
    closeCache(vm1);

    vm0.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm1.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    // Expected warning logs only on the child region, because without the child there's nothing
    // known about the remaining hierarchy
    vm0.invoke(new ExpectedNumLogMessageSetter(numChildGenerations));
    vm1.invoke(new ExpectedNumLogMessageSetter(numChildGenerations));

    Object logMsg = "";
    AsyncInvocation async0 = vm0.invokeAsync(createPRsMissingChildRegionThread);
    AsyncInvocation async1 = vm1.invokeAsync(createPRsMissingChildRegionThread);
    logMsg = async1.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    async0.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    vm0.invoke(new ExpectedNumLogMessageResetter());
    vm1.invoke(new ExpectedNumLogMessageResetter());
    vm0.invoke(new ColocationLoggerIntervalResetter());
    vm1.invoke(new ColocationLoggerIntervalResetter());

    assertTrue(
        "Expected missing colocated region warning on remote. Got message \"" + logMsg + "\"",
        logMsg.toString().matches(PATTERN_FOR_MISSING_CHILD_LOG));
  }

  /**
   * Testing that all missing persistent PRs in a colocation hierarchy are logged as warnings
   */
  @Test
  public void testHierarchyOfColocatedChildPRsMissingGrandchild() throws Throwable {
    int loggerTestInterval = 4000; // millis
    int numChildGenerations = 3;
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(new NumChildPRGenerationsSetter(numChildGenerations));
    vm1.invoke(new NumChildPRGenerationsSetter(numChildGenerations));
    vm0.invoke(createPRColocationHierarchy);
    vm1.invoke(createPRColocationHierarchy);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");
    createData(vm0, 0, NUM_BUCKETS, "c", "region3");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertFalse(vm0Buckets.isEmpty());
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertFalse(vm1Buckets.isEmpty());
    for (int i = 2; i < numChildGenerations + 2; ++i) {
      String childName = "region" + i;
      assertEquals(vm0Buckets, getBucketList(vm0, childName));
      assertEquals(vm1Buckets, getBucketList(vm1, childName));
    }

    closeCache(vm0);
    closeCache(vm1);

    vm0.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm1.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    // Expected warning logs only on the child region, because without the child there's nothing
    // known about the remaining hierarchy
    vm0.invoke(
        new ExpectedNumLogMessageSetter(numChildGenerations * (numChildGenerations + 1) / 2));
    vm1.invoke(
        new ExpectedNumLogMessageSetter(numChildGenerations * (numChildGenerations + 1) / 2));
    vm0.invoke(new DelayForChildCreationSetter((int) (loggerTestInterval)));
    vm1.invoke(new DelayForChildCreationSetter((int) (loggerTestInterval)));

    Object logMsg = "";
    AsyncInvocation async0 = vm0.invokeAsync(createPRsSequencedChildrenCreationThread);
    AsyncInvocation async1 = vm1.invokeAsync(createPRsSequencedChildrenCreationThread);
    logMsg = async1.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    async0.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    vm0.invoke(new ExpectedNumLogMessageResetter());
    vm1.invoke(new ExpectedNumLogMessageResetter());
    vm0.invoke(new ColocationLoggerIntervalResetter());
    vm1.invoke(new ColocationLoggerIntervalResetter());

    assertTrue(
        "Expected missing colocated region warning on remote. Got message \"" + logMsg + "\"",
        logMsg.toString().matches(PATTERN_FOR_MISSING_CHILD_LOG));
  }

  private SerializableRunnable createPRColocationTree =
      new SerializableRunnable("create PR colocation hierarchy") {
        @Override
        public void run() throws Exception {
          createPR("Parent", true);
          createPR("Gen1_C1", "Parent", true);
          createPR("Gen1_C2", "Parent", true);
          createPR("Gen2_C1_1", "Gen1_C1", true);
          createPR("Gen2_C1_2", "Gen1_C1", true);
          createPR("Gen2_C2_1", "Gen1_C2", true);
          createPR("Gen2_C2_2", "Gen1_C2", true);
        }
      };

  /**
   * The colocation tree has the regions started in a specific order so that the logging is
   * predictable. For each entry in the list, the array values are:
   *
   * <pre>
   *
   *   [0] - the region name
   *   [1] - the name of that region's parent
   *   [2] - the number of warnings that will be logged after the region is created (1 warning for
   *         each region in the tree that exists that still has 1 or more missing children.)
   * </pre>
   */
  private static final List<Object[]> CHILD_REGION_RESTART_ORDER = new ArrayList<Object[]>();
  static {
    CHILD_REGION_RESTART_ORDER.add(new Object[] {"Gen1_C1", "Parent", 2});
    CHILD_REGION_RESTART_ORDER.add(new Object[] {"Gen2_C1_1", "Gen1_C1", 2});
    CHILD_REGION_RESTART_ORDER.add(new Object[] {"Gen1_C2", "Parent", 3});
    CHILD_REGION_RESTART_ORDER.add(new Object[] {"Gen2_C1_2", "Gen1_C1", 2});
    CHILD_REGION_RESTART_ORDER.add(new Object[] {"Gen2_C2_1", "Gen1_C2", 2});
    CHILD_REGION_RESTART_ORDER.add(new Object[] {"Gen2_C2_2", "Gen1_C2", 0});
  }

  /**
   * This thread starts up multiple colocated child regions in the sequence defined by
   * {@link #CHILD_REGION_RESTART_ORDER}. The complete startup sequence, which includes timed
   * periods waiting for log messages, takes at least 28 secs. Tests waiting for this
   * {@link SerializableCallable} to complete must have sufficient overhead in the wait for runtime
   * variations that exceed the minimum time to complete.
   */
  private SerializableCallable createPRsSequencedColocationTreeCreationThread =
      new SerializableCallable("createPRsSequencedColocationTreeCreation") {
        Appender mockAppender;
        ArgumentCaptor<LogEvent> loggingEventCaptor;

        public Object call() throws Exception {
          // Setup for capturing logger messages
          mockAppender = mock(Appender.class);
          when(mockAppender.getName()).thenReturn("MockAppender");
          when(mockAppender.isStarted()).thenReturn(true);
          when(mockAppender.isStopped()).thenReturn(false);
          Logger logger = (Logger) LogManager.getLogger(ColocationLogger.class);
          logger.addAppender(mockAppender);
          logger.setLevel(Level.WARN);
          loggingEventCaptor = ArgumentCaptor.forClass(LogEvent.class);

          // Logger interval may have been hooked by the test, so adjust test delays here
          int logInterval = ColocationLogger.getLogInterval();
          List<LogEvent> logEvents = Collections.emptyList();
          int nExpectedLogs = 1;

          createPR("Parent", true);
          // Delay creation of descendant regions in the hierarchy to see missing colocated region
          // log messages (logInterval/2 < delay < logInterval)
          for (Object[] regionInfo : CHILD_REGION_RESTART_ORDER) {
            loggingEventCaptor = ArgumentCaptor.forClass(LogEvent.class);
            String childPRName = (String) regionInfo[0];
            String colocatedWithRegionName = (String) regionInfo[1];

            // delay between starting generations of child regions and verify expected logging
            await().untilAsserted(() -> {
              verify(mockAppender, times(nExpectedLogs)).append(loggingEventCaptor.capture());
            });

            // Finally start the next child region
            createPR(childPRName, colocatedWithRegionName, true);
          }
          String logMsg;
          logEvents = loggingEventCaptor.getAllValues();
          assertEquals(String.format("Expected warning messages to be logged."), nExpectedLogs,
              logEvents.size());
          logMsg = logEvents.get(0).getMessage().getFormattedMessage();

          // acknowledge interactions with the mock that have occurred
          verify(mockAppender, atLeastOnce()).getName();
          verify(mockAppender, atLeastOnce()).isStarted();
          try {
            // Another delay before exiting the thread to make sure that missing region logging
            // doesn't continue after all regions are created (delay > logInterval)
            verify(mockAppender, atLeastOnce()).append(any(LogEvent.class));
            Thread.sleep(logInterval * 2);
            verifyNoMoreInteractions(mockAppender);
          } finally {
            logger.removeAppender(mockAppender);
          }

          numExpectedLogMessages = 1;
          mockAppender = null;
          return logMsg;
        }
      };

  /**
   * Testing that all missing persistent PRs in a colocation tree hierarchy are logged as warnings.
   * This test is a combines the "multiple children" and "hierarchy of children" tests. This is the
   * colocation tree for this test
   *
   * <pre>
   *                  Parent
   *                /         \
   *             /               \
   *         Gen1_C1            Gen1_C2
   *         /    \              /    \
   *  Gen2_C1_1  Gen2_C1_2  Gen2_C2_1  Gen2_C2_2
   * </pre>
   */
  @Test
  public void testFullTreeOfColocatedChildPRsWithMissingRegions() throws Throwable {
    int loggerTestInterval = 4000; // millis
    int numChildPRs = 2;
    int numChildGenerations = 2;
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(createPRColocationTree);
    vm1.invoke(createPRColocationTree);

    createData(vm0, 0, NUM_BUCKETS, "a", "Parent");
    createData(vm0, 0, NUM_BUCKETS, "b", "Gen1_C1");
    createData(vm0, 0, NUM_BUCKETS, "c", "Gen1_C2");
    createData(vm0, 0, NUM_BUCKETS, "c", "Gen2_C1_1");
    createData(vm0, 0, NUM_BUCKETS, "c", "Gen2_C1_2");
    createData(vm0, 0, NUM_BUCKETS, "c", "Gen2_C2_1");
    createData(vm0, 0, NUM_BUCKETS, "c", "Gen2_C2_2");

    Set<Integer> vm0Buckets = getBucketList(vm0, "Parent");
    assertFalse(vm0Buckets.isEmpty());
    Set<Integer> vm1Buckets = getBucketList(vm1, "Parent");
    assertFalse(vm1Buckets.isEmpty());
    for (String region : new String[] {"Gen1_C1", "Gen1_C2", "Gen2_C1_1", "Gen2_C1_2", "Gen2_C2_1",
        "Gen2_C2_2"}) {
      assertEquals(vm0Buckets, getBucketList(vm0, region));
      assertEquals(vm1Buckets, getBucketList(vm1, region));
    }

    closeCache(vm0);
    closeCache(vm1);

    vm0.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm1.invoke(new ColocationLoggerIntervalSetter(loggerTestInterval));
    vm0.invoke(new DelayForChildCreationSetter((int) (loggerTestInterval)));
    vm1.invoke(new DelayForChildCreationSetter((int) (loggerTestInterval)));

    Object logMsg = "";
    AsyncInvocation async0 = vm0.invokeAsync(createPRsSequencedColocationTreeCreationThread);
    AsyncInvocation async1 = vm1.invokeAsync(createPRsSequencedColocationTreeCreationThread);
    logMsg = async1.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    async0.get(MAX_WAIT, TimeUnit.MILLISECONDS);
    vm0.invoke(new ColocationLoggerIntervalResetter());
    vm1.invoke(new ColocationLoggerIntervalResetter());

    // Expected warning logs only on the child region, because without the child there's nothing
    // known about the remaining hierarchy
    assertTrue(
        "Expected missing colocated region warning on remote. Got message \"" + logMsg + "\"",
        logMsg.toString().matches(PATTERN_FOR_MISSING_CHILD_LOG));
  }

  /**
   * Testing what happens we we recreate colocated persistent PRs by creating one PR everywhere and
   * then the other PR everywhere.
   */
  @Test
  public void testColocatedPRsRecoveryOnePRAtATime() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createParentPR = new SerializableRunnable("createParentPR") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());
      }
    };
    SerializableRunnable createChildPR = getCreateChildPRRunnable();
    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm2.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    vm2.invoke(createChildPR);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    Set<Integer> vm2Buckets = getBucketList(vm2, getPartitionedRegionName());
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));

    Set<Integer> vm0PrimaryBuckets = getPrimaryBucketList(vm0, getPartitionedRegionName());
    assertEquals(vm0PrimaryBuckets, getPrimaryBucketList(vm0, "region2"));
    Set<Integer> vm1PrimaryBuckets = getPrimaryBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1PrimaryBuckets, getPrimaryBucketList(vm1, "region2"));
    Set<Integer> vm2PrimaryBuckets = getPrimaryBucketList(vm2, getPartitionedRegionName());
    assertEquals(vm2PrimaryBuckets, getPrimaryBucketList(vm2, "region2"));

    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);

    AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
    AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
    AsyncInvocation async2 = vm2.invokeAsync(createParentPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);

    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    vm2.invoke(createChildPR);

    Wait.pause(4000);

    assertEquals(vm0Buckets, getBucketList(vm0, getPartitionedRegionName()));
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    assertEquals(vm1Buckets, getBucketList(vm1, getPartitionedRegionName()));
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    assertEquals(vm2Buckets, getBucketList(vm2, getPartitionedRegionName()));
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));

    // primary can differ
    vm0PrimaryBuckets = getPrimaryBucketList(vm0, getPartitionedRegionName());
    assertEquals(vm0PrimaryBuckets, getPrimaryBucketList(vm0, "region2"));
    vm1PrimaryBuckets = getPrimaryBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1PrimaryBuckets, getPrimaryBucketList(vm1, "region2"));
    vm2PrimaryBuckets = getPrimaryBucketList(vm2, getPartitionedRegionName());
    assertEquals(vm2PrimaryBuckets, getPrimaryBucketList(vm2, "region2"));


    checkData(vm0, 0, NUM_BUCKETS, "a");

    // region 2 didn't have persistent data, so it nothing should be recovered
    checkData(vm0, 0, NUM_BUCKETS, null, "region2");

    // Make sure can do a put in all of the buckets in vm2
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");

    // Now all of those buckets should exist
    checkData(vm0, 0, NUM_BUCKETS, "c", "region2");

    // Now all the buckets should be restored in the appropriate places.
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));
  }

  private SerializableRunnable getCreateChildPRRunnable() {
    return new SerializableRunnable("createChildPR") {
      public void run() {
        Cache cache = getCache();

        final CountDownLatch recoveryDone = new CountDownLatch(1);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            if (region.getName().equals("region2")) {
              recoveryDone.countDown();
            }
          }
        };
        InternalResourceManager.setResourceObserver(observer);

        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setColocatedWith(getPartitionedRegionName());
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());

        try {
          recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };
  }

  @Test
  public void testColocatedPRsRecoveryOneMemberLater() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createParentPR = new SerializableRunnable("createParentPR") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());
      }
    };
    SerializableRunnable createChildPR = getCreateChildPRRunnable();

    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm2.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    vm2.invoke(createChildPR);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, getPartitionedRegionName());
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    Set<Integer> vm1Buckets = getBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    Set<Integer> vm2Buckets = getBucketList(vm2, getPartitionedRegionName());
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));

    Set<Integer> vm0PrimaryBuckets = getPrimaryBucketList(vm0, getPartitionedRegionName());
    assertEquals(vm0PrimaryBuckets, getPrimaryBucketList(vm0, "region2"));
    Set<Integer> vm1PrimaryBuckets = getPrimaryBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1PrimaryBuckets, getPrimaryBucketList(vm1, "region2"));
    Set<Integer> vm2PrimaryBuckets = getPrimaryBucketList(vm2, getPartitionedRegionName());
    assertEquals(vm2PrimaryBuckets, getPrimaryBucketList(vm2, "region2"));

    closeCache(vm2);
    // Make sure the other members notice that vm2 has gone
    // TODO use a callback for this.
    Thread.sleep(4000);

    closeCache(vm0);
    closeCache(vm1);

    // Create the members, but don't initialize
    // VM2 yet
    AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
    AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);

    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    waitForBucketRecovery(vm0, vm0Buckets);
    waitForBucketRecovery(vm1, vm1Buckets);

    checkData(vm0, 0, NUM_BUCKETS, "a");

    // region 2 didn't have persistent data, so it nothing should be recovered
    checkData(vm0, 0, NUM_BUCKETS, null, "region2");

    // Make sure can do a put in all of the buckets in vm2
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");

    // Now all of those buckets should exist
    checkData(vm0, 0, NUM_BUCKETS, "c", "region2");

    // Now we initialize vm2.
    vm2.invoke(createParentPR);
    // Make sure vm2 hasn't created any buckets in the parent PR yet
    // We don't want any buckets until the child PR is created
    assertEquals(Collections.emptySet(), getBucketList(vm2, getPartitionedRegionName()));
    vm2.invoke(createChildPR);

    // Now vm2 should have created all of the appropriate buckets.
    assertEquals(vm2Buckets, getBucketList(vm2, getPartitionedRegionName()));
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));

    vm0PrimaryBuckets = getPrimaryBucketList(vm0, getPartitionedRegionName());
    assertEquals(vm0PrimaryBuckets, getPrimaryBucketList(vm0, "region2"));
    vm1PrimaryBuckets = getPrimaryBucketList(vm1, getPartitionedRegionName());
    assertEquals(vm1PrimaryBuckets, getPrimaryBucketList(vm1, "region2"));
    vm2PrimaryBuckets = getPrimaryBucketList(vm2, getPartitionedRegionName());
    assertEquals(vm2PrimaryBuckets, getPrimaryBucketList(vm2, "region2"));
  }

  @Test
  public void testReplaceOfflineMemberAndRestart() throws Throwable {
    SerializableRunnable createPRs = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }

        final CountDownLatch recoveryDone = new CountDownLatch(2);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            recoveryDone.countDown();
          }
        };
        InternalResourceManager.setResourceObserver(observer);

        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());

        paf.setColocatedWith(getPartitionedRegionName());
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());

        try {
          if (!recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS)) {
            fail("timed out");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };

    replaceOfflineMemberAndRestart(createPRs);
  }

  /**
   * Test that if we replace an offline member, even if colocated regions are in different disk
   * stores, we still keep our metadata consistent.
   *
   */
  @Test
  public void testReplaceOfflineMemberAndRestartTwoDiskStores() throws Throwable {
    SerializableRunnable createPRs = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }

        final CountDownLatch recoveryDone = new CountDownLatch(2);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            recoveryDone.countDown();
          }
        };
        InternalResourceManager.setResourceObserver(observer);

        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());

        DiskStore ds2 = cache.findDiskStore("disk2");
        if (ds2 == null) {
          ds2 = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk2");
        }

        paf.setColocatedWith(getPartitionedRegionName());
        af.setPartitionAttributes(paf.create());
        af.setDiskStoreName("disk2");
        cache.createRegion("region2", af.create());

        try {
          if (!recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS)) {
            fail("timed out");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };

    replaceOfflineMemberAndRestart(createPRs);
  }

  /**
   * Test for support issue 7870. 1. Run three members with redundancy 1 and recovery delay 0 2.
   * Kill one of the members, to trigger replacement of buckets 3. Shutdown all members and restart.
   *
   * What was happening is that in the parent PR, we discarded our offline data in one member, but
   * in the child PR the other members ended up waiting for the child bucket to be created in the
   * member that discarded it's offline data.
   *
   */
  public void replaceOfflineMemberAndRestart(SerializableRunnable createPRs) throws Throwable {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    // Create the PR on three members
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    vm2.invoke(createPRs);

    // Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");

    // Close one of the members to trigger redundancy recovery.
    closeCache(vm2);

    // Wait until redundancy is recovered.
    waitForRedundancyRecovery(vm0, 1, getPartitionedRegionName());
    waitForRedundancyRecovery(vm0, 1, "region2");

    createData(vm0, 0, NUM_BUCKETS, "b");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");
    IgnoredException expected = IgnoredException.addIgnoredException("PartitionOfflineException");
    try {

      // Close the remaining members.
      vm0.invoke(new SerializableCallable() {

        public Object call() throws Exception {
          InternalDistributedSystem ds =
              (InternalDistributedSystem) getCache().getDistributedSystem();
          AdminDistributedSystemImpl.shutDownAllMembers(ds.getDistributionManager(), 600000);
          return null;
        }
      });

      // Make sure that vm-1 is completely disconnected
      // The shutdown all asynchronously finishes the disconnect after
      // replying to the admin member.
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          basicGetSystem().disconnect();
        }
      });

      // Recreate the members. Try to make sure that
      // the member with the latest copy of the buckets
      // is the one that decides to throw away it's copy
      // by starting it last.
      AsyncInvocation async0 = vm0.invokeAsync(createPRs);
      AsyncInvocation async1 = vm1.invokeAsync(createPRs);
      Wait.pause(2000);
      AsyncInvocation async2 = vm2.invokeAsync(createPRs);
      async0.getResult(MAX_WAIT);
      async1.getResult(MAX_WAIT);
      async2.getResult(MAX_WAIT);

      checkData(vm0, 0, NUM_BUCKETS, "b");
      checkData(vm0, 0, NUM_BUCKETS, "b", "region2");

      waitForRedundancyRecovery(vm0, 1, getPartitionedRegionName());
      waitForRedundancyRecovery(vm0, 1, "region2");
      waitForRedundancyRecovery(vm1, 1, getPartitionedRegionName());
      waitForRedundancyRecovery(vm1, 1, "region2");
      waitForRedundancyRecovery(vm2, 1, getPartitionedRegionName());
      waitForRedundancyRecovery(vm2, 1, "region2");

      // Make sure we don't have any extra buckets after the restart
      int totalBucketCount = getBucketList(vm0).size();
      totalBucketCount += getBucketList(vm1).size();
      totalBucketCount += getBucketList(vm2).size();

      assertEquals(2 * NUM_BUCKETS, totalBucketCount);

      totalBucketCount = getBucketList(vm0, "region2").size();
      totalBucketCount += getBucketList(vm1, "region2").size();
      totalBucketCount += getBucketList(vm2, "region2").size();

      assertEquals(2 * NUM_BUCKETS, totalBucketCount);
    } finally {
      expected.remove();
    }
  }

  @Test
  public void testReplaceOfflineMemberAndRestartCreateColocatedPRLate() throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());
      }
    };

    SerializableRunnable createChildPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();

        final CountDownLatch recoveryDone = new CountDownLatch(1);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            if (region.getName().contains("region2")) {
              recoveryDone.countDown();
            }
          }
        };
        InternalResourceManager.setResourceObserver(observer);

        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        paf.setColocatedWith(getPartitionedRegionName());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());

        try {
          if (!recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS)) {
            fail("timed out");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };

    replaceOfflineMemberAndRestartCreateColocatedPRLate(createParentPR, createChildPR);
  }

  @Test
  public void testReplaceOfflineMemberAndRestartCreateColocatedPRLateTwoDiskStores()
      throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());
      }
    };

    SerializableRunnable createChildPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();

        final CountDownLatch recoveryDone = new CountDownLatch(1);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            if (region.getName().contains("region2")) {
              recoveryDone.countDown();
            }
          }
        };
        InternalResourceManager.setResourceObserver(observer);

        DiskStore ds2 = cache.findDiskStore("disk2");
        if (ds2 == null) {
          ds2 = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk2");
        }

        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        paf.setColocatedWith(getPartitionedRegionName());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk2");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());

        try {
          if (!recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS)) {
            fail("timed out");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };

    replaceOfflineMemberAndRestartCreateColocatedPRLate(createParentPR, createChildPR);
  }

  /**
   * Test for support issue 7870. 1. Run three members with redundancy 1 and recovery delay 0 2.
   * Kill one of the members, to trigger replacement of buckets 3. Shutdown all members and restart.
   *
   * What was happening is that in the parent PR, we discarded our offline data in one member, but
   * in the child PR the other members ended up waiting for the child bucket to be created in the
   * member that discarded it's offline data.
   *
   * In this test case, we're creating the child PR later, after the parent buckets have already
   * been completely created.
   *
   */
  public void replaceOfflineMemberAndRestartCreateColocatedPRLate(
      SerializableRunnable createParentPR, SerializableRunnable createChildPR) throws Throwable {
    IgnoredException.addIgnoredException("PartitionOfflineException");
    IgnoredException.addIgnoredException("RegionDestroyedException");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);



    // Create the PRs on three members
    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm2.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    vm2.invoke(createChildPR);

    // Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");

    // Close one of the members to trigger redundancy recovery.
    closeCache(vm2);

    // Wait until redundancy is recovered.
    waitForRedundancyRecovery(vm0, 1, getPartitionedRegionName());
    waitForRedundancyRecovery(vm0, 1, "region2");

    createData(vm0, 0, NUM_BUCKETS, "b");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");

    // Close the remaining members.
    vm0.invoke(new SerializableCallable() {

      public Object call() throws Exception {
        InternalDistributedSystem ds =
            (InternalDistributedSystem) getCache().getDistributedSystem();
        AdminDistributedSystemImpl.shutDownAllMembers(ds.getDistributionManager(), 0);
        return null;
      }
    });

    // Make sure that vm-1 is completely disconnected
    // The shutdown all asynchronously finishes the disconnect after
    // replying to the admin member.
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        basicGetSystem().disconnect();
      }
    });

    // Recreate the parent region. Try to make sure that
    // the member with the latest copy of the buckets
    // is the one that decides to throw away it's copy
    // by starting it last.
    AsyncInvocation async2 = vm2.invokeAsync(createParentPR);
    AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
    Wait.pause(2000);
    AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);

    // Wait for async tasks
    Wait.pause(2000);

    // Recreate the child region.
    async2 = vm2.invokeAsync(createChildPR);
    async1 = vm1.invokeAsync(createChildPR);
    async0 = vm0.invokeAsync(createChildPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);

    // Validate the data
    checkData(vm0, 0, NUM_BUCKETS, "b");
    checkData(vm0, 0, NUM_BUCKETS, "b", "region2");

    // Make sure we can actually use the buckets in the child region.
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");

    waitForRedundancyRecovery(vm0, 1, getPartitionedRegionName());
    waitForRedundancyRecovery(vm0, 1, "region2");

    // Make sure we don't have any extra buckets after the restart
    int totalBucketCount = getBucketList(vm0).size();
    totalBucketCount += getBucketList(vm1).size();
    totalBucketCount += getBucketList(vm2).size();

    assertEquals(2 * NUM_BUCKETS, totalBucketCount);

    totalBucketCount = getBucketList(vm0, "region2").size();
    totalBucketCount += getBucketList(vm1, "region2").size();
    totalBucketCount += getBucketList(vm2, "region2").size();

    assertEquals(2 * NUM_BUCKETS, totalBucketCount);
  }

  /**
   * Test what happens when we crash in the middle of satisfying redundancy for a colocated bucket.
   *
   */
  // This test method is disabled because it is failing
  // periodically and causing cruise control failures
  // See bug #46748
  @Test
  public void testCrashDuringRedundancySatisfaction() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable createPRs = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        // Workaround for 44414 - disable recovery delay so we shutdown
        // vm1 at a predictable point.
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());

        paf.setColocatedWith(getPartitionedRegionName());
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };

    // Create the PR on vm0
    vm0.invoke(createPRs);


    // Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");

    vm1.invoke(createPRs);

    // We shouldn't have created any buckets in vm1 yet.
    assertEquals(Collections.emptySet(), getBucketList(vm1));

    // Add an observer that will disconnect before allowing the peer to
    // GII a colocated bucket. This should leave the peer with only the parent
    // bucket
    vm0.invoke(new SerializableRunnable() {

      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          @Override
          public void beforeProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof RequestImageMessage) {
              if (((RequestImageMessage) message).regionPath.contains("region2")) {
                DistributionMessageObserver.setInstance(null);
                disconnectFromDS();
              }
            }
          }
        });
      }
    });

    IgnoredException ex = IgnoredException.addIgnoredException("PartitionOfflineException", vm1);
    try {

      // Do a rebalance to create buckets in vm1. THis will cause vm0 to disconnect
      // as we satisfy redundancy with vm1.
      try {
        RebalanceResults rr = rebalance(vm1);
      } catch (Exception expected) {
        // We expect to see a partition offline exception because of the
        // disconnect
        if (!(expected.getCause() instanceof PartitionOfflineException)) {
          throw expected;
        }
      }


      // Wait for vm0 to be closed by the callback
      vm0.invoke(new SerializableCallable() {

        public Object call() throws Exception {
          GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
            public boolean done() {
              InternalDistributedSystem ds = basicGetSystem();
              return ds == null || !ds.isConnected();
            }

            public String description() {
              return "DS did not disconnect";
            }
          });

          return null;
        }
      });

      // close the cache in vm1
      SerializableCallable disconnectFromDS = new SerializableCallable() {

        public Object call() throws Exception {
          disconnectFromDS();
          return null;
        }
      };
      vm1.invoke(disconnectFromDS);

      // Make sure vm0 is disconnected. This avoids a race where we
      // may still in the process of disconnecting even though the our async listener
      // found the system was disconnected
      vm0.invoke(disconnectFromDS);
    } finally {
      ex.remove();
    }

    // Create the cache and PRs on both members
    AsyncInvocation async0 = vm0.invokeAsync(createPRs);
    AsyncInvocation async1 = vm1.invokeAsync(createPRs);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);

    // Make sure the data was recovered correctly
    checkData(vm0, 0, NUM_BUCKETS, "a");
    // Workaround for bug 46748.
    checkData(vm0, 0, NUM_BUCKETS, "a", "region2");
  }

  @Test
  public void testRebalanceWithOfflineChildRegion() throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable("createParentPR") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());
      }
    };

    SerializableRunnable createChildPR = new SerializableRunnable("createChildPR") {
      public void run() {
        Cache cache = getCache();

        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(0);
        paf.setColocatedWith(getPartitionedRegionName());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };

    rebalanceWithOfflineChildRegion(createParentPR, createChildPR);

  }

  /**
   * Test that a rebalance will regions are in the middle of recovery doesn't cause issues.
   *
   * This is slightly different than {{@link #testRebalanceWithOfflineChildRegion()} because in this
   * case all of the regions have been created, but they are in the middle of actually recovering
   * buckets from disk.
   */
  @Test
  public void testRebalanceDuringRecovery() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPRs = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());

        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setColocatedWith(getPartitionedRegionName());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };


    // Create the PRs on two members
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);

    // Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");

    // Close the members
    closeCache(vm1);
    closeCache(vm0);

    SerializableRunnable addHook = new SerializableRunnable() {
      @Override
      public void run() {
        PartitionedRegionObserverHolder.setInstance(new PRObserver());
      }
    };

    SerializableRunnable waitForHook = new SerializableRunnable() {
      @Override
      public void run() {
        PRObserver observer = (PRObserver) PartitionedRegionObserverHolder.getInstance();
        try {
          observer.waitForCreate();
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };

    SerializableRunnable removeHook = new SerializableRunnable() {
      @Override
      public void run() {
        PRObserver observer = (PRObserver) PartitionedRegionObserverHolder.getInstance();
        observer.release();
        PartitionedRegionObserverHolder.setInstance(new PartitionedRegionObserverAdapter());
      }
    };

    vm1.invoke(addHook);
    AsyncInvocation async0;
    AsyncInvocation async1;
    AsyncInvocation async2;
    RebalanceResults rebalanceResults;
    try {
      async0 = vm0.invokeAsync(createPRs);
      async1 = vm1.invokeAsync(createPRs);

      vm1.invoke(waitForHook);

      // Now create the parent region on vm-2. vm-2 did not
      // previous host the child region.
      vm2.invoke(createPRs);

      // Try to forcibly move some buckets to vm2 (this should not succeed).
      moveBucket(0, vm1, vm2);
      moveBucket(1, vm1, vm2);

    } finally {
      vm1.invoke(removeHook);
    }

    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);

    // Validate the data
    checkData(vm0, 0, NUM_BUCKETS, "a");
    checkData(vm0, 0, NUM_BUCKETS, "a", "region2");

    // Make sure we can actually use the buckets in the child region.
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");

    // Make sure the system is recoverable
    // by restarting it
    closeCache(vm2);
    closeCache(vm1);
    closeCache(vm0);

    async0 = vm0.invokeAsync(createPRs);
    async1 = vm1.invokeAsync(createPRs);
    async2 = vm2.invokeAsync(createPRs);
    async0.getResult();
    async1.getResult();
    async2.getResult();
  }

  @Test
  public void testRebalanceWithOfflineChildRegionTwoDiskStores() throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(getPartitionedRegionName(), af.create());
      }
    };

    SerializableRunnable createChildPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();

        DiskStore ds2 = cache.findDiskStore("disk2");
        if (ds2 == null) {
          ds2 = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk2");
        }

        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(0);
        paf.setColocatedWith(getPartitionedRegionName());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk2");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };

    rebalanceWithOfflineChildRegion(createParentPR, createChildPR);
  }

  /**
   * Test that a user is not allowed to change the colocation of a PR with persistent data.
   *
   */
  @Test
  public void testModifyColocation() throws Throwable {
    // Create PRs where region3 is colocated with region1.
    createColocatedPRs("region1");

    // Close everything
    closeCache();

    // Restart colocated with "region2"
    IgnoredException ex =
        IgnoredException.addIgnoredException("DiskAccessException|IllegalStateException");
    try {
      createColocatedPRs("region2");
      fail("Should have received an illegal state exception");
    } catch (IllegalStateException expected) {
      // do nothing
    } finally {
      ex.remove();
    }

    // Close everything
    closeCache();

    // Restart colocated with region1.
    // Make sure we didn't screw anything up.
    createColocatedPRs("/region1");

    // Close everything
    closeCache();

    // Restart uncolocated. We don't allow changing
    // from uncolocated to colocated.
    ex = IgnoredException.addIgnoredException("DiskAccessException|IllegalStateException");
    try {
      createColocatedPRs(null);
      fail("Should have received an illegal state exception");
    } catch (IllegalStateException expected) {
      // do nothing
    } finally {
      ex.remove();
    }

    // Close everything
    closeCache();
  }

  @Test
  public void testParentRegionGetWithOfflineChildRegion() throws Throwable {

    SerializableRunnable createParentPR = new SerializableRunnable("createParentPR") {
      public void run() {
        String oldRetryTimeout = System.setProperty(
            DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout", "10000");
        try {
          Cache cache = getCache();
          DiskStore ds = cache.findDiskStore("disk");
          if (ds == null) {
            ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
          }
          AttributesFactory af = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(0);
          paf.setRecoveryDelay(0);
          af.setPartitionAttributes(paf.create());
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName("disk");
          cache.createRegion(getPartitionedRegionName(), af.create());
        } finally {
          System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout",
              String.valueOf(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
        }
      }
    };

    SerializableRunnable createChildPR = new SerializableRunnable("createChildPR") {
      public void run() throws InterruptedException {
        String oldRetryTimeout = System.setProperty(
            DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout", "10000");
        try {
          Cache cache = getCache();
          AttributesFactory af = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(0);
          paf.setRecoveryDelay(0);
          paf.setColocatedWith(getPartitionedRegionName());
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName("disk");
          af.setPartitionAttributes(paf.create());
          // delay child region creations to cause a delay in persistent recovery
          Thread.sleep(100);
          cache.createRegion("region2", af.create());
        } finally {
          System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout",
              String.valueOf(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
        }
      }
    };

    boolean caughtException = false;
    try {
      // Expect a get() on the un-recovered (due to offline child) parent region to fail
      regionGetWithOfflineChild(createParentPR, createChildPR, false);
    } catch (Exception e) {
      caughtException = true;
      assertTrue(e instanceof RMIException);
      assertTrue(e.getCause() instanceof PartitionOfflineException);
    }
    if (!caughtException) {
      fail("Expected TimeoutException from remote");
    }
  }

  @Test
  public void testParentRegionGetWithRecoveryInProgress() throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable("createParentPR") {
      public void run() {
        String oldRetryTimeout = System.setProperty(
            DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout", "10000");
        try {
          Cache cache = getCache();
          DiskStore ds = cache.findDiskStore("disk");
          if (ds == null) {
            ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
          }
          AttributesFactory af = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(0);
          paf.setRecoveryDelay(0);
          af.setPartitionAttributes(paf.create());
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName("disk");
          cache.createRegion(getPartitionedRegionName(), af.create());
        } finally {
          System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout",
              String.valueOf(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
          System.out.println("oldRetryTimeout = " + oldRetryTimeout);
        }
      }
    };

    SerializableRunnable createChildPR = new SerializableRunnable("createChildPR") {
      public void run() throws InterruptedException {
        String oldRetryTimeout = System.setProperty(
            DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout", "10000");
        try {
          Cache cache = getCache();
          AttributesFactory af = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(0);
          paf.setRecoveryDelay(0);
          paf.setColocatedWith(getPartitionedRegionName());
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName("disk");
          af.setPartitionAttributes(paf.create());
          cache.createRegion("region2", af.create());
        } finally {
          System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout",
              String.valueOf(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
        }
      }
    };

    boolean caughtException = false;
    try {
      // Expect a get() on the un-recovered (due to offline child) parent region to fail
      regionGetWithOfflineChild(createParentPR, createChildPR, false);
    } catch (Exception e) {
      caughtException = true;
      assertTrue(e instanceof RMIException);
      assertTrue(e.getCause() instanceof PartitionOfflineException);
    }
    if (!caughtException) {
      fail("Expected TimeoutException from remote");
    }
  }

  @Test
  public void testParentRegionPutWithRecoveryInProgress() throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable("createParentPR") {
      public void run() {
        String oldRetryTimeout = System.setProperty(
            DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout", "10000");
        System.out.println("oldRetryTimeout = " + oldRetryTimeout);
        try {
          Cache cache = getCache();
          DiskStore ds = cache.findDiskStore("disk");
          if (ds == null) {
            ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
          }
          AttributesFactory af = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(0);
          paf.setRecoveryDelay(0);
          af.setPartitionAttributes(paf.create());
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName("disk");
          cache.createRegion(getPartitionedRegionName(), af.create());
        } finally {
          System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout",
              String.valueOf(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
        }
      }
    };

    SerializableRunnable createChildPR = new SerializableRunnable("createChildPR") {
      public void run() throws InterruptedException {
        String oldRetryTimeout = System.setProperty(
            DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout", "10000");
        try {
          Cache cache = getCache();
          AttributesFactory af = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(0);
          paf.setRecoveryDelay(0);
          paf.setColocatedWith(getPartitionedRegionName());
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName("disk");
          af.setPartitionAttributes(paf.create());
          Thread.sleep(1000);
          cache.createRegion("region2", af.create());
        } finally {
          System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout",
              String.valueOf(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
        }
      }
    };

    boolean caughtException = false;
    try {
      // Expect a get() on the un-recovered (due to offline child) parent region to fail
      regionGetWithOfflineChild(createParentPR, createChildPR, false);
    } catch (Exception e) {
      caughtException = true;
      assertTrue(e instanceof RMIException);
      assertTrue(e.getCause() instanceof PartitionOfflineException);
    }
    if (!caughtException) {
      fail("Expected TimeoutException from remote");
    }
  }

  /**
   * Create three PRs on a VM, named region1, region2, and region3. The colocated with attribute
   * describes which region region3 should be colocated with.
   *
   */
  private void createColocatedPRs(final String colocatedWith) {
    Cache cache = getCache();

    DiskStore ds = cache.findDiskStore("disk");
    if (ds == null) {
      ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
    }
    AttributesFactory af = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(0);
    af.setPartitionAttributes(paf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    af.setDiskStoreName("disk");
    cache.createRegion("region1", af.create());

    cache.createRegion("region2", af.create());

    if (colocatedWith != null) {
      paf.setColocatedWith(colocatedWith);
    }
    af.setPartitionAttributes(paf.create());
    cache.createRegion("region3", af.create());
  }

  /**
   * Test for bug 43570. Rebalance a persistent parent PR before we recover the persistent child PR
   * from disk.
   *
   */
  public void rebalanceWithOfflineChildRegion(SerializableRunnable createParentPR,
      SerializableRunnable createChildPR) throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);



    // Create the PRs on two members
    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);

    // Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");

    // Close the members
    closeCache(vm1);
    closeCache(vm0);

    // Recreate the parent region. Try to make sure that
    // the member with the latest copy of the buckets
    // is the one that decides to throw away it's copy
    // by starting it last.
    AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
    AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);

    // Now create the parent region on vm-2. vm-2 did not
    // previous host the child region.
    vm2.invoke(createParentPR);

    // Rebalance the parent region.
    // This should not move any buckets, because
    // we haven't recovered the child region
    RebalanceResults rebalanceResults = rebalance(vm2);
    assertEquals(0, rebalanceResults.getTotalBucketTransfersCompleted());

    // Recreate the child region.
    async1 = vm1.invokeAsync(createChildPR);
    async0 = vm0.invokeAsync(createChildPR);
    AsyncInvocation async2 = vm2.invokeAsync(createChildPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);

    // Validate the data
    checkData(vm0, 0, NUM_BUCKETS, "a");
    checkData(vm0, 0, NUM_BUCKETS, "a", "region2");

    // Make sure we can actually use the buckets in the child region.
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");
  }

  /**
   * Create a colocated pair of persistent regions and populate them with data. Shut down the
   * servers and then restart them and check the data.
   * <p>
   * On the restart, try region operations ({@code get()}) on the parent region before or during
   * persistent recovery. The {@code concurrentCheckData} argument determines whether the operation
   * from the parent region occurs before or concurrent with the child region creation and recovery.
   *
   * @param createParentPR {@link SerializableRunnable} for creating the parent region on one member
   * @param createChildPR {@link SerializableRunnable} for creating the child region on one member
   */
  public void regionGetWithOfflineChild(SerializableRunnable createParentPR,
      SerializableRunnable createChildPR, boolean concurrentCheckData) throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    // Create the PRs on two members
    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);

    // Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");

    // Close the members
    closeCache(vm1);
    closeCache(vm0);

    SerializableRunnable checkDataOnParent = (new SerializableRunnable("checkDataOnParent") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(getPartitionedRegionName());

        for (int i = 0; i < NUM_BUCKETS; i++) {
          assertEquals("For key " + i, "a", region.get(i));
        }
      }
    });

    try {
      // Recreate the parent region. Try to make sure that
      // the member with the latest copy of the buckets
      // is the one that decides to throw away it's copy
      // by starting it last.
      AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
      AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
      async0.getResult(MAX_WAIT);
      async1.getResult(MAX_WAIT);
      // Now create the parent region on vm-2. vm-2 did not
      // previously host the child region.
      vm2.invoke(createParentPR);

      AsyncInvocation async2 = null;
      AsyncInvocation asyncCheck = null;
      if (concurrentCheckData) {
        // Recreate the child region.
        async1 = vm1.invokeAsync(createChildPR);
        async0 = vm0.invokeAsync(createChildPR);
        async2 = vm2.invokeAsync(new SerializableRunnable("delay") {
          @Override
          public void run() throws InterruptedException {
            Thread.sleep(100);
            vm2.invoke(createChildPR);
          }
        });

        asyncCheck = vm0.invokeAsync(checkDataOnParent);
      } else {
        vm0.invoke(checkDataOnParent);
      }
      async0.getResult(MAX_WAIT);
      async1.getResult(MAX_WAIT);
      async2.getResult(MAX_WAIT);
      asyncCheck.getResult(MAX_WAIT);
      // Validate the data
      checkData(vm0, 0, NUM_BUCKETS, "a");
      checkData(vm0, 0, NUM_BUCKETS, "a", "region2");
      // Make sure we can actually use the buckets in the child region.
      createData(vm0, 0, NUM_BUCKETS, "c", "region2");
    } finally {
      // Close the members
      closeCache(vm1);
      closeCache(vm0);
      closeCache(vm2);
    }
  }

  /**
   * Create a colocated pair of persistent regions and populate them with data. Shut down the
   * servers and then restart them.
   * <p>
   * On the restart, try region operations ({@code put()}) on the parent region before or during
   * persistent recovery. The {@code concurrentCreatekData} argument determines whether the
   * operation from the parent region occurs before or concurrent with the child region creation and
   * recovery.
   *
   * @param createParentPR {@link SerializableRunnable} for creating the parent region on one member
   * @param createChildPR {@link SerializableRunnable} for creating the child region on one member
   */
  public void regionPutWithOfflineChild(SerializableRunnable createParentPR,
      SerializableRunnable createChildPR, boolean concurrentCreateData) throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable checkDataOnParent = (new SerializableRunnable("checkDataOnParent") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(getPartitionedRegionName());

        for (int i = 0; i < NUM_BUCKETS; i++) {
          assertEquals("For key " + i, "a", region.get(i));
        }
      }
    });

    SerializableRunnable createDataOnParent = new SerializableRunnable("createDataOnParent") {

      public void run() {
        Cache cache = getCache();
        LogWriterUtils.getLogWriter().info("creating data in " + getPartitionedRegionName());
        Region region = cache.getRegion(getPartitionedRegionName());

        for (int i = 0; i < NUM_BUCKETS; i++) {
          region.put(i, "c");
          assertEquals("For key " + i, "c", region.get(i));
        }
      }
    };

    // Create the PRs on two members
    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);

    // Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");

    // Close the members
    closeCache(vm1);
    closeCache(vm0);

    try {
      // Recreate the parent region. Try to make sure that
      // the member with the latest copy of the buckets
      // is the one that decides to throw away it's copy
      // by starting it last.
      AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
      AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
      async0.getResult(MAX_WAIT);
      async1.getResult(MAX_WAIT);
      // Now create the parent region on vm-2. vm-2 did not
      // previous host the child region.
      vm2.invoke(createParentPR);

      AsyncInvocation async2 = null;
      AsyncInvocation asyncPut = null;
      if (concurrentCreateData) {
        // Recreate the child region.
        async1 = vm1.invokeAsync(createChildPR);
        async0 = vm0.invokeAsync(createChildPR);
        async2 = vm2.invokeAsync(createChildPR);

        Thread.sleep(100);
        asyncPut = vm0.invokeAsync(createDataOnParent);
      } else {
        vm0.invoke(createDataOnParent);
      }
      async0.getResult(MAX_WAIT);
      async1.getResult(MAX_WAIT);
      async2.getResult(MAX_WAIT);
      asyncPut.getResult(MAX_WAIT);
      // Validate the data
      checkData(vm0, 0, NUM_BUCKETS, "c");
      checkData(vm0, 0, NUM_BUCKETS, "a", "region2");
      // Make sure we can actually use the buckets in the child region.
      createData(vm0, 0, NUM_BUCKETS, "c", "region2");
    } finally {
      // Close the members
      closeCache(vm1);
      closeCache(vm0);
      closeCache(vm2);
    }
  }

  private RebalanceResults rebalance(VM vm) {
    return (RebalanceResults) vm.invoke(new SerializableCallable() {

      public Object call() throws Exception {
        RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();
        return op.getResults();
      }
    });
  }

  private static class PRObserver extends PartitionedRegionObserverAdapter {
    private CountDownLatch rebalanceDone = new CountDownLatch(1);
    private CountDownLatch bucketCreateStarted = new CountDownLatch(3);

    @Override
    public void beforeBucketCreation(PartitionedRegion region, int bucketId) {
      if (region.getName().contains("region2")) {
        bucketCreateStarted.countDown();
        waitForRebalance();
      }
    }



    private void waitForRebalance() {
      try {
        if (!rebalanceDone.await(MAX_WAIT, TimeUnit.SECONDS)) {
          fail("Failed waiting for the rebalance to start");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void waitForCreate() throws InterruptedException {
      if (!bucketCreateStarted.await(MAX_WAIT, TimeUnit.SECONDS)) {
        fail("Failed waiting for bucket creation to start");
      }
    }

    public void release() {
      rebalanceDone.countDown();
    }
  }

}
