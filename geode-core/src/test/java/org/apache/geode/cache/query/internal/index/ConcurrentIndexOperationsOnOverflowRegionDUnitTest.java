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
/**
 *
 */
package org.apache.geode.cache.query.internal.index;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

import java.util.Collection;
import java.util.Set;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;

/**
 * Test creates a persistent-overflow region and performs updates in region which has compact
 * indexes and queries the indexes simultaneously.
 *
 * This test's main purpose is to look for deadlock problems caused by NOT taking early locks on
 * compact indexes and remove the requirement of the system property
 * gemfire.index.acquireCompactIndexLocksWithRegionEntryLocks.
 *
 *
 */
@Category(DistributedTest.class)
public class ConcurrentIndexOperationsOnOverflowRegionDUnitTest extends JUnit4CacheTestCase {

  String name;

  public static volatile boolean hooked = false;

  @Category(FlakyTest.class) // GEODE-1828
  @Test
  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnRR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    hooked = false;
    name = "PartionedPortfoliosPR";
    // Create Overflow Persistent Partition Region
    vm0.invoke(
        new CacheSerializableRunnable("Create local region with synchronous index maintenance") {
          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            Region partitionRegion = null;
            IndexManager.testHook = null;
            try {
              DiskStore ds = cache.findDiskStore("disk");
              if (ds == null) {
                ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
              }
              AttributesFactory attr = new AttributesFactory();
              attr.setValueConstraint(PortfolioData.class);
              attr.setIndexMaintenanceSynchronous(true);
              EvictionAttributesImpl evicAttr =
                  new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
              evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
              attr.setEvictionAttributes(evicAttr);
              attr.setDataPolicy(DataPolicy.REPLICATE);
              // attr.setPartitionAttributes(new
              // PartitionAttributesFactory().setTotalNumBuckets(1).create());
              attr.setDiskStoreName("disk");
              RegionFactory regionFactory = cache.createRegionFactory(attr.create());
              partitionRegion = regionFactory.create(name);
            } catch (IllegalStateException ex) {
              LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
            }
            assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
            assertNotNull("Region ref null", partitionRegion);
            assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
            // Create Indexes
            try {
              Index index =
                  cache.getQueryService().createIndex("statusIndex", "p.ID", "/" + name + " p");
              assertNotNull(index);
            } catch (Exception e1) {
              e1.printStackTrace();
              fail("Index creation failed");
            }
          }
        });

    // Start changing the value in Region which should turn into a deadlock if the fix is not there
    AsyncInvocation asyncInv1 =
        vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();

            // Do a put in region.
            Region r = getCache().getRegion(name);

            for (int i = 0; i < 100; i++) {
              r.put(i, new PortfolioData(i));
            }

            assertNull(IndexManager.testHook);
            IndexManager.testHook = new IndexManagerTestHook();

            // Destroy one of the values.
            getCache().getLogger().fine("Destroying the value");
            r.destroy(1);

            IndexManager.testHook = null;
          }
        });

    AsyncInvocation asyncInv2 =
        vm0.invokeAsync(new CacheSerializableRunnable("Run query on region") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();

            Query statusQuery = getCache().getQueryService()
                .newQuery("select * from /" + name + " p where p.ID > -1");

            while (!hooked) {
              Wait.pause(100);
            }
            try {
              getCache().getLogger().fine("Querying the region");
              SelectResults results = (SelectResults) statusQuery.execute();
              assertEquals(100, results.size());
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        });

    // If we take more than 30 seconds then its a deadlock.
    ThreadUtils.join(asyncInv2, 30 * 1000);
    ThreadUtils.join(asyncInv1, 30 * 1000);
  }

  /**
   *
   */
  @Test
  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    hooked = false;
    name = "PartionedPortfoliosPR";
    // Create Overflow Persistent Partition Region
    vm0.invoke(
        new CacheSerializableRunnable("Create local region with synchronous index maintenance") {
          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            Region partitionRegion = null;
            IndexManager.testHook = null;
            try {
              DiskStore ds = cache.findDiskStore("disk");
              if (ds == null) {
                ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
              }
              AttributesFactory attr = new AttributesFactory();
              attr.setValueConstraint(PortfolioData.class);
              attr.setIndexMaintenanceSynchronous(true);
              EvictionAttributesImpl evicAttr =
                  new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
              evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
              attr.setEvictionAttributes(evicAttr);
              attr.setDataPolicy(DataPolicy.PARTITION);
              attr.setPartitionAttributes(
                  new PartitionAttributesFactory().setTotalNumBuckets(1).create());
              attr.setDiskStoreName("disk");
              RegionFactory regionFactory = cache.createRegionFactory(attr.create());
              partitionRegion = regionFactory.create(name);
            } catch (IllegalStateException ex) {
              LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
            }
            assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
            assertNotNull("Region ref null", partitionRegion);
            assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
            // Create Indexes
            try {
              Index index =
                  cache.getQueryService().createIndex("statusIndex", "p.ID", "/" + name + " p");
              assertNotNull(index);
            } catch (Exception e1) {
              e1.printStackTrace();
              fail("Index creation failed");
            }
          }
        });

    // Start changing the value in Region which should turn into a deadlock if the fix is not there
    AsyncInvocation asyncInv1 =
        vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();

            // Do a put in region.
            Region r = getCache().getRegion(name);

            for (int i = 0; i < 100; i++) {
              r.put(i, new PortfolioData(i));
            }

            assertNull(IndexManager.testHook);
            IndexManager.testHook = new IndexManagerTestHook();

            // Destroy one of the values.
            getCache().getLogger().fine("Destroying the value");
            r.destroy(1);

            IndexManager.testHook = null;
          }
        });

    AsyncInvocation asyncInv2 =
        vm0.invokeAsync(new CacheSerializableRunnable("Run query on region") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();

            Query statusQuery = getCache().getQueryService()
                .newQuery("select * from /" + name + " p where p.ID > -1");

            while (!hooked) {
              Wait.pause(100);
            }
            try {
              getCache().getLogger().fine("Querying the region");
              SelectResults results = (SelectResults) statusQuery.execute();
              assertEquals(100, results.size());
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        });

    // If we take more than 30 seconds then its a deadlock.
    ThreadUtils.join(asyncInv2, 30 * 1000);
    ThreadUtils.join(asyncInv1, 30 * 1000);
  }

  /**
  *
  */
  @Test
  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnPersistentRR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    hooked = false;
    name = "PartionedPortfoliosPR";
    // Create Overflow Persistent Partition Region
    vm0.invoke(
        new CacheSerializableRunnable("Create local region with synchronous index maintenance") {
          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            Region partitionRegion = null;
            IndexManager.testHook = null;
            try {
              DiskStore ds = cache.findDiskStore("disk");
              if (ds == null) {
                ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
              }
              AttributesFactory attr = new AttributesFactory();
              attr.setValueConstraint(PortfolioData.class);
              attr.setIndexMaintenanceSynchronous(true);
              EvictionAttributesImpl evicAttr =
                  new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
              evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
              attr.setEvictionAttributes(evicAttr);
              attr.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
              // attr.setPartitionAttributes(new
              // PartitionAttributesFactory().setTotalNumBuckets(1).create());
              attr.setDiskStoreName("disk");
              RegionFactory regionFactory = cache.createRegionFactory(attr.create());
              partitionRegion = regionFactory.create(name);
            } catch (IllegalStateException ex) {
              LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
            }
            assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
            assertNotNull("Region ref null", partitionRegion);
            assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
            // Create Indexes
            try {
              Index index =
                  cache.getQueryService().createIndex("statusIndex", "p.ID", "/" + name + " p");
              assertNotNull(index);
            } catch (Exception e1) {
              e1.printStackTrace();
              fail("Index creation failed");
            }
          }
        });

    // Start changing the value in Region which should turn into a deadlock if
    // the fix is not there
    AsyncInvocation asyncInv1 =
        vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

          @Override
          public void run2() throws CacheException {
            // Do a put in region.
            Region r = getCache().getRegion(name);

            for (int i = 0; i < 100; i++) {
              r.put(i, new PortfolioData(i));
            }

            assertNull(IndexManager.testHook);
            IndexManager.testHook = new IndexManagerTestHook();

            // Destroy one of the values.
            getCache().getLogger().fine("Destroying the value");
            r.destroy(1);

            IndexManager.testHook = null;
          }
        });

    AsyncInvocation asyncInv2 =
        vm0.invokeAsync(new CacheSerializableRunnable("Run query on region") {

          @Override
          public void run2() throws CacheException {
            Query statusQuery = getCache().getQueryService()
                .newQuery("select * from /" + name + " p where p.ID > -1");

            while (!hooked) {
              Wait.pause(100);
            }
            try {
              getCache().getLogger().fine("Querying the region");
              SelectResults results = (SelectResults) statusQuery.execute();
              assertEquals(100, results.size());
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        });

    // If we take more than 30 seconds then its a deadlock.
    ThreadUtils.join(asyncInv2, 30 * 1000);
    ThreadUtils.join(asyncInv1, 30 * 1000);
  }

  /**
  *
  */
  @Test
  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnPersistentPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    hooked = false;
    name = "PartionedPortfoliosPR";
    // Create Overflow Persistent Partition Region
    vm0.invoke(
        new CacheSerializableRunnable("Create local region with synchronous index maintenance") {
          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            Region partitionRegion = null;
            IndexManager.testHook = null;
            try {
              DiskStore ds = cache.findDiskStore("disk");
              if (ds == null) {
                ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
              }
              AttributesFactory attr = new AttributesFactory();
              attr.setValueConstraint(PortfolioData.class);
              attr.setIndexMaintenanceSynchronous(true);
              EvictionAttributesImpl evicAttr =
                  new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
              evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
              attr.setEvictionAttributes(evicAttr);
              attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
              attr.setPartitionAttributes(
                  new PartitionAttributesFactory().setTotalNumBuckets(1).create());
              attr.setDiskStoreName("disk");
              RegionFactory regionFactory = cache.createRegionFactory(attr.create());
              partitionRegion = regionFactory.create(name);
            } catch (IllegalStateException ex) {
              LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
            }
            assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
            assertNotNull("Region ref null", partitionRegion);
            assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
            // Create Indexes
            try {
              Index index =
                  cache.getQueryService().createIndex("statusIndex", "p.ID", "/" + name + " p");
              assertNotNull(index);
            } catch (Exception e1) {
              e1.printStackTrace();
              fail("Index creation failed");
            }
          }
        });

    // Start changing the value in Region which should turn into a deadlock if
    // the fix is not there
    AsyncInvocation asyncInv1 =
        vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

          @Override
          public void run2() throws CacheException {
            // Do a put in region.
            Region r = getCache().getRegion(name);

            for (int i = 0; i < 100; i++) {
              r.put(i, new PortfolioData(i));
            }

            assertNull(IndexManager.testHook);
            IndexManager.testHook = new IndexManagerTestHook();

            // Destroy one of the values.
            getCache().getLogger().fine("Destroying the value");
            r.destroy(1);

            IndexManager.testHook = null;
          }
        });

    AsyncInvocation asyncInv2 =
        vm0.invokeAsync(new CacheSerializableRunnable("Run query on region") {

          @Override
          public void run2() throws CacheException {
            Query statusQuery = getCache().getQueryService()
                .newQuery("select * from /" + name + " p where p.ID > -1");

            while (!hooked) {
              Wait.pause(100);
            }
            try {
              getCache().getLogger().fine("Querying the region");
              SelectResults results = (SelectResults) statusQuery.execute();
              assertEquals(100, results.size());
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        });

    // If we take more than 30 seconds then its a deadlock.
    ThreadUtils.join(asyncInv2, 30 * 1000);
    ThreadUtils.join(asyncInv1, 30 * 1000);
  }

  /**
  *
  */
  @Test
  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnNonOverflowRR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    hooked = false;
    name = "PartionedPortfoliosPR";
    // Create Overflow Persistent Partition Region
    vm0.invoke(
        new CacheSerializableRunnable("Create local region with synchronous index maintenance") {
          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            Region partitionRegion = null;
            IndexManager.testHook = null;
            try {
              AttributesFactory attr = new AttributesFactory();
              attr.setValueConstraint(PortfolioData.class);
              attr.setIndexMaintenanceSynchronous(true);
              attr.setDataPolicy(DataPolicy.REPLICATE);
              // attr.setPartitionAttributes(new
              // PartitionAttributesFactory().setTotalNumBuckets(1).create());
              RegionFactory regionFactory = cache.createRegionFactory(attr.create());
              partitionRegion = regionFactory.create(name);
            } catch (IllegalStateException ex) {
              LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
            }
            assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
            assertNotNull("Region ref null", partitionRegion);
            assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
            // Create Indexes
            try {
              Index index =
                  cache.getQueryService().createIndex("statusIndex", "p.ID", "/" + name + " p");
              assertNotNull(index);
            } catch (Exception e1) {
              e1.printStackTrace();
              fail("Index creation failed");
            }
          }
        });

    // Start changing the value in Region which should turn into a deadlock if the fix is not there
    AsyncInvocation asyncInv1 =
        vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();

            // Do a put in region.
            Region r = getCache().getRegion(name);

            for (int i = 0; i < 100; i++) {
              r.put(i, new PortfolioData(i));
            }

            assertNull(IndexManager.testHook);
            IndexManager.testHook = new IndexManagerNoWaitTestHook();

            // Destroy one of the values.
            getCache().getLogger().fine("Destroying the value");
            r.destroy(1);

            IndexManager.testHook = null;
          }
        });

    AsyncInvocation asyncInv2 =
        vm0.invokeAsync(new CacheSerializableRunnable("Run query on region") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();

            Query statusQuery = getCache().getQueryService()
                .newQuery("select * from /" + name + " p where p.ID > -1");

            while (!hooked) {
              Wait.pause(10);
            }
            try {
              getCache().getLogger().fine("Querying the region");
              SelectResults results = (SelectResults) statusQuery.execute();
              assertEquals(100, results.size());
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        });

    // If we take more than 30 seconds then its a deadlock.
    ThreadUtils.join(asyncInv2, 30 * 1000);
    ThreadUtils.join(asyncInv1, 30 * 1000);
  }

  /**
  *
  */
  @Test
  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnOnNonOverflowPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    hooked = false;
    name = "PartionedPortfoliosPR";
    // Create Overflow Persistent Partition Region
    vm0.invoke(
        new CacheSerializableRunnable("Create local region with synchronous index maintenance") {
          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            Region partitionRegion = null;
            IndexManager.testHook = null;
            try {
              AttributesFactory attr = new AttributesFactory();
              attr.setValueConstraint(PortfolioData.class);
              attr.setIndexMaintenanceSynchronous(true);
              attr.setDataPolicy(DataPolicy.PARTITION);
              attr.setPartitionAttributes(
                  new PartitionAttributesFactory().setTotalNumBuckets(1).create());
              RegionFactory regionFactory = cache.createRegionFactory(attr.create());
              partitionRegion = regionFactory.create(name);
            } catch (IllegalStateException ex) {
              LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
            }
            assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
            assertNotNull("Region ref null", partitionRegion);
            assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
            // Create Indexes
            try {
              Index index =
                  cache.getQueryService().createIndex("statusIndex", "p.ID", "/" + name + " p");
              assertNotNull(index);
            } catch (Exception e1) {
              e1.printStackTrace();
              fail("Index creation failed");
            }
          }
        });

    // Start changing the value in Region which should turn into a deadlock if the fix is not there
    AsyncInvocation asyncInv1 =
        vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

          @Override
          public void run2() throws CacheException {
            // Do a put in region.
            Region r = getCache().getRegion(name);

            for (int i = 0; i < 100; i++) {
              r.put(i, new PortfolioData(i));
            }

            assertNull(IndexManager.testHook);
            IndexManager.testHook = new IndexManagerNoWaitTestHook();

            // Destroy one of the values.
            getCache().getLogger().fine("Destroying the value");
            r.destroy(1);

            IndexManager.testHook = null;
          }
        });

    AsyncInvocation asyncInv2 =
        vm0.invokeAsync(new CacheSerializableRunnable("Run query on region") {

          @Override
          public void run2() throws CacheException {
            Query statusQuery = getCache().getQueryService()
                .newQuery("select * from /" + name + " p where p.ID > -1");

            while (!hooked) {
              Wait.pause(10);
            }
            try {
              getCache().getLogger().fine("Querying the region");
              SelectResults results = (SelectResults) statusQuery.execute();
              assertEquals(100, results.size());
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        });

    // If we take more than 30 seconds then its a deadlock.
    ThreadUtils.join(asyncInv2, 30 * 1000);
    ThreadUtils.join(asyncInv1, 30 * 1000);
  }

  public class IndexManagerTestHook
      implements org.apache.geode.cache.query.internal.index.IndexManager.TestHook {
    public void hook(final int spot) throws RuntimeException {
      switch (spot) {
        case 5: // Before Index update and after region entry lock.
          hooked = true;
          LogWriterUtils.getLogWriter().fine("IndexManagerTestHook is hooked.");
          Wait.pause(10000);
          // hooked = false;
          break;
        default:
          break;
      }
    }
  }
  public class IndexManagerNoWaitTestHook
      implements org.apache.geode.cache.query.internal.index.IndexManager.TestHook {
    public void hook(final int spot) throws RuntimeException {
      switch (spot) {
        case 5: // Before Index update and after region entry lock.
          hooked = true;
          LogWriterUtils.getLogWriter().fine("IndexManagerTestHook is hooked.");
          Wait.pause(100);
          // hooked = false;
          break;
        default:
          break;
      }
    }
  }
}

