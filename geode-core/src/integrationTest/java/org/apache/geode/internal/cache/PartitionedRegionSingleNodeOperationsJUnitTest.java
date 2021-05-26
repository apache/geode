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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache30.RegionTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Test for Partitioned Region operations on a single node. Following tests are included:
 * <P>
 * (1) testPut() - Tests the put() functionality for the partitioned region.
 * </P>
 *
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionedRegionSingleNodeOperationsJUnitTest {
  private static final Logger logger = LogService.getLogger();

  static DistributedSystem sys = null;

  static int var = 1;

  final String val = "string_value";

  LogWriter logWriter = null;

  @Before
  public void setUp() throws Exception {
    logWriter = PartitionedRegionTestHelper.getLogger();
  }

  @After
  public void tearDown() {
    PartitionedRegionTestHelper.closeCache();
  }

  /**
   * This isa test for PartitionedRegion put() operation.
   * <p>
   * Following functionalities are tested:
   * </p>
   * <p>
   * 1) put() on PR with localMaxMemory = 0. PartitionedRegionException expected.
   * </p>
   * <p>
   * 2)test the put() operation and validate that old values are returned in case of PR with scope
   * D_ACK.
   * </p>
   *
   *
   */
  @Test
  public void test000Put() throws Exception {
    String regionname = "testPut";
    int localMaxMemory = 0;
    System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, "20000");
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion(regionname, String.valueOf(localMaxMemory), 0);
    System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY,
        Integer.toString(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
    final String expectedExceptions = PartitionedRegionStorageException.class.getName();
    logger.info("<ExpectedException action=add>" + expectedExceptions + "</ExpectedException>");
    try {
      pr.put(new Integer(1), val);
      fail(
          "testPut()- The expected PartitionedRegionException was not thrown for localMaxMemory = 0");
    } catch (PartitionedRegionStorageException ex) {
      if (logWriter.fineEnabled()) {
        logWriter.fine(
            "testPut() - Got a correct exception-PartitionedRegionStorageException for localMaxMemory=0 ");
      }
    }
    logger.info("<ExpectedException action=remove>" + expectedExceptions + "</ExpectedException>");

    if (!pr.isDestroyed()) {
      pr.destroyRegion();
    }

    pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion(regionname,
        String.valueOf(400), 0);
    final int maxEntries = 3;
    for (int num = 0; num < maxEntries; num++) {
      final Integer key = new Integer(num);
      final Object oldVal = pr.put(key, this.val);
      // Assert a more generic return value here because the bucket has not been allocated yet
      // thus do not know if the value is local or not
      assertTrue(oldVal == null);
      assertEquals(this.val, pr.get(key));

      final Region.Entry entry = pr.getEntry(key);
      assertNotNull(entry);
      assertEquals(this.val, entry.getValue());
      assertTrue(pr.values().contains(this.val));
      if (RegionTestCase.entryIsLocal(entry)) {
        assertEquals("Failed for key " + num, this.val, pr.put(key, key));
      } else {
        assertEquals("Failed for key " + num, null, pr.put(key, key));
      }
      assertEquals((num + 1) * 2, ((GemFireCacheImpl) pr.getCache()).getCachePerfStats().getPuts());
    }

    if (!pr.isDestroyed()) {
      pr.destroyRegion();
    }

    pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion(regionname,
        String.valueOf(400), 0);

    for (int num = 0; num < maxEntries; num++) {
      pr.put(new Integer(num), this.val);
      Object retval = pr.get(new Integer(num));
      assertEquals(this.val, retval);
    }

    for (int num = 0; num < maxEntries; num++) {
      if (RegionTestCase.entryIsLocal(pr.getEntry(new Integer(num)))) {
        assertEquals(this.val, pr.put(new Integer(num), this.val));
      } else {
        assertEquals(null, pr.put(new Integer(num), this.val));
      }
    }

    final Object dummyVal = "DummyVal";
    for (int num = 0; num < maxEntries; num++) {
      final Object getObj = pr.get(new Integer(num));
      final Object oldPut = pr.put(new Integer(num), dummyVal);
      if (((EntrySnapshot) pr.getEntry(new Integer(num))).wasInitiallyLocal()) {
        assertEquals("Returned value from put operation is not same as the old value", getObj,
            oldPut);

      } else {
        assertEquals(null, oldPut);
      }
      assertEquals("testPut()- error in putting the value in the Partitioned Region", dummyVal,
          pr.get(new Integer(num)));
    }
  }

  /**
   * This is a test for PartitionedRegion destroy(key) operation.
   *
   */
  @Test
  public void test001Destroy() throws Exception {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testDestroy", String.valueOf(200), 0);

    final String expectedExceptions = EntryNotFoundException.class.getName();
    final String addExpected =
        "<ExpectedException action=add>" + expectedExceptions + "</ExpectedException>";
    final String removeExpected =
        "<ExpectedException action=remove>" + expectedExceptions + "</ExpectedException>";
    logger.info(addExpected);
    for (int num = 0; num < 3; num++) {
      try {
        pr.destroy(new Integer(num));
        fail(
            "Destroy doesn't throw EntryNotFoundException for the entry which never existed in the system");
      } catch (EntryNotFoundException expected) {
      }
    }
    logger.info(removeExpected);

    for (int num = 0; num < 3; num++) {
      pr.put(new Integer(num), val);
      final long initialDestroyCount = getDestroyCount(pr);
      pr.destroy(new Integer(num));
      assertEquals(initialDestroyCount + 1, getDestroyCount(pr));
    }

    for (int num = 0; num < 3; num++) {
      Object retval = pr.get(new Integer(num));
      if (retval != null) {
        fail("testDestroy()- entry not destroyed  properly in destroy(key)");
      }
    }

    logger.info(addExpected);
    for (int num = 0; num < 3; num++) {
      try {
        pr.destroy(new Integer(num));
        fail(
            "Destroy doesn't throw EntryNotFoundException for the entry which is already deleted from the system");
      } catch (EntryNotFoundException enf) {
      }
    }
    logger.info(removeExpected);

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testDestroy() Completed successfully ... ");
    }
  }

  private long getDestroyCount(PartitionedRegion pr) {
    return ((GemFireCacheImpl) pr.getCache()).getCachePerfStats().getDestroys();
  }

  private long getCreateCount(PartitionedRegion pr) {
    return ((GemFireCacheImpl) pr.getCache()).getCachePerfStats().getCreates();
  }

  /**
   * This is a test for PartitionedRegion get(key) operation.
   *
   */
  @Test
  public void test002Get() throws Exception {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testGet", String.valueOf(200), 0);
    for (int num = 0; num < 3; num++) {
      pr.put(new Integer(num), val);
      Object retval = pr.get(new Integer(num));
      if (!val.equals(String.valueOf(retval))) {
        fail("testGet() - get operation failed for Partitioned Region ");
      }
    }
    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testGet() Completed successfully ... ");
    }
  }

  /**
   * This is a test for PartitionedRegion destroyRegion() operation.
   */

  @Test
  public void test003DestroyRegion() throws Exception {
    String regionName = "testDestroyRegion";
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion(regionName, String.valueOf(200), 0);
    pr.put(new Integer(1), new Integer(1));
    pr.get(new Integer(1));

    if (pr.isDestroyed()) {
      fail(
          "PartitionedRegionSingleNodeOperationsJUnitTest:testDestroyRegion(): Returns true in isDestroyed method, before the region is destroyed");
    }

    logWriter.info("JDEBUG 1");
    pr.destroyRegion();
    logWriter.info("JDEBUG 2");

    // Validate that the meta-data and bucket regions are cleaned.
    assertTrue(pr.isDestroyed());
    // assertTrue(pr.getBucket2Node().isEmpty());

    Region root = PartitionedRegionHelper.getPRRoot(PartitionedRegionTestHelper.createCache());
    // assertNull(PartitionedRegionHelper.getPRConfigRegion(root,
    // PartitionedRegionTestHelper.createCache()).get(regionName));
    java.util.Iterator regItr = root.subregions(false).iterator();
    while (regItr.hasNext()) {
      Region rg = (Region) regItr.next();
      // System.out.println("Region = " + rg.getName());
      assertEquals(
          rg.getName().indexOf(PartitionedRegionHelper.BUCKET_REGION_PREFIX + pr.getPRId() + "_"),
          -1);
    }

    if (!pr.isDestroyed()) {
      fail("testDestroyRegion(): "
          + "Returns false in isDestroyed method, after the region is destroyed");
    }
    logWriter.info("JDEBUG 3");
    try {
      pr.put(new Integer(2), new Integer(2));
      fail("testdestroyRegion() Expected RegionDestroyedException not thrown");
    } catch (RegionDestroyedException ex) {
      if (logWriter.fineEnabled()) {
        logWriter.fine(
            "testDestroyRegion() got a corect RegionDestroyedException for put() after destroyRegion()");
      }
    }
    logWriter.info("JDEBUG 4");
    try {
      pr.get(new Integer(2));
      fail("testdestroyRegion() - Expected RegionDestroyedException not thrown");
    } catch (RegionDestroyedException ex) {
      if (logWriter.fineEnabled()) {
        logWriter.fine("PartitionedRegionSingleNodeOperationsJUnitTest - "
            + "testDestroyRegion() got a correct RegionDestroyedException for get() after destroyRegion()");
      }
    }
    logWriter.info("JDEBUG 5");

    try {
      pr.destroy(new Integer(1));
      fail("testdestroyRegion() Expected RegionDestroyedException not thrown");
    } catch (RegionDestroyedException ex) {
      if (logWriter.fineEnabled()) {
        logWriter.fine("PartitionedRegionSingleNodeOperationsJUnitTest - "
            + "testDestroyRegion() got a correct RegionDestroyedException for destroy() after destroyRegion()");
      }
    }
    logWriter.info("JDEBUG 6");

    pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion(regionName,
        String.valueOf(200), 0);

    if (logWriter.fineEnabled()) {
      logWriter.fine("PartitionedRegionSingleNodeOperationsJUnitTest - testDestroyRegion():"
          + " PartitionedRegion with same name as the destroyed region, can be created.");
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testDestroyRegion(): Completed Successfully ...");
    }
  }

  /**
   * Tests getCache() API of PR. This should return same value as the cache used.
   *
   */
  @Test
  public void test004GetCache() throws Exception {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testGetCache", String.valueOf(200), 0);

    GemFireCacheImpl ca = (GemFireCacheImpl) pr.getCache();
    if (!PartitionedRegionTestHelper.createCache().equals(ca)) {
      fail("testGetCache() - getCache method is not returning proper cache handle");
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testGetCache() Completed Successfully");
    }
  }

  /**
   * Tests getFullPath() API of PartitionedRegion.
   *
   */
  @Test
  public void test005GetFullPath() throws Exception {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testGetFullPath", String.valueOf(200), 0);
    String fullPath = pr.getFullPath();
    if (!(SEPARATOR + "testGetFullPath").equals(fullPath)) {
      fail("testGetFullPath() - getFullPath method is not returning proper fullPath");
    }
    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testGetFullPath() Completed Successfully");
    }
  }

  /**
   * Tests getParentRegion() API of PR. This should return null as PR doesnt have a parentRegion
   *
   */
  @Test
  public void test006GetParentRegion() throws Exception {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testGetParentRegion", String.valueOf(200), 0);
    Region parentRegion = pr.getParentRegion();
    if (parentRegion != null) {
      fail("getParentRegion method is not returning null as parent region for a PartitionedRegion");
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testGetParentRegion() Completed Successfully");
    }
  }

  /**
   * Tests getName() API of PR. Name returned should be same as the one used to create PR.
   *
   */
  @Test
  public void test007GetName() throws Exception {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testGetName", String.valueOf(200), 0);
    String name = pr.getName();
    if (!("testGetName").equals(name)) {
      fail("getName() method is not returning proper region name");
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testGetName() Completed Successfully");
    }
  }

  /**
   * This method validates containsKey() operations.
   */
  @Test
  public void test008ContainsKey() throws Exception {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testContainsKey", String.valueOf(200), 0);
    for (int num = 0; num < 3; num++) {
      pr.put(new Integer(num), val);
      boolean retval = pr.containsKey(new Integer(num));
      if (!retval) {
        fail("PartitionedRegionSingleNodeOperationTest:testContainsKey() operation failed");
      }
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testContainsKey() Completed successfully ... ");
    }
  }

  /**
   * This method validates containsKey() operations.
   */
  @Test
  public void test009ContainsValueForKey() throws Exception {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testContainsValueForKey", String.valueOf(200), 0);
    for (int num = 0; num < 3; num++) {
      pr.put(new Integer(num), val);
      boolean retval = pr.containsValueForKey(new Integer(num));
      if (!retval) {
        fail(
            "PartitionedRegionSingleNodeOperationTest:testContainsValueForKey()  operation failed");
      }
    }
    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testContainsValueForKey() Completed successfully ... ");
    }
  }

  /**
   * Tests close() API of PR. It should close the accessor and delete the data-store and related
   * meta-data information.
   *
   */
  @Test
  public void test010Close() throws Exception {
    logWriter.fine("Getting inside testClose method");
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testClose", String.valueOf(200), 0);
    pr.put("K1", "V1");
    pr.get("K1");
    pr.close();
    if (!pr.isClosed || !pr.isDestroyed()) {
      fail("testClose(): After close isClosed = " + pr.isClosed + " and isDestroyed = "
          + pr.isDestroyed());
    }
    try {
      pr.put("K2", "V2");
      fail("testClose(): put operation completed on a closed PartitionedRegion. ");
    } catch (RegionDestroyedException expected) {
    }
    // validating get operation

    try {
      pr.get("K1");
      fail("testClose(): get operation should not succeed on closed PartitionedRegion. ");
    } catch (RegionDestroyedException expected) {
      // if (logger.fineEnabled()) {
      // logger.fine("Got correct RegionDestroyedException after close() ");
      // }
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testClose() Completed successfully ... ");
    }
  }

  /**
   * Tests localDestroyRegion() API of PR. It should locallyDestroy the accessor, data store and
   * meta-data.
   *
   */
  @Test
  public void test011LocalDestroyRegion() throws Exception {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testClose", String.valueOf(200), 0);
    pr.put("K1", "V1");
    pr.get("K1");
    pr.localDestroyRegion();
    if (pr.isClosed || !pr.isDestroyed()) {
      fail("testClose(): After close isClosed = " + pr.isClosed + " and isDestroyed = "
          + pr.isDestroyed());
    }
    try {
      pr.put("K2", "V2");
      fail("testClose(): put operation completed on a closed PartitionedRegion. ");
    } catch (RegionDestroyedException expected) {
    }
    try {
      pr.get("K1");
      fail("testClose(): get operation should not succeed on closed PartitionedRegion. ");
    } catch (RegionDestroyedException expected) {
      if (logWriter.fineEnabled()) {
        logWriter.fine("Got correct RegionDestroyedException after close() ");
      }
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testClose() Completed successfully ... ");
    }
  }

  /**
   * This method is used to test the isDestroyed() functionality.
   *
   */
  @Test
  public void test012IsDestroyed() throws Exception {
    String regionName = "testIsDestroyed";
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion(regionName, String.valueOf(200), 0);
    pr.put(new Integer(1), new Integer(1));
    pr.get(new Integer(1));

    if (pr.isDestroyed()) {
      fail(
          "PartitionedRegionSingleNodeOperationsJUnitTest:testIsDestroyed(): Returns true in isDestroyed method, before the region is destroyed");
    }

    pr.destroyRegion();

    if (!pr.isDestroyed()) {
      fail("PartitionedRegionSingleNodeOperationsJUnitTest:testIsDestroyed(): "
          + "Returns false in isDestroyed method, after the region is destroyed");
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testIsDestroyed() Completed successfully ... ");
    }
  }

  /**
   * This method validates that PR closed behavior matches that of local region closed behavior.
   *
   */
  /*
   * public void xxNoTestValidateCloseFunction() throws Exception { String partitionRegionName =
   * "testValidateCloseFunction"; PartitionedRegion pr =
   * (PartitionedRegion)PartitionedRegionTestHelper .createPartitionedRegion(partitionRegionName,
   * String.valueOf(200), 2, Scope.DISTRIBUTED_ACK); String diRegion = "diRegion";
   *
   * Region distRegion = null;
   *
   * AttributesFactory af = new AttributesFactory(); RegionAttributes regionAttributes; // setting
   * property af.setScope(Scope.DISTRIBUTED_ACK); // creating region attributes regionAttributes =
   * af.create(); try { distRegion = PartitionedRegionTestHelper.createCache().createRegion(
   * diRegion, regionAttributes); } catch (RegionExistsException rex) { distRegion =
   * PartitionedRegionTestHelper.createCache() .getRegion(diRegion); } // Closing the regions
   * distRegion.close(); pr.close();
   *
   * if (!pr.getCache().equals(distRegion.getCache())) {
   * fail("testValidateCloseFunction: getCache is not matching. "); } else { if
   * (logger.fineEnabled()) { logger.fine("getCache() matched on closed PR and distributed region
   * "); } } if (distRegion.getName().length() != pr.getName().length()) {
   * fail("testValidateCloseFunction: getName behavior not matching. "); } else { if
   * (logger.fineEnabled()) { logger.fine("getName) matched on closed PR and distributed region ");
   * } }
   *
   * if (distRegion.getFullPath().length() != pr.getFullPath().length()) {
   * fail("testValidateCloseFunction: getFullPath behavior not matching. "); } else { if
   * (logger.fineEnabled()) { logger .fine("getFullPath) matched on closed PR and distributed region
   * "); } }
   *
   * if (logger.fineEnabled()) { logger .fine("PartitionedRegionSingleNodeOperationsJUnitTest -
   * testValidateCloseFunction() Completed successfully ... "); } }
   */

  // /**
  // * This method is used to test the entrySet method. It verifies that it throws
  // * UnsupportedOperationException.
  // *
  // */
  // public void xxNoTestEntrySet() throws Exception
  // {
  // String regionName = "testEntrySet";
  // PartitionedRegion pr = (PartitionedRegion)PartitionedRegionTestHelper
  // .createPartitionedRegion(regionName, String.valueOf(200), 0,
  // Scope.DISTRIBUTED_ACK);
  //
  // try {
  // Set s = pr.entrySet(true);
  // fail("testEntrySet() does NOT throw UnsupportedOperationException");
  // }
  // catch (UnsupportedOperationException onse) {
  // if (logger.fineEnabled()) {
  // logger
  // .fine("testEntrySet() is correctly throwing UnsupportedOperationException on an empty PR");
  // }
  // }
  //
  // pr.put(new Integer(1), new Integer(1));
  //
  // try {
  // Set s = pr.entrySet(true);
  // fail("testEntrySet() does NOT throw UnsupportedOperationException");
  // }
  // catch (UnsupportedOperationException onse) {
  // if (logger.fineEnabled()) {
  // logger
  // .fine("testEntrySet() is correctly throwing UnsupportedOperationException on a non-empty PR");
  // }
  // }
  //
  // if (logger.fineEnabled()) {
  // logger
  // .fine("PartitionedRegionSingleNodeOperationsJUnitTest - testEntrySet() Completed successfully
  // ... ");
  // }
  // }

  /**
   * Test either the keySet() or keys() methods, which logically are expected to be the same
   */
  private void keysSetTester(Region pr) {
    assertEquals(Collections.EMPTY_SET, pr.keySet());
    pr.put(new Integer(1), "won");
    pr.put(new Integer(2), "to");
    pr.put(new Integer(3), "free");
    pr.put(new Integer(5), "hive");
    final Set ks = pr.keySet();
    assertEquals(4, ks.size());

    try {
      ks.clear();
      fail("Expected key set to be read only");
    } catch (Exception expected) {
    }
    try {
      ks.add("foo");
      fail("Expected key set to be read only");
    } catch (Exception expected) {
    }
    try {
      ks.addAll(Arrays.asList(new String[] {"one", "two", "three"}));
      fail("Expected key set to be read only");
    } catch (Exception expected) {
    }
    try {
      ks.remove("boom");
      fail("Expected key set to be read only");
    } catch (Exception expected) {
    }
    try {
      ks.removeAll(Arrays.asList(new Integer[] {new Integer(1), new Integer(2)}));
      fail("Expected key set to be read only");
    } catch (Exception expected) {
    }
    try {
      ks.retainAll(Arrays.asList(new Integer[] {new Integer(3), new Integer(5)}));
      fail("Expected key set to be read only");
    } catch (Exception expected) {
    }

    final Iterator ksI = ks.iterator();
    for (int i = 0; i < 4; i++) {
      try {
        ksI.remove();
        fail("Expected key set iterator to be read only");
      } catch (Exception expected) {
      }
      assertTrue(ksI.hasNext());
      Object key = ksI.next();
      assertEquals(Integer.class, key.getClass());
    }
    try {
      ksI.remove();
      fail("Expected key set iterator to be read only");
    } catch (Exception expected) {
    }
    try {
      ksI.next();
      fail("Expected no such element exception");
    } catch (NoSuchElementException expected) {
    }
  }

  @Test
  public void test014KeySet() throws Exception {
    String regionName = "testKeysSet";
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion(regionName, String.valueOf(200), 0);
    keysSetTester(pr);
  }

  // /**
  // * This method is used to test the keySet method. It verifies that it throws
  // * UnsupportedOperationException.
  // *
  // */
  // public void xxNoTestKeySet() throws Exception
  // {
  // String regionName = "testKeySet";
  // PartitionedRegion pr = (PartitionedRegion)PartitionedRegionTestHelper
  // .createPartitionedRegion(regionName, String.valueOf(200), 0,
  // Scope.DISTRIBUTED_ACK);
  //
  // try {
  // Set s = pr.keySet();
  // fail("testKeySet() does NOT throw UnsupportedOperationException");
  // }
  // catch (UnsupportedOperationException onse) {
  // if (logger.fineEnabled()) {
  // logger
  // .fine("testKeySet() is correctly throwing UnsupportedOperationException on an empty PR");
  // }
  // }
  //
  // pr.put(new Integer(1), new Integer(1));
  //
  // try {
  // Set s = pr.keySet();
  // fail("testKeySet() does NOT throw UnsupportedOperationException");
  // }
  // catch (UnsupportedOperationException onse) {
  //
  // if (logger.fineEnabled()) {
  // logger
  // .fine("testKeySet() is correctly throwing UnsupportedOperationException on a non-empty PR");
  // }
  // }
  //
  // if (logger.fineEnabled()) {
  // logger
  // .fine("PartitionedRegionSingleNodeOperationsJUnitTest - testKeySet() Completed successfully ...
  // ");
  // }
  // }

  // /**
  // * This method is used to test the putAll method. It verifies that it throws
  // * UnsupportedOperationException.
  // *
  // */
  // public void xxNoTestPutAll() throws Exception
  // {
  // final String rName = "testPutAll";
  // PartitionedRegion pr = (PartitionedRegion)PartitionedRegionTestHelper
  // .createPartitionedRegion(rName, String.valueOf(8), 0,
  // Scope.DISTRIBUTED_ACK);
  // try {
  // assertNotNull(pr);
  // final String foo = "foo";
  // final String bing = "bing";
  // final String supper = "super";
  // HashMap data = new HashMap();
  // data.put(foo, "bar");
  // data.put(bing, "bam");
  // data.put(supper, "hero");
  //
  // assertNull(pr.get(foo));
  // assertNull(pr.get(bing));
  // assertNull(pr.get(supper));
  // try {
  // pr.putAll(data);
  // fail("testPutAll() does NOT throw UnsupportedOperationException");
  // }
  // catch (UnsupportedOperationException onse) {
  // if (logger.fineEnabled()) {
  // logger
  // .fine("testPutall() is correctly throwing UnsupportedOperationException on an empty PR");
  // }
  // }
  // assertNull(pr.get(foo));
  // assertNull(pr.get(bing));
  // assertNull(pr.get(supper));
  // }
  // finally {
  // pr.destroyRegion();
  // }
  // }

  // /**
  // * This method is used to test the values functionality. It verifies that it
  // * throws UnsupportedOperationException.
  // *
  // */
  // public void xxNoTestValues() throws Exception
  // {
  // String regionName = "testValues";
  // PartitionedRegion pr = (PartitionedRegion)PartitionedRegionTestHelper
  // .createPartitionedRegion(regionName, String.valueOf(200), 0,
  // Scope.DISTRIBUTED_ACK);
  // try {
  // Collection c = pr.values();
  // fail("testValues() does NOT throw UnsupportedOperationException");
  // }
  // catch (UnsupportedOperationException onse) {
  // if (logger.fineEnabled()) {
  // logger
  // .fine("testValues() correctly throwing UnsupportedOperationException on an empty PR ");
  // }
  // }
  //
  // pr.put(new Integer(1), new Integer(1));
  //
  // try {
  // Collection c = pr.values();
  // fail("testValues() does NOT throw UnsupportedOperationException");
  // }
  // catch (UnsupportedOperationException onse) {
  //
  // if (logger.fineEnabled()) {
  // logger
  // .fine("testValues() is correctly throwing UnsupportedOperationException on a non-empty PR ");
  // }
  // }
  // if (logger.fineEnabled()) {
  // logger
  // .fine("PartitionedRegionSingleNodeOperationsJUnitTest - testValues() Completed successfully ...
  // ");
  // }
  // }

  // /**
  // * This method validates containsKey operations. It verifies that it throws
  // * UnsupportedOperationException.
  // */
  // public void xxNoTestContainsValue() throws Exception
  // {
  // int key = 0;
  // Region pr = (PartitionedRegion)PartitionedRegionTestHelper
  // .createPartitionedRegion("testContainsValue", String.valueOf(200), 0,
  // Scope.DISTRIBUTED_ACK);
  // for (int num = 0; num < 3; num++) {
  // key++;
  // pr.put(new Integer(key), val);
  // try {
  // boolean retval = pr.containsValue(val);
  // fail("PartitionedRegionSingleNodeOperationTest:testContainsValue() operation failed");
  // }
  // catch (UnsupportedOperationException onse) {
  //
  // if (logger.fineEnabled()) {
  // logger
  // .fine(" testContainsValue is correctly throwing UnsupportedOperationException for value = "
  // + val);
  // }
  // }
  // }
  // if (logger.fineEnabled()) {
  // logger
  // .fine("PartitionedRegionSingleNodeOperationsJUnitTest - testContainsValue() Completed
  // successfully ... ");
  // }
  // }

  /**
   * This method is used to test the setUserAttributes functionality. It verifies that it sets the
   * UserAttributes on an open PR without throwing any exception. It also verifies that it throws
   * RegionDestroyedException on a closed PR.
   *
   */
  @Test
  public void test015SetUserAttributes() throws Exception {
    String regionName = "testSetUserAttributes";
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion(regionName, String.valueOf(200), 0);
    String s = "DUMMY";
    pr.setUserAttribute(s);

    // Close PR
    pr.close();

    try {
      pr.setUserAttribute(s);
      fail(
          "testSetUserAttributes() doesnt throw RegionDestroyedException on a closed PartitionedRegion.");
    } catch (RegionDestroyedException rde) {

      if (logWriter.fineEnabled()) {
        logWriter.fine(
            "testSetUserAttributes(): setUserAttributes() throws RegionDestroyedException on a closed PartitionedRegion. ");
      }
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testSetUserAttributes() Completed successfully ... ");
    }
  }

  /**
   * This method is used to test the getUserAttributes functionality. It verifies that it gets the
   * UserAttributes on an open PR without throwing any exception. It also verifies that it throws
   * RegionDestroyedException on a closed PR.
   *
   */
  @Test
  public void test016GetUserAttributes() throws Exception {
    String regionName = "testGetUserAttributes";
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion(regionName, String.valueOf(200), 0);

    String s = "DUMMY";
    pr.setUserAttribute(s);
    Object o = pr.getUserAttribute();
    if (!o.equals(s)) {
      fail("testGetUserAttributes() returns null on an open accessor");
    }

    // Close PR
    pr.close();

    assertEquals(s, pr.getUserAttribute());

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testGetUserAttributes() Completed successfully ... ");
    }
  }

  /**
   * This method is used to test the getAttributesMutator functionality. It verifies that it gets
   * the handle for attributes mutator on an open PR without throwing any exception. It also
   * verifies that it throws RegionDestroyedException on a closed PR.
   *
   */
  @Test
  public void test017GetAttributesMutator() throws Exception {
    String regionName = "testGetAttributesMutator";
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion(regionName, String.valueOf(200), 0);

    Object o = pr.getAttributesMutator();
    if (o == null) {
      fail("testGetAttributesMutator(): getAttributesMutator() returns null on an open accessor");
    }

    // Close PR
    pr.close();

    try {
      o = pr.getAttributesMutator();
      fail(
          "testGetAttributesMutator() does not throw RegionDestroyedException on a closed PartitionedRegion.");
    } catch (RegionDestroyedException rde) {
      if (logWriter.fineEnabled()) {
        logWriter.fine(
            "testGetAttributesMutator(): getAttributesMutator() throws RegionDestroyedException on a closed PartitionedRegion. ");
      }
    }
    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testGetAttributesMutator() Completed successfully ... ");
    }
  }

  /**
   * Test to check local-cache property. Creates PR with default local-cache setting. TODO Enable
   * this when we have local caching properly implemented -- mthomas 2/23/2006
   */
  //
  // public void xxNoTestLoalCache() throws Exception
  // {
  // String regionName = "testLocalCache";
  // PartitionedRegion pr = (PartitionedRegion)PartitionedRegionTestHelper
  // .createPartitionedRegion(regionName, String.valueOf(200), 2,
  // Scope.DISTRIBUTED_ACK, false);
  // assertFalse(pr.getAttributes().getPartitionAttributes()
  // .isLocalCacheEnabled());
  //
  // PartitionedRegion pr1 = (PartitionedRegion)PartitionedRegionTestHelper
  // .createPartitionedRegion(regionName + 2, String.valueOf(200), 2,
  // Scope.DISTRIBUTED_ACK, true);
  // assertTrue(pr1.getAttributes().getPartitionAttributes()
  // .isLocalCacheEnabled());
  //
  // pr.destroyRegion();
  // pr1.destroyRegion();
  //
  // }
  /**
   * Test to verify the scope of the Bucket Regions created. Scope is Local for redundantCopies 0.
   * Scope is DISTRIBUTED_ACK if redundantCopies > 0 and Region scope is DISTRIBUTED_ACK.Scope is
   * DISTRIBUTED_NO_ACK if redundantCopies > 0 and Region scope is DISTRIBUTED_NO_ACK
   */
  @Test
  public void test018BucketScope() throws Exception {
    String regionName = "testBucketScope";
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion(regionName, String.valueOf(200), 0);
    putSomeValues(pr);

    java.util.Iterator buckRegionIterator =
        pr.getDataStore().getLocalBucket2RegionMap().values().iterator();
    while (buckRegionIterator.hasNext()) {
      Region bucket = (Region) buckRegionIterator.next();
      // assertIndexDetailsEquals(Scope.LOCAL, bucket.getAttributes().getScope());
      // assertIndexDetailsEquals(DataPolicy.NORMAL, bucket.getAttributes().getDataPolicy());
      assertEquals(BucketRegion.class, bucket.getClass());
    }

    pr.destroyRegion();

    /*
     * PartitionedRegion pr1 = (PartitionedRegion)PartitionedRegionTestHelper
     * .createPartitionedRegion(regionName + 2, String.valueOf(200), 0, Scope.DISTRIBUTED_ACK);
     *
     * putSomeValues(pr1); java.util.Iterator buckRegionIterator1 =
     * pr1.getDataStore().localBucket2RegionMap .values().iterator(); while
     * (buckRegionIterator1.hasNext()) { Region bucket = (Region)buckRegionIterator1.next();
     * assertIndexDetailsEquals(Scope.DISTRIBUTED_ACK, bucket.getAttributes().getScope());
     * assertIndexDetailsEquals(MirrorType.KEYS_VALUES, bucket.getAttributes() .getMirrorType()); }
     *
     * pr1.destroyRegion();
     *
     * PartitionedRegion pr2 = (PartitionedRegion)PartitionedRegionTestHelper
     * .createPartitionedRegion(regionName + 2, String.valueOf(200), 0, Scope.DISTRIBUTED_NO_ACK);
     *
     * putSomeValues(pr2); java.util.Iterator buckRegionIterator2 =
     * pr2.getDataStore().localBucket2RegionMap .values().iterator(); while
     * (buckRegionIterator2.hasNext()) { Region bucket = (Region)buckRegionIterator2.next();
     * assertIndexDetailsEquals(Scope.DISTRIBUTED_NO_ACK, bucket.getAttributes().getScope());
     * assertIndexDetailsEquals(MirrorType.KEYS_VALUES, bucket.getAttributes() .getMirrorType()); }
     * pr2.destroyRegion();
     */
  }

  private void putSomeValues(Region r) {
    for (int i = 0; i < 10; i++) {
      r.put("" + i, "" + i);
    }
  }

  /**
   * This method validates create operations. It verfies that entries are created and if entry
   * already exists, it throws EntryExistsException.
   */
  @Test
  public void test019Create() throws EntryExistsException {
    int key = 0;
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testCreate", String.valueOf(200), 0);

    // testing if the create is working fine.
    // It checks that it throws EntryExistsException for an already existing key

    final String expectedExceptions = EntryExistsException.class.getName();
    for (int num = 0; num < 3; num++) {
      key++;
      final long initialCreates = getCreateCount(pr);
      pr.create(new Integer(key), val + num);
      assertEquals(initialCreates + 1, getCreateCount(pr));
      final Object getObj1 = pr.get(new Integer(key));

      if (!getObj1.equals(val + num)) {
        fail("Create could not create an entry");
      }
      pr.getCache().getLogger()
          .info("<ExpectedException action=add>" + expectedExceptions + "</ExpectedException>");
      try {
        pr.create(new Integer(key), val + num + "Added number");
        fail(
            "create doesnt throw EntryExistsException subsequent create on an already existing key");
      } catch (EntryExistsException eee) {
      }
      pr.getCache().getLogger()
          .info("<ExpectedException action=remove>" + expectedExceptions + "</ExpectedException>");

      final Object getObj2 = pr.get(new Integer(key));

      if (!getObj1.equals(getObj2)) {
        fail("Create could not create an entry");
      }
    }
    // I am going to null val check;

    for (int cnt = 0; cnt < 3; cnt++) {
      Object dummyObj = "DummyVal";
      Object nonNullKey = new Long(cnt);
      Object nullVal = null;
      Object nonNullVal = cnt + "";
      pr.create(nonNullKey, nullVal);
      dummyObj = pr.get(nonNullKey);

      if (dummyObj != null) {
        fail("Create can not create an entry with null value");
      }
      pr.getCache().getLogger()
          .info("<ExpectedException action=add>" + expectedExceptions + "</ExpectedException>");
      try {
        pr.create(nonNullKey, nonNullVal);
        fail("Create does not throw EntryExistsException with for an existing null value as well");
      } catch (EntryExistsException eee) {
      }
      pr.getCache().getLogger()
          .info("<ExpectedException action=remove>" + expectedExceptions + "</ExpectedException>");
    }
  }

  @Test
  public void test020CreateWriter() {
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testCreateWriter", String.valueOf(200), 0);
    MyCacheWriter writer = new MyCacheWriter();
    pr.getAttributesMutator().setCacheWriter(writer);
    writer.setExpectedKeyAndValue("key1", "value1");
    pr.create("key1", "value1");
    assertTrue(writer.isValidationSuccessful());

    writer.setExpectedKeyAndValue("key1", "value2");
    pr.put("key1", "value2");
    assertTrue(writer.isValidationSuccessful());

    writer.setExpectedKeyAndValue("key1", "value2");
    pr.destroy("key1");
    assertTrue(writer.isValidationSuccessful());
  }

  private class MyCacheWriter implements CacheWriter {
    private Object key;
    private Object value;
    private boolean validationSuccessful = false;

    public void setExpectedKeyAndValue(Object key, Object value) {
      this.key = key;
      this.value = value;
      this.validationSuccessful = false;
    }

    public boolean isValidationSuccessful() {
      return validationSuccessful;
    }

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {
      assertTrue(event.getOperation().isCreate());
      assertTrue(!event.getRegion().containsKey(this.key));
      assertTrue(!event.getRegion().containsValueForKey(this.key));
      assertNull(event.getRegion().getEntry(event.getKey()));
      this.validationSuccessful = true;
    }

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
      assertTrue(event.getOperation().isDestroy());
      assertTrue(event.getRegion().containsKey(this.key));
      assertTrue(event.getRegion().containsValueForKey(this.key));
      this.validationSuccessful = true;
    }

    @Override
    public void beforeRegionClear(RegionEvent event) throws CacheWriterException {}

    @Override
    public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {}

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
      assertTrue(event.getOperation().isUpdate());
      assertTrue(event.getRegion().containsKey(this.key));
      assertTrue(event.getRegion().containsValueForKey(this.key));
      assertNotNull(event.getRegion().getEntry(this.key));
      assertNotSame(this.value, event.getRegion().get(this.key));
      this.validationSuccessful = true;
    }

    @Override
    public void close() {}

  }

  /**
   * This method validates invalidate operations. It verfies that if entry exists, it is
   * invalidated. If entry does not exist, it throws EntryNotFoundException.
   */
  @Test
  public void test021Invalidate() throws Exception {
    int key = 0;
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testInvalidate", String.valueOf(200), 0);

    // testing if the create is working fine.
    // It checks that it throws EntryExistsException for an already existing key
    Object getObj1 = null;
    Object getObj2 = null;
    for (int num = 0; num < 3; num++) {
      key++;
      try {
        pr.put(new Integer(key), val + num);
      } catch (Exception e) {
        fail("testInvalidate(): put throws exception");
      }

      getObj1 = pr.get(new Integer(key));

      if (!getObj1.equals(val + num)) {
        fail("testInvalidate(): Could not put a correct entry");
      }

      pr.invalidate(new Integer(key));

      getObj2 = pr.get(new Integer(key));
      if (getObj2 != null) {
        fail("testInvalidate(): Invalidate could not invalidate an entry");
      }

      String prefix = "1";
      String dummyKey = prefix + key;
      try {
        pr.invalidate(new Integer(dummyKey));
        fail("testInvalidate(): Invalidate doesnt not throw EntryNotFoundException");
      } catch (EntryNotFoundException enf) {
        if (logWriter.fineEnabled()) {
          logWriter.fine("testInvalidate(): Invalidate throws EntryNotFoundException");
        }
      }

      pr.destroy(new Integer(key));

      try {
        pr.invalidate(new Integer(dummyKey));
        fail("testInvalidate(): Invalidate doesnt not throw EntryNotFoundException");
      } catch (EntryNotFoundException enf) {

        if (logWriter.fineEnabled()) {
          logWriter.fine("testInvalidate(): Invalidate throws EntryNotFoundException");
        }
      } catch (Exception e) {
        fail("testInvalidate(): Invalidate throws exception other than EntryNotFoundException");
      }
      assertEquals(num + 1,
          ((GemFireCacheImpl) pr.getCache()).getCachePerfStats().getInvalidates());

    }
    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testInvalidate() Completed successfully ... ");
    }
  }

  @Test
  public void test022GetEntry() throws Exception {
    Region pr = null;
    try {
      pr = PartitionedRegionTestHelper.createPartitionedRegion("testGetEntry", String.valueOf(200),
          0);
      final Integer one = new Integer(1);
      pr.put(one, "one");
      final Region.Entry re = pr.getEntry(one);
      assertFalse(re.isDestroyed());
      assertFalse(re.isLocal());
      assertTrue(((EntrySnapshot) re).wasInitiallyLocal());

      assertEquals("one", re.getValue());
      assertEquals(one, re.getKey());
      // TODO: Finish out the entry operations
      assertNull(pr.getEntry("nuthin"));
    } finally {
      if (pr != null) {
        pr.destroyRegion();
      }
    }
  }

  @Test
  public void test023UnsupportedOps() throws Exception {
    Region pr = null;
    try {
      pr = PartitionedRegionTestHelper.createPartitionedRegion("testUnsupportedOps",
          String.valueOf(200), 0);

      pr.put(new Integer(1), "one");
      pr.put(new Integer(2), "two");
      pr.put(new Integer(3), "three");
      pr.getEntry("key");

      try {
        pr.clear();
        fail(
            "PartitionedRegionSingleNodeOperationTest:testUnSupportedOps() operation failed on a blank PartitionedRegion");
      } catch (UnsupportedOperationException expected) {
      }

      // try {
      // pr.entries(true);
      // fail();
      // }
      // catch (UnsupportedOperationException expected) {
      // }

      // try {
      // pr.entrySet(true);
      // fail();
      // }
      // catch (UnsupportedOperationException expected) {
      // }

      try {
        HashMap data = new HashMap();
        data.put("foo", "bar");
        data.put("bing", "bam");
        data.put("supper", "hero");
        pr.putAll(data);
        // fail("testPutAll() does NOT throw UnsupportedOperationException");
      } catch (UnsupportedOperationException onse) {
      }


      // try {
      // pr.values();
      // fail("testValues() does NOT throw UnsupportedOperationException");
      // }
      // catch (UnsupportedOperationException expected) {
      // }


      try {
        pr.containsValue("foo");
      } catch (UnsupportedOperationException ex) {
        fail("PartitionedRegionSingleNodeOperationTest:testContainsValue() operation failed");
      }

    } finally {
      if (pr != null) {
        pr.destroyRegion();
      }
    }
  }

  /**
   * This method validates size operations. It verifies that it returns correct size of the
   * PartitionedRegion.
   */
  @Test
  public void test024Size() throws Exception {
    int key = 0;
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testSize", String.valueOf(200), 0);
    int size = 0;
    size = pr.size();

    assertEquals(size, 0);

    int count = 10;
    int removeCnt = 5;
    // Testing for a populated PR
    for (int num = 1; num <= count; num++) {
      key++;
      pr.put(new Integer(key), val);
      Object tmpVal = pr.get(new Integer(key));
      int tmpSize = pr.size();
      assertEquals(num, tmpSize);
    }
    size = pr.size();
    assertEquals(size, count);

    // Testing for a populated PR
    for (int num = 1; num <= removeCnt; num++) {
      pr.destroy(new Integer(num));
    }
    size = pr.size();
    assertEquals(size, (count - removeCnt));

    pr.close();

    try {
      pr.size();
    } catch (RegionDestroyedException rde) {
      if (logWriter.fineEnabled()) {
        logWriter.fine(
            "PartitionedRegionSingleNodeOperationsJUnitTest - testSize() returns RegionDestroyedException on a closed PartitionedRegion.");
      }
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testSize() Completed successfully ... ");
    }
  }

  /**
   * This method validates isEmpty operation. It verifies that it returns true if PartitionedRegion
   * is empty.
   */
  @Test
  public void test025IsEmpty() throws Exception {
    int key = 0;
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testIsEmpty", String.valueOf(200), 0);

    boolean isEmpty = pr.isEmpty();
    assertEquals(isEmpty, true);

    int count = 10;

    for (int num = 1; num <= count; num++) {
      key++;
      pr.put(new Integer(key), val);
      assertEquals(num, pr.size());
    }

    isEmpty = pr.isEmpty();
    assertEquals(isEmpty, false);

    for (int num = 1; num <= count; num++) {
      pr.destroy(new Integer(num));
    }

    isEmpty = pr.isEmpty();
    assertEquals(isEmpty, true);

    pr.close();

    try {
      pr.isEmpty();
    } catch (RegionDestroyedException rde) {
      if (logWriter.fineEnabled()) {
        logWriter.fine(
            "PartitionedRegionSingleNodeOperationsJUnitTest - testIsEmpty() returns RegionDestroyedException on a closed PartitionedRegion.");
      }
    }

    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testIsEmpty() Completed successfully ... ");
    }
  }

  @Test
  public void test026CloseDestroy() throws Exception {
    logWriter.fine("Getting inside testCloseDestroy");
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion("testCloseDestroy", String.valueOf(200), 0);
    pr.close();
    pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion("testCloseDestroy",
        String.valueOf(200), 0);
    try {
      pr.destroyRegion();
    } catch (RegionDestroyedException expected) {
      fail("testCloseDestroy(): Can not destroy a newly created region.");
    }
    pr = (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion("testCloseDestroy",
        String.valueOf(200), 0);
    if (logWriter.fineEnabled()) {
      logWriter.fine(
          "PartitionedRegionSingleNodeOperationsJUnitTest - testCloseDestroy() Completed successfully ... ");
    }
  }

}
