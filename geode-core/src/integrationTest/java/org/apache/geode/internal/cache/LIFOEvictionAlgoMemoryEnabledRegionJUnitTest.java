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
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.cache.DistributedRegion.DiskPosition;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.eviction.EvictionList;
import org.apache.geode.internal.cache.eviction.EvictionNode;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * This is a test verifies region is LIFO enabled by MEMORY verifies correct stats updating and
 * faultin is not evicting another entry - not strict LIFO
 *
 * @since GemFire 5.7
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LIFOEvictionAlgoMemoryEnabledRegionJUnitTest {

  /** The cache instance */
  private static Cache cache = null;

  /** Stores LIFO Related Statistics */
  private static EvictionCounters lifoStats = null;

  /** The distributedSystem instance */
  private static DistributedSystem distributedSystem = null;

  private static final String regionName = "LIFOMemoryEvictionEnabledRegion";

  private static final int maximumMegabytes = 1;

  private static final int byteArraySize = 20480;

  private static long memEntryCountForFirstPutOperation;

  private final int deltaSize = 20738;

  private static EvictionList lifoClockHand = null;


  @Before
  public void setUp() throws Exception {
    initializeVM();
  }

  @After
  public void tearDown() throws Exception {
    assertNotNull(cache);
    Region rgn = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(rgn);
    rgn.localDestroyRegion();
    cache.close();

  }

  /**
   * Method for intializing the VM and create region with LIFO attached
   */
  private static void initializeVM() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "info"); // to keep diskPerf logs smaller
    distributedSystem = DistributedSystem.connect(props);
    cache = CacheFactory.create(distributedSystem);
    assertNotNull(cache);
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    File dir = new File("testingDirectoryDefault");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = {dir};
    dsf.setDiskDirsAndSizes(dirs, new int[] {Integer.MAX_VALUE});
    dsf.setAutoCompact(false);
    ((DiskStoreFactoryImpl) dsf).setDiskDirSizesUnit(DiskDirSizesUnit.BYTES);

    factory.setDiskStoreName(dsf.create(regionName).getName());
    factory.setDiskSynchronous(true);
    factory.setDataPolicy(DataPolicy.NORMAL);

    /* setting LIFO MEMORY related eviction attributes */

    factory.setEvictionAttributes(EvictionAttributes.createLIFOMemoryAttributes(maximumMegabytes,
        EvictionAction.OVERFLOW_TO_DISK));
    RegionAttributes attr = factory.create();

    ((GemFireCacheImpl) cache).createRegion(regionName, attr);
    lifoClockHand =
        ((VMLRURegionMap) ((LocalRegion) cache.getRegion(SEPARATOR + regionName)).entries)
            .getEvictionList();

    /* storing stats reference */
    lifoStats = lifoClockHand.getStatistics();

  }

  /**
   * This test does the following :<br>
   * 1)Perform put operation <br>
   * 2)Verify count of faultin entries <br>
   */
  @Test
  public void test000EntryFaultinCount() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion) cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);

      DiskRegionStats diskRegionStats = rgn.getDiskRegion().getStats();
      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats.getCounter())));

      // put 60 entries into the region
      for (long i = 0L; i < 60L; i++) {
        rgn.put("key" + i, newDummyObject(i));
      }

      assertEquals(
          "LRU eviction entry count and entries overflown to disk count from diskstats is not equal ",
          lifoStats.getEvictions(), diskRegionStats.getNumOverflowOnDisk());
      assertNull("Entry value in VM is not null", rgn.getValueInVM("key59"));
      // used to get number of entries required to reach the limit of memory assign
      memEntryCountForFirstPutOperation = diskRegionStats.getNumEntriesInVM();
      rgn.get("key59");
      assertEquals("Not equal to number of entries present in VM : ", 51L,
          diskRegionStats.getNumEntriesInVM());
      assertEquals("Not equal to number of entries present on disk : ", 9L,
          diskRegionStats.getNumOverflowOnDisk());

    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }
  }

  /**
   * This test does the following :<br>
   * 1)Varify region is LIFO Enabled <br>
   * 2)Perform put operation <br>
   * 3)perform get operation <br>
   * 4)Varify value retrived <br>
   * 5)Verify count (entries present in memory) after put operations <br>
   * 6)Verify count (entries present in memory) after get (performs faultin) operation <br>
   * 7)Verify count (entries present in memory) after remove operation <br>
   */
  @Test
  public void test001LIFOStatsUpdation() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion) cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);

      // check for is LIFO Enable
      assertTrue("Eviction Algorithm is not LIFO",
          (((EvictionAttributesImpl) rgn.getAttributes().getEvictionAttributes()).isLIFO()));

      // put 60 entries into the region
      for (long i = 0L; i < 60L; i++) {
        rgn.put(new Long(i), newDummyObject(i));
      }

      // verifies evicted entry values are null in memory
      assertTrue("In memory ", rgn.entries.getEntry(new Long(51)).isValueNull());
      assertTrue("In memory ", rgn.entries.getEntry(new Long(52)).isValueNull());
      assertTrue("In memory ", rgn.entries.getEntry(new Long(53)).isValueNull());

      // get an entry back
      rgn.get(new Long(46));
      rgn.get(new Long(51));
      rgn.get(new Long(56));
      // gets stuck in while loop
      rgn.put(new Long(60), newDummyObject(60));
      rgn.put(new Long(61), newDummyObject(61));
      assertNull("Entry value in VM is not null", rgn.getValueInVM(new Long(58)));
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }

  }

  /**
   * This test does the following :<br>
   * 1)Perform put operation <br>
   * 2)Verify entry evicted is LIFO Entry and is not present in vm<br>
   */
  @Test
  public void test002LIFOEntryEviction() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion) cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);

      assertEquals("Region is not properly cleared ", 0, rgn.size());
      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats.getCounter())));
      // put sixty entries into the region
      for (long i = 0L; i < 60L; i++) {
        rgn.put(new Long(i), newDummyObject(i));
        if (i < memEntryCountForFirstPutOperation) {
          // entries are in memory
          assertNotNull("Entry is not in VM ", rgn.getValueInVM(new Long(i)));
        } else {
          /*
           * assertTrue("LIFO Entry is not evicted", lifoClockHand.getLRUEntry() .testEvicted());
           */
          assertTrue("Entry is not null ", rgn.entries.getEntry(new Long(i)).isValueNull());
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }
  }

  /**
   * This test does the following :<br>
   * 1)Perform put operation <br>
   * 2)Verify count of evicted entries <br>
   */
  @Test
  public void test003EntryEvictionCount() {
    try {
      assertNotNull(cache);
      Region rgn = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);

      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats.getCounter())));
      // put 60 entries into the region
      for (long i = 0L; i < 60L; i++) {
        rgn.put(new Long(i), newDummyObject(i));
      }

      assertTrue("1)Total eviction count is not correct ",
          new Long(10).equals(new Long(lifoStats.getEvictions())));
      rgn.put(new Long(60), newDummyObject(60));
      rgn.get(new Long(55));
      assertTrue("2)Total eviction count is not correct ",
          new Long(11).equals(new Long(lifoStats.getEvictions())));
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }
  }

  // Basic checks to validate lifo queue implementation works as expected
  @Test
  public void testLIFOQueue() {
    try {
      assertNotNull(cache);
      Region rgn = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);
      // insert data
      lifoClockHand.appendEntry(new TestLRUNode(1));
      lifoClockHand.appendEntry(new TestLRUNode(2));
      lifoClockHand.appendEntry(new TestLRUNode(3));
      assertTrue(lifoClockHand.size() == 3);
      // make sure data is removed in LIFO fashion
      TestLRUNode tailValue = (TestLRUNode) lifoClockHand.getEvictableEntry();
      assertTrue("Value = " + tailValue.getValue(), tailValue.value == 3);
      assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 2);
      tailValue = (TestLRUNode) lifoClockHand.getEvictableEntry();
      assertTrue("Value = " + tailValue.getValue(), tailValue.value == 2);
      assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 1);
      tailValue = (TestLRUNode) lifoClockHand.getEvictableEntry();
      assertTrue("Value = " + tailValue.getValue(), tailValue.value == 1);
      assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 0);
      tailValue = (TestLRUNode) lifoClockHand.getEvictableEntry();
      assertTrue("No Value - null", tailValue == null);
      assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 0);
      // check that entries not available or already evicted are skipped and removed
      TestLRUNode testlrunode = new TestLRUNode(1);
      lifoClockHand.appendEntry(testlrunode);
      testlrunode = new TestLRUNode(2);
      testlrunode.setEvicted();
      lifoClockHand.appendEntry(testlrunode);
      testlrunode = new TestLRUNode(3);
      testlrunode.setEvicted();
      lifoClockHand.appendEntry(testlrunode);
      tailValue = (TestLRUNode) lifoClockHand.getEvictableEntry();
      assertTrue("Value = " + tailValue.getValue(), tailValue.value == 1);
      assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 0);
      tailValue = (TestLRUNode) lifoClockHand.getEvictableEntry();
      assertTrue("No Value - null", tailValue == null);
      assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 0);
      // TODO : need tests for data still part of transaction
    } catch (Exception ex) {
      ex.printStackTrace();
      fail(ex.getMessage());
    }
  }

  // purpose to create object ,size of byteArraySize
  private Object newDummyObject(long i) {
    byte[] value = new byte[byteArraySize];
    Arrays.fill(value, (byte) i);
    return value;
  }

  // test class for validating LIFO queue
  static class TestLRUNode implements EvictableEntry {

    EvictionNode next = null;
    EvictionNode prev = null;
    boolean evicted = false;
    boolean recentlyUsed = false;
    int value = 0;

    public TestLRUNode(int value) {
      this.value = value;
    }

    @Override
    public Token getValueAsToken() {
      return null;
    }

    @Override
    public void setValueWithTombstoneCheck(final Object value, final EntryEvent event)
        throws RegionClearedException {

    }

    @Override
    public Object getTransformedValue() {
      return null;
    }

    @Override
    public Object getValueInVM(final RegionEntryContext context) {
      return null;
    }

    @Override
    public Object getValueOnDisk(final InternalRegion region) throws EntryNotFoundException {
      return null;
    }

    @Override
    public Object getValueOnDiskOrBuffer(final InternalRegion region)
        throws EntryNotFoundException {
      return null;
    }

    @Override
    public boolean initialImagePut(final InternalRegion region, final long lastModified,
        final Object newValue, final boolean wasRecovered, final boolean acceptedVersionTag)
        throws RegionClearedException {
      return false;
    }

    @Override
    public boolean initialImageInit(final InternalRegion region, final long lastModified,
        final Object newValue, final boolean create, final boolean wasRecovered,
        final boolean acceptedVersionTag) throws RegionClearedException {
      return false;
    }

    @Override
    public boolean destroy(final InternalRegion region, final EntryEventImpl event,
        final boolean inTokenMode, final boolean cacheWrite, final Object expectedOldValue,
        final boolean forceDestroy, final boolean removeRecoveredEntry) throws CacheWriterException,
        EntryNotFoundException, TimeoutException, RegionClearedException {
      return false;
    }

    @Override
    public boolean getValueWasResultOfSearch() {
      return false;
    }

    @Override
    public void setValueResultOfSearch(final boolean value) {

    }

    @Override
    public Object getSerializedValueOnDisk(final InternalRegion region) {
      return null;
    }

    @Override
    public Object getValueInVMOrDiskWithoutFaultIn(final InternalRegion region) {
      return null;
    }

    @Override
    public Object getValueOffHeapOrDiskWithoutFaultIn(final InternalRegion region) {
      return null;
    }

    @Override
    public boolean isUpdateInProgress() {
      return false;
    }

    @Override
    public void setUpdateInProgress(final boolean underUpdate) {

    }

    @Override
    public boolean isCacheListenerInvocationInProgress() {
      return false;
    }

    @Override
    public void setCacheListenerInvocationInProgress(final boolean isListenerInvoked) {

    }

    @Override
    public boolean isValueNull() {
      return false;
    }

    @Override
    public boolean isInvalid() {
      return false;
    }

    @Override
    public boolean isDestroyed() {
      return false;
    }

    @Override
    public boolean isDestroyedOrRemoved() {
      return false;
    }

    @Override
    public boolean isDestroyedOrRemovedButNotTombstone() {
      return false;
    }

    @Override
    public boolean isInvalidOrRemoved() {
      return false;
    }

    @Override
    public void setValueToNull() {

    }

    @Override
    public void returnToPool() {

    }

    @Override
    public void setNext(EvictionNode next) {
      this.next = next;
    }

    @Override
    public void setPrevious(EvictionNode previous) {
      prev = previous;
    }

    @Override
    public EvictionNode next() {
      return next;
    }

    @Override
    public EvictionNode previous() {
      return prev;
    }

    @Override
    public int updateEntrySize(EvictionController ccHelper) {
      return 0;
    }

    @Override
    public int updateEntrySize(EvictionController ccHelper, Object value) {
      return 0;
    }

    @Override
    public int getEntrySize() {
      return 0;
    }

    @Override
    public boolean isRecentlyUsed() {
      return recentlyUsed;
    }

    @Override
    public void setRecentlyUsed(final RegionEntryContext context) {
      recentlyUsed = true;
      context.incRecentlyUsed();
    }

    @Override
    public long getLastModified() {
      return 0;
    }

    @Override
    public boolean hasStats() {
      return false;
    }

    @Override
    public long getLastAccessed() throws InternalStatisticsDisabledException {
      return 0;
    }

    @Override
    public long getHitCount() throws InternalStatisticsDisabledException {
      return 0;
    }

    @Override
    public long getMissCount() throws InternalStatisticsDisabledException {
      return 0;
    }

    @Override
    public void updateStatsForPut(final long lastModifiedTime, final long lastAccessedTime) {

    }

    @Override
    public VersionStamp getVersionStamp() {
      return null;
    }

    @Override
    public VersionTag generateVersionTag(final VersionSource member, final boolean withDelta,
        final InternalRegion region, final EntryEventImpl event) {
      return null;
    }

    @Override
    public boolean dispatchListenerEvents(final EntryEventImpl event) throws InterruptedException {
      return false;
    }

    @Override
    public void updateStatsForGet(final boolean hit, final long time) {

    }

    @Override
    public void txDidDestroy(final long currentTime) {

    }

    @Override
    public void resetCounts() throws InternalStatisticsDisabledException {

    }

    @Override
    public void makeTombstone(final InternalRegion region, final VersionTag version)
        throws RegionClearedException {

    }

    @Override
    public void removePhase1(final InternalRegion region, final boolean clear)
        throws RegionClearedException {

    }

    @Override
    public void removePhase2() {

    }

    @Override
    public boolean isRemoved() {
      return false;
    }

    @Override
    public boolean isRemovedPhase2() {
      return false;
    }

    @Override
    public boolean isTombstone() {
      return false;
    }

    @Override
    public boolean fillInValue(final InternalRegion region, final Entry entry,
        final ByteArrayDataInput in, final DistributionManager distributionManager,
        final KnownVersion version) {
      return false;
    }

    @Override
    public boolean isOverflowedToDisk(final InternalRegion region,
        final DiskPosition diskPosition) {
      return false;
    }

    @Override
    public Object getKey() {
      return null;
    }

    @Override
    public Object getValue(final RegionEntryContext context) {
      return null;
    }

    @Override
    public Object getValueRetain(final RegionEntryContext context) {
      return null;
    }

    @Override
    public void setValue(final RegionEntryContext context, final Object value)
        throws RegionClearedException {

    }

    @Override
    public void setValue(final RegionEntryContext context, final Object value,
        final EntryEventImpl event) throws RegionClearedException {

    }

    @Override
    public Object getValueRetain(final RegionEntryContext context, final boolean decompress) {
      return null;
    }

    @Override
    public Object getValue() {
      return null;
    }

    @Override
    public void unsetRecentlyUsed() {
      recentlyUsed = false;
    }

    @Override
    public void setEvicted() {
      evicted = true;
    }

    @Override
    public void unsetEvicted() {
      evicted = false;
    }

    @Override
    public boolean isEvicted() {
      return evicted;
    }

    @Override
    public boolean isInUseByTransaction() {
      return false;
    }

    @Override
    public void incRefCount() {

    }

    @Override
    public void decRefCount(final EvictionList lruList, final InternalRegion region) {

    }

    @Override
    public void resetRefCount(final EvictionList lruList) {

    }

    @Override
    public Object prepareValueForCache(final RegionEntryContext context, final Object value,
        final boolean isEntryUpdate) {
      return null;
    }

    @Override
    public Object prepareValueForCache(final RegionEntryContext context, final Object value,
        final EntryEventImpl event, final boolean isEntryUpdate) {
      return null;
    }

    @Override
    public Object getKeyForSizing() {
      return null;
    }

    @Override
    public void setDelayedDiskId(final DiskRecoveryStore diskRecoveryStore) {

    }
  }
}
