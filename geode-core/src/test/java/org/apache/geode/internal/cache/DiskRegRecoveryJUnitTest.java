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

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Disk region recovery tests
 * 
 * @since GemFire 5.1
 */
@Category(IntegrationTest.class)
public class DiskRegRecoveryJUnitTest extends DiskRegionTestingBase {

  private static int EMPTY_RVV_SIZE = 6;
  private static int ENTRY_SIZE = 1024;

  private static boolean oplogsIDsNotifiedToRoll;

  private boolean proceedWithRolling;
  private boolean rollingDone;
  private boolean verifiedOplogs;
  private final Object verifiedSync = new Object();

  private DiskRegionProperties diskProps = new DiskRegionProperties();

  @Override
  protected final void postSetUp() throws Exception {
    diskProps.setDiskDirs(dirs);
  }

  /**
   * Disk region recovery test for Persist only with sync writes. Test has four steps : STEP 1:
   * Create cache. Create Region. Put entries. Close cache. STEP 2: Create cache. Create Region with
   * the same name as that of in STEP 1. Put few more entries. Get and verify the entries put in
   * STEP 1 and STEP 2 STEP 3: Again Create cache. Create Region with the same name as that of in
   * STEP 1. Get and verify the entries put in STEP 1 and STEP 2. STEP 4: Create cache. Create
   * Region with the same name as that of in STEP 1. Get and verify the entries put in STEP 1 and
   * STEP 2.
   */
  @Test
  public void testDiskRegRecovery() {
    /**
     * STEP 1
     */
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("RecoveryTestRegion");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    verifyOplogSizeZeroAfterRecovery(region);
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte) 77);
    for (int i = 0; i < 100; i++) {
      region.put(new Integer(i), value);
    }
    // for recovery test:
    try {
      region.put("100", new byte[1024]);
      region.put("101", "101");
      region.put("102", new Character('a'));
      region.put("103", new Byte("103"));
      region.put("104", Boolean.TRUE);
      region.put("105", new Short("105"));
      region.put("106", new Integer(106));
      region.put("107", new Long(107L));
      region.put("108", new Float(108F));
      region.put("109", new Double(109d));
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("FAILED WHILE PUT:" + ex.toString());
    }
    // Verifying the get operation:
    getByteArrVal("100", region);
    assertEquals("101", region.get("101"));
    assertEquals(new Character('a'), region.get("102"));
    assertEquals(new Byte("103"), region.get("103"));
    assertEquals(Boolean.TRUE, region.get("104"));
    assertEquals(new Short("105"), region.get("105"));
    assertEquals(new Integer(106), region.get("106"));
    assertEquals(new Long(107L), region.get("107"));
    assertEquals(new Float(108F), region.get("108"));
    assertEquals(new Double(109d), region.get("109"));
    int origSize = region.size();
    /**
     * close the cache after that create it again and then put few more values
     */
    if (cache != null) {
      cache.close();
    }

    /**
     * STEP 2: create the cache and region with the same name
     */
    try {
      try {
        cache = createCache();
      } catch (Exception e) {
        fail(" failure in creation of cache due to " + e);
      }
      // Create region
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("RecoveryTestRegion");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      assertEquals(origSize, region.size());
      // Verifying the get operation:
      getByteArrVal("100", region);
      assertEquals("101", region.get("101"));
      assertEquals(new Character('a'), region.get("102"));
      assertEquals(new Byte("103"), region.get("103"));
      assertEquals(Boolean.TRUE, region.get("104"));
      assertEquals(new Short("105"), region.get("105"));
      assertEquals(new Integer(106), region.get("106"));
      assertEquals(new Long(107L), region.get("107"));
      assertEquals(new Float(108F), region.get("108"));
      assertEquals(new Double(109d), region.get("109"));
      // put few more entries
      region.put("110", new String("110"));
      region.put("111", new Character('b'));
      region.put("112", new Byte("112"));
      region.put("113", new Boolean(false));
      region.put("114", new Short("114"));
      region.put("115", new Integer(115));
      region.put("116", new Long(116L));
      region.put("117", new Float(117F));
      region.put("118", new Double(118d));
      region.put("119", new byte[0]);
    } catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("failed while (in STEP 2) creating the cache and/or region" + e.toString());
    }
    // Verifying the get operation:
    getByteArrVal("100", region);
    assertTrue(region.get("101").equals(new String("101")));
    assertTrue(region.get("102").equals(new Character('a')));
    assertTrue(region.get("103").equals(new Byte("103")));
    assertTrue(region.get("104").equals(new Boolean(true)));
    assertTrue(region.get("105").equals(new Short("105")));
    assertTrue(region.get("106").equals(new Integer(106)));
    assertTrue(region.get("107").equals(new Long(107L)));
    assertTrue(region.get("108").equals(new Float(108F)));
    assertTrue(region.get("109").equals(new Double(109d)));
    assertTrue(region.get("110").equals(new String("110")));
    assertTrue(region.get("111").equals(new Character('b')));
    assertTrue(region.get("112").equals(new Byte("112")));
    assertTrue(region.get("113").equals(new Boolean(false)));
    assertTrue(region.get("114").equals(new Short("114")));
    assertTrue(region.get("115").equals(new Integer(115)));
    assertTrue(region.get("116").equals(new Long(116L)));
    assertTrue(region.get("117").equals(new Float(117F)));
    assertTrue(region.get("118").equals(new Double(118d)));
    getByteArrValZeroLnth("119", region);
    /**
     * close the cache after that create it again
     */
    if (cache != null) {
      cache.close();
      if (logWriter.fineEnabled())
        logWriter.fine("Cache closed");
    }

    /**
     * STEP 3: Create the cache and region And close the cache
     */
    try {
      try {
        cache = createCache();
      } catch (Exception e) {
        fail(" failure in creation of cache due to " + e);
      }
      if (logWriter.fineEnabled())
        logWriter.fine("Cache created to test the recovery..");
      // Create region
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("RecoveryTestRegion");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed while (in STEP 3) creating the cache and/or region" + e.toString());
    }
    // Verifying the get operation:
    getByteArrVal("100", region);
    assertTrue(region.get("101").equals(new String("101")));
    assertTrue(region.get("102").equals(new Character('a')));
    assertTrue(region.get("103").equals(new Byte("103")));
    assertTrue(region.get("104").equals(new Boolean(true)));
    assertTrue(region.get("105").equals(new Short("105")));
    assertTrue(region.get("106").equals(new Integer(106)));
    assertTrue(region.get("107").equals(new Long(107L)));
    assertTrue(region.get("108").equals(new Float(108F)));
    assertTrue(region.get("109").equals(new Double(109d)));
    assertTrue(region.get("110").equals(new String("110")));
    assertTrue(region.get("111").equals(new Character('b')));
    assertTrue(region.get("112").equals(new Byte("112")));
    assertTrue(region.get("113").equals(new Boolean(false)));
    assertTrue(region.get("114").equals(new Short("114")));
    assertTrue(region.get("115").equals(new Integer(115)));
    assertTrue(region.get("116").equals(new Long(116L)));
    assertTrue(region.get("117").equals(new Float(117F)));
    assertTrue(region.get("118").equals(new Double(118d)));
    getByteArrValZeroLnth("119", region);
    /**
     * close the cache after that create it again
     */
    if (cache != null) {
      cache.close();
      if (logWriter.fineEnabled())
        logWriter.fine("Cache closed");
    }

    /**
     * STEP 4: Create the cache and region verify disk reg recovery And close the cache
     */
    try {
      try {
        cache = createCache();
      } catch (Exception e) {
        fail(" failure in creation of cache due to " + e);
      }
      if (logWriter.fineEnabled())
        logWriter.fine("Cache created to test the recovery..");
      // Create region
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("RecoveryTestRegion");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed while (in STEP 4) creating the cache and/or region" + e.toString());
    }
    // Get all the test values, put in STEP 1 & 2:
    // Verifying the get operation:
    getByteArrVal("100", region);
    assertTrue(region.get("101").equals(new String("101")));
    assertTrue(region.get("102").equals(new Character('a')));
    assertTrue(region.get("103").equals(new Byte("103")));
    assertTrue(region.get("104").equals(new Boolean(true)));
    assertTrue(region.get("105").equals(new Short("105")));
    assertTrue(region.get("106").equals(new Integer(106)));
    assertTrue(region.get("107").equals(new Long(107L)));
    assertTrue(region.get("108").equals(new Float(108F)));
    assertTrue(region.get("109").equals(new Double(109d)));
    assertTrue(region.get("110").equals(new String("110")));
    assertTrue(region.get("111").equals(new Character('b')));
    assertTrue(region.get("112").equals(new Byte("112")));
    assertTrue(region.get("113").equals(new Boolean(false)));
    assertTrue(region.get("114").equals(new Short("114")));
    assertTrue(region.get("115").equals(new Integer(115)));
    assertTrue(region.get("116").equals(new Long(116L)));
    assertTrue(region.get("117").equals(new Float(117F)));
    assertTrue(region.get("118").equals(new Double(118d)));
    getByteArrValZeroLnth("119", region);

    closeDown(); // closes disk file which will flush all buffers
  }

  /**
   * Disk region recovery test for Persist only with sync writes. Test has four steps : STEP 1:
   * Create cache. Create Region. Put entries. Close cache. STEP 2: Create cache. Create Region with
   * the same name as that of in STEP 1. Delete some entries. Close the Cache * 3: Again Create
   * cache. Create Region with the same name as that of in STEP 4) Verify that the entries got
   * deleted
   */
  @Test
  public void testBug39989_1() {
    /**
     * STEP 1
     */
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("RecoveryTestRegion");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    verifyOplogSizeZeroAfterRecovery(region);
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte) 77);
    for (int i = 0; i < 10; i++) {

      region.put(new Integer(i), value);
    }

    /**
     * close the cache after that create it again and then put few more values
     */
    if (cache != null) {
      cache.close();
    }

    /**
     * STEP 2: create the cache and region with the same name
     */
    try {
      try {
        cache = createCache();
      } catch (Exception e) {
        fail(" failure in creation of cache due to " + e);
      }
      // Create region
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("RecoveryTestRegion");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      // Remove the recovered entries
      for (int i = 0; i < 10; i++) {
        region.remove(new Integer(i));
      }


    } catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("failed while (in STEP 2) creating the cache and/or region" + e.toString());
    }
    /**
     * close the cache after that create it again
     */
    if (cache != null) {
      cache.close();
      if (logWriter.fineEnabled())
        logWriter.fine("Cache closed");
    }

    /**
     * STEP 3: Create the cache and region And close the cache
     */
    try {
      try {
        cache = createCache();
      } catch (Exception e) {
        fail(" failure in creation of cache due to " + e);
      }
      if (logWriter.fineEnabled())
        logWriter.fine("Cache created to test the recovery..");
      // Create region
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("RecoveryTestRegion");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      // Size should be zero
      assertEquals(0, region.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed while (in STEP 3) creating the cache and/or region" + e.toString());
    }

    closeDown(); // closes disk file which will flush all buffers
  }

  /**
   * Disk region recovery test for Persist only with sync writes. Test has four steps : STEP 1:
   * Create cache. Create Region. Put entries. Close cache. STEP 2: Create cache. Create Region with
   * the same name as that of in STEP 1. Delete some entries. 3) Recreate the deleted entries Close
   * the Cache * 3: Again Create cache. 4) check if the region creation is successful
   */
  @Test
  public void testBug39989_2() {
    /**
     * STEP 1
     */
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("RecoveryTestRegion");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    verifyOplogSizeZeroAfterRecovery(region);
    final byte[] value = new byte[ENTRY_SIZE];
    final byte[] value2 = new byte[ENTRY_SIZE + 1];
    Arrays.fill(value, (byte) 77);
    Arrays.fill(value2, (byte) 77);
    for (int i = 0; i < 10; i++) {
      region.put(new Integer(i), value);
    }

    /**
     * close the cache after that create it again and then put few more values
     */
    if (cache != null) {
      cache.close();
    }

    /**
     * STEP 2: create the cache and region with the same name
     */
    try {
      try {
        cache = createCache();
      } catch (Exception e) {
        fail(" failure in creation of cache due to " + e);
      }
      // Create region
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("RecoveryTestRegion");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      assertEquals(10, region.size());
      // Remove the recovered entries
      for (int i = 0; i < 10; i++) {
        region.remove(new Integer(i));
      }

      // add new entries
      for (int i = 0; i < 10; i++) {
        region.put(new Integer(i), value2);
      }


    } catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("failed while (in STEP 2) creating the cache and/or region" + e.toString());
    }
    /**
     * close the cache after that create it again
     */
    if (cache != null) {
      cache.close();
      if (logWriter.fineEnabled())
        logWriter.fine("Cache closed");
    }

    /**
     * STEP 3: Create the cache and region And close the cache
     */
    try {
      try {
        cache = createCache();
      } catch (Exception e) {
        fail(" failure in creation of cache due to " + e);
      }
      if (logWriter.fineEnabled())
        logWriter.fine("Cache created to test the recovery..");
      // Create region
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("RecoveryTestRegion");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      assertEquals(10, region.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed while (in STEP 3) creating the cache and/or region" + e.toString());
    }

    closeDown(); // closes disk file which will flush all buffers
  }

  /**
   * To validate the get operation performed on a byte array.
   */
  private void getByteArrVal(String key, Region region) {
    byte[] val = (byte[]) region.get(key);
    // verify that the retrieved byte[] equals to the value put initially.
    // val should be an unitialized array of bytes of length 1024
    assertEquals(1024, val.length);
    for (int i = 0; i < 1024; i++) {
      assertEquals(0, val[i]);
    }
  }

  /**
   * to validate the get operation performed on a byte array of length zero
   */
  private boolean getByteArrValZeroLnth(String key, Region region) {
    Object val0 = null;
    byte[] val2 = new byte[0];
    try {
      val0 = region.get(key);
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to get the value on disk");
    }
    // verify that the retrieved byte[] equals to the value put initially.
    boolean result = false;
    byte[] x = null;
    x = (byte[]) val0;
    // verify that the value of the entry is an instance of byte []
    assertTrue("the value of the entry having key 119 is NOT an" + " instance of byte []",
        val0 instanceof byte[]);

    result = x.length == val2.length;

    if (!result) {
      fail("The lenghth of byte[] put at 119th key obtained from disk "
          + "is not euqal to the lenght of byte[] put initially");
    }
    return result;
  }

  private void verifyOplogSizeZeroAfterRecovery(Region region) {
    assertEquals(
        Oplog.OPLOG_MAGIC_SEQ_REC_SIZE * 2 + Oplog.OPLOG_DISK_STORE_REC_SIZE * 2 + EMPTY_RVV_SIZE
            + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE * 2,
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getOplogSize());
  }

  @Test
  public void testNoEvictionDuringRecoveryIfNoGIIRecoverValuesTrue() {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    String oldLruValue = System.getProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    System.setProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME, "true");
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setPersistBackup(true);
      int overflowCapacity = 5;
      diskProps.setOverFlowCapacity(overflowCapacity);
      diskProps.setRegionName("RecoveryTestRegion");
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);

      int size = 1000;

      for (int i = 0; i < size; i++) {
        region.put(new Integer(i), new Integer(i));
      }
      region.close();
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);

      int serilizedValuesInVm = 0;
      for (int i = 0; i < size; i++) {
        try {
          Object value = ((LocalRegion) region).getValueInVM(new Integer(i));
          if (value instanceof CachedDeserializable) {
            serilizedValuesInVm++;
          }
        } catch (EntryNotFoundException e) {
          fail("Entry not found not expected but occurred ");
        }
      }
      // Test to see if values are in serialized form, when disk recovery is performed.
      if (serilizedValuesInVm != overflowCapacity) {
        // overflowCapacity - number of max items region will hold.
        fail("Values are not in default Serialized form, when it was loaded from disk.");
      }

      // verifyOplogSizeZeroAfterRecovery(region);
      for (int i = 0; i < size; i++) {
        Assert.assertTrue(region.get(new Integer(i)).equals(new Integer(i)));
      }
      // region.close();
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }

      if (oldLruValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME, oldLruValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testNoEvictionDuringRecoveryIfNoGIIRecoverValuesFalse() {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setPersistBackup(true);
      diskProps.setOverFlowCapacity(1);
      diskProps.setRegionName("RecoveryTestRegion");
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);

      for (int i = 0; i < 1000; i++) {
        region.put(new Integer(i), new Integer(i));
      }

      region.close();

      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);

      for (int i = 0; i < 1000; i++) {
        try {
          ((LocalRegion) region).getValueInVM(new Integer(i));
        } catch (EntryNotFoundException e) {
          fail("Entry not found not expected but occurred ");
        }
      }

      // verifyOplogSizeZeroAfterRecovery(region);
      for (int i = 0; i < 1000; i++) {
        try {
          Assert.assertTrue(((LocalRegion) region).getValueInVM(new Integer(i)) == null);
        } catch (EntryNotFoundException e) {
          fail("Entry not found not expected but occurred ");
        }
      }
      for (int i = 0; i < 1000; i++) {
        try {
          Assert.assertTrue(
              ((LocalRegion) region).getValueOnDisk(new Integer(i)).equals(new Integer(i)));
        } catch (EntryNotFoundException e) {
          fail("Entry not found not expected but occurred ");
        }
      }
      // region.close();
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testEmptyRegionRecover() {
    diskProps.setDiskDirs(dirs);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    Assert.assertTrue(region.size() == 0);
    verifyOplogSizeZeroAfterRecovery(region);
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    Assert.assertTrue(region.size() == 0);
    verifyOplogSizeZeroAfterRecovery(region);
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    Assert.assertTrue(region.size() == 0);
    verifyOplogSizeZeroAfterRecovery(region);
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    Assert.assertTrue(region.size() == 0);
    verifyOplogSizeZeroAfterRecovery(region);
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    Assert.assertTrue(region.size() == 0);
    verifyOplogSizeZeroAfterRecovery(region);
    // region.close();
  }

  @Test
  public void testReadCorruptedFile() {
    diskProps.setDiskDirs(dirs);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    byte[] value = new byte[1024];

    region.put("1", value);
    region.put("2", value);
    region.put("3", value);
    File oplogFile = null;
    try {
      oplogFile = ((LocalRegion) region).getDiskRegion().testHook_getChild().getOplogFileForTest();
    } catch (Exception e) {
      logWriter.error(
          "Exception in synching data present in the buffers of RandomAccessFile of Oplog, to the disk",
          e);
      fail("Test failed because synching of data present in buffer of RandomAccesFile ");
    }
    region.close();
    try {
      FileInputStream fis = new FileInputStream(oplogFile);
      DataInputStream dis = new DataInputStream(new BufferedInputStream(fis, 1024 * 1024));
      byte[] values = new byte[1000 * 3];
      dis.read(values);
      dis.close();
      fis.close();
      FileOutputStream fos = new FileOutputStream(oplogFile);
      fos.write(values);
      fos.close();

    } catch (FileNotFoundException e) {
      fail(" file expected to be there but not found");
    } catch (IOException e) {
      fail(" exception due to" + e);
    }

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    Assert.assertTrue(region.size() == 2, "Expected size to be 2 but it is " + region.size());
    // region.close();
  }

  @Test
  public void testForceCompactionForRegionWithRollingDisabled() throws Exception {
    diskProps.setDiskDirs(dirs);
    diskProps.setMaxOplogSize(2048 + (18 * 2) + 15 * 7);
    diskProps.setRolling(false);
    diskProps.setAllowForceCompaction(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    // region created

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

    // set cache observer to check oplogs rolling
    CacheObserver co = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      public void afterHavingCompacted() {
        synchronized (region) {
          if (!rollingDone) {
            rollingDone = true;
          } else {
            fail("rollingDone was set before actually rolling");
          }
        }
      }
    });

    byte[] value = new byte[900]; // two values per oplog
    region.put("0", value);
    region.put("1", value);
    region.put("2", value);
    region.put("3", value);
    region.put("4", value);
    region.put("5", value);
    region.put("6", value);
    region.put("7", value);
    region.put("8", value);
    region.put("9", value);

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    // Should have nothing to roll because each oplog still has
    // both entries in it as live.
    // start force rolling on the region
    // get array of oplog IDs being picked up for rolling
    oplogsIDsNotifiedToRoll = ((LocalRegion) region).getDiskStore().forceCompaction();
    assertEquals(false, oplogsIDsNotifiedToRoll);

    // now destroy all "odd" entries; This should allow each oplog to be compacted
    region.remove("1");
    region.remove("3");
    region.remove("5");
    region.remove("7");
    region.remove("9");
    oplogsIDsNotifiedToRoll = ((LocalRegion) region).getDiskStore().forceCompaction();
    assertEquals(true, oplogsIDsNotifiedToRoll);
    assertEquals(true, rollingDone);

    synchronized (region) {
      boolean condition = ((LocalRegion) region).getDiskRegion().getOplogToBeCompacted() == null;
      Assert.assertTrue(condition, "Oplogs still remain after having compacted");
    }
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  /**
   * This tests the case where potential of writing a dummy byte was tehre. The case would be
   * compactor terminating early. The create is present in both Htree as well as Oplog. When
   * recovery is done, the create is added to the Oplog createdEntrySet. Now while compactor
   * iterates over the entries in the region, it skips the created entry because its Htree Offset is
   * > -1. As a result it leaves it in created set & so when the compactor processes the created Set
   * it thinks that the entry is now referenced in the any of the subsequent oplogs & thus
   * overwrites it with a byte[].
   */
  @Test
  public void testVestigialCreatesInOplog() throws Exception {
    diskProps.setDiskDirs(dirs);
    diskProps.setMaxOplogSize(40);
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("VestigialCreatesTestRegion");
    diskProps.setRolling(true);

    // create sync persist region.
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    System.err.println(
        "<ExpectedException action=add>" + "KillCompactorException" + "</ExpectedException>");
    logWriter
        .info("<ExpectedException action=add>" + "KillCompactorException" + "</ExpectedException>");
    try {
      CacheObserver cob = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        public void beforeDeletingCompactedOplog(Oplog compactedOplog) {
          // killed compactor after rolling but before deleting oplogs
          throw new DiskStoreImpl.KillCompactorException();
        }
      });
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      region.put("key1", "Testing");
      region.put("key2", "Vestigial");
      region.put("key3", "Creates");
      region.put("key4", "Into");
      region.put("key5", "Oplog");

      // ((LocalRegion)region).getDiskStore().forceCompaction();

      if (cache != null) {
        cache.close();
      }
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
    } finally {
      System.err.println(
          "<ExpectedException action=remove>" + "KillCompactorException" + "</ExpectedException>");
      logWriter.info(
          "<ExpectedException action=remove>" + "KillCompactorException" + "</ExpectedException>");
    }

    cache = createCache();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertTrue("Expected region size is 5 but got it as " + region.entrySet().size(),
        region.entrySet().size() == 5);
    assertEquals(region.get("key1"), "Testing");
    Thread.sleep(5);
    if (cache != null) {
      cache.close();
    }
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter());
    cache = createCache();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertTrue("Expected region size is 5 but got it as " + region.entrySet().size(),
        region.entrySet().size() == 5);
    assertTrue(region.get("key1").equals("Testing"));
  }

  @Test
  public void testDiskIDFieldsForPersistOnlyRecoverValuesTrue() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setMaxOplogSize(1024);
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testDiskIDFieldsForPersistOnlyRecoverValuesTrue");
      diskProps.setRolling(false);
      // create sync persist region.
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      // the creates which will go in first oplog
      for (int i = 0; i < 3; ++i) {
        region.put("" + i, "" + i);
      }
      // Ensure that next few entries go in the second oplog while the first set
      // of entries go in the first oplog
      region.forceRolling();
      for (int i = 3; i < 6; ++i) {
        region.put("" + i, "" + i);
      }

      region.close();

      diskProps.setRolling(false);
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      // Now verify the DiskIds of each of the siz entries & the values.
      LocalRegion rgn = (LocalRegion) region;
      byte b = 0;
      b = EntryBits.setSerialized(b, true);
      b = EntryBits.setRecoveredFromDisk(b, true);
      b = EntryBits.setWithVersions(b, true);

      // TODO need to wait for async recovery
      for (int i = 0; i < 3; ++i) {
        DiskEntry de = (DiskEntry) rgn.basicGetEntry("" + i);
        DiskId did = de.getDiskId();
        assertTrue(did.getKeyId() > 0);
        assertEquals(1, did.getOplogId());
        assertTrue(did.getOffsetInOplog() > -1);
        assertEquals(b, did.getUserBits());
        assertTrue(did.getValueLength() > 0);
        assertEquals("" + i, rgn.get("" + i));
        assertTrue(did.getKeyId() > 0);
        assertFalse(de.isValueNull());
      }
      // this last oplog does not have a krf because this disk store has not
      // been closed. So its value should be in memory now.
      for (int i = 3; i < 6; ++i) {
        DiskEntry de = (DiskEntry) rgn.basicGetEntry("" + i);
        DiskId did = de.getDiskId();
        assertTrue(did.getKeyId() > 0);
        assertFalse(de.isValueNull());
        assertEquals(2, did.getOplogId());
        assertTrue(did.getOffsetInOplog() > -1);
        assertEquals(b, did.getUserBits());
        assertTrue(did.getValueLength() > 0);
        assertEquals("" + i, rgn.get("" + i));
      }

      if (cache != null) {
        cache.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testDiskIDFieldsForPersistOverFlowRecoverValuesTrue() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setMaxOplogSize(1024);
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testDiskIDFieldsForPersistOverFlowRecoverValuesTrue");
      diskProps.setOverFlowCapacity(2);
      diskProps.setRolling(false);
      // create sync persist region.
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);

      // the creates which will go in first oplog
      for (int i = 0; i < 3; ++i) {
        region.put("" + i, "" + i);
      }
      // Ensure that next few entries go in the second oplog while the first set
      // of entries go in the first oplog
      region.forceRolling();

      for (int i = 3; i < 6; ++i) {
        region.put("" + i, "" + i);
      }

      region.close();

      diskProps.setRolling(false);
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
      // Now verify the DiskIds of each of the siz entries & the values.
      LocalRegion rgn = (LocalRegion) region;
      byte b = 0;
      b = EntryBits.setSerialized(b, true);
      b = EntryBits.setRecoveredFromDisk(b, true);
      b = EntryBits.setWithVersions(b, true);

      for (int i = 0; i < 3; ++i) {
        DiskEntry de = (DiskEntry) rgn.basicGetEntry("" + i);
        DiskId did = de.getDiskId();
        assertEquals(1, did.getOplogId());
        assertTrue(did.getOffsetInOplog() > -1);
        assertEquals(b, did.getUserBits());
        assertTrue(did.getValueLength() > 0);
        assertEquals("" + i, rgn.get("" + i));
        assertTrue(did.getKeyId() > 0);
      }
      for (int i = 3; i < 6; ++i) {
        DiskEntry de = (DiskEntry) rgn.basicGetEntry("" + i);
        DiskId did = de.getDiskId();
        assertEquals(2, did.getOplogId());
        assertTrue(did.getOffsetInOplog() > -1);
        assertEquals(b, did.getUserBits());
        assertTrue(did.getValueLength() > 0);
        assertEquals("" + i, rgn.get("" + i));
        assertTrue(did.getKeyId() > 0);
      }

      // ((LocalRegion)region).getDiskStore().forceCompaction();

      if (cache != null) {
        cache.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testDiskIDFieldsForPersistOnlyRecoverValuesFalse() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setMaxOplogSize(1024);
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testDiskIDFieldsForPersistOnlyRecoverValuesFalse");
      diskProps.setRolling(false);
      // create sync persist region.
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      // the creates which will go in first oplog
      for (int i = 0; i < 3; ++i) {
        region.put("" + i, "" + i);
      }

      // Ensure that next few entries go in the second oplog while the first set
      // of entries go in the first oplog
      region.forceRolling();

      for (int i = 3; i < 6; ++i) {
        region.put("" + i, "" + i);
      }

      region.close();

      diskProps.setRolling(false);
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      // Now verify the DiskIds of each of the siz entries & the values.
      LocalRegion rgn = (LocalRegion) region;
      byte b = 0;
      b = EntryBits.setSerialized(b, true);
      b = EntryBits.setRecoveredFromDisk(b, true);
      b = EntryBits.setWithVersions(b, true);

      for (int i = 0; i < 3; ++i) {
        DiskEntry de = (DiskEntry) rgn.basicGetEntry("" + i);
        DiskId did = de.getDiskId();
        assertTrue(did.getKeyId() > 0);
        assertTrue(de.isValueNull());
        assertEquals(1, did.getOplogId());
        assertTrue(did.getOffsetInOplog() > -1);
        assertEquals(b, did.getUserBits());
        assertTrue(did.getValueLength() > 0);
        assertEquals("" + i, rgn.get("" + i));
        assertFalse(de.isValueNull());
        assertTrue(did.getKeyId() > 0);
      }
      for (int i = 3; i < 6; ++i) {
        DiskEntry de = (DiskEntry) rgn.basicGetEntry("" + i);
        DiskId did = de.getDiskId();
        assertTrue(de.isValueNull());
        assertTrue(did.getKeyId() > 0);
        assertEquals(2, did.getOplogId());
        assertTrue(did.getOffsetInOplog() > -1);
        assertEquals(b, did.getUserBits());
        assertTrue(did.getValueLength() > 0);
        assertEquals("" + i, rgn.get("" + i));
        assertFalse(de.isValueNull());
        assertTrue(did.getKeyId() > 0);
      }
      // ((LocalRegion)region).getDiskStore().forceCompaction();

      if (cache != null) {
        cache.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testDiskIDFieldsForPersistOverFlowRecoverValuesFalse() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setMaxOplogSize(1024);
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testDiskIDFieldsForPersistOverFlowRecoverValuesFalse");
      diskProps.setOverFlowCapacity(2);
      diskProps.setRolling(false);
      // create sync persist region.
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);

      // the creates which will go in first oplog
      for (int i = 0; i < 3; ++i) {
        region.put("" + i, "" + i);
      }
      // Ensure that next few entries go in the second oplog while the first set
      // of entries go in the first oplog
      region.forceRolling();

      for (int i = 3; i < 6; ++i) {
        region.put("" + i, "" + i);
      }

      region.close();

      diskProps.setRolling(false);
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
      // Now verify the DiskIds of each of the siz entries & the values.
      LocalRegion rgn = (LocalRegion) region;
      byte b = 0;
      b = EntryBits.setSerialized(b, true);
      b = EntryBits.setRecoveredFromDisk(b, true);
      b = EntryBits.setWithVersions(b, true);

      for (int i = 0; i < 3; ++i) {
        DiskEntry de = (DiskEntry) rgn.basicGetEntry("" + i);
        DiskId did = de.getDiskId();
        assertEquals(1, did.getOplogId());
        assertTrue(did.getOffsetInOplog() > -1);
        assertEquals(b, did.getUserBits());
        assertTrue(did.getValueLength() > 0);
        assertEquals("" + i, rgn.get("" + i));
        assertTrue(did.getKeyId() > 0);
      }
      for (int i = 3; i < 6; ++i) {
        DiskEntry de = (DiskEntry) rgn.basicGetEntry("" + i);
        DiskId did = de.getDiskId();
        assertEquals(2, did.getOplogId());
        assertTrue(did.getOffsetInOplog() > -1);
        assertEquals(b, did.getUserBits());
        assertTrue(did.getValueLength() > 0);
        assertEquals("" + i, rgn.get("" + i));
        assertTrue(did.getKeyId() > 0);
      }

      // ((LocalRegion)region).getDiskStore().forceCompaction();

      if (cache != null) {
        cache.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }

  }

  @Test
  public void testBug40375() throws Exception {
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setPersistBackup(true);
      diskProps.setSynchronous(true);
      diskProps.setRolling(true);
      System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME,
          "true");
      diskProps.setRegionName("testBug");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      assertEquals(0, region.size());
      region.put("1", "1");
      region.put("2", "2");
      region.forceRolling();
      // Thread.sleep(4000); why did we sleep for 4 seconds?
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      assertEquals(2, region.size());
      region.put("1", "1`");
      region.put("2", "2`");
      region.put("3", "3");
      region.put("4", "4");
      assertEquals(4, region.size());
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      assertEquals(4, region.size());
      region.close();
    } finally {
      System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "");
    }
  }

  @Test
  public void testBug41340() throws Exception {
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setSynchronous(true);
    diskProps.setRolling(true);
    diskProps.setRegionName("testBug41340");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertEquals(0, region.size());
    // put some entries
    region.put("0", "0");
    region.put("1", "1");
    region.put("2", "2");
    region.put("3", "3");


    // Create another oplog
    DiskStore store = cache.findDiskStore(region.getAttributes().getDiskStoreName());
    store.forceRoll();

    // Now create and destroy all of the entries in the new
    // oplog. This should cause us to remove the CRF but leave
    // the DRF, which has creates in reverse order. Now we have
    // garbage destroys which have higher IDs than any crate
    region.put("4", "1");
    region.put("5", "2");
    region.put("6", "3");
    region.destroy("0");
    region.destroy("6");
    region.destroy("5");
    region.destroy("4");

    store.forceRoll();

    // Force a recovery
    GemFireCacheImpl.getInstance().close();
    cache = createCache();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertEquals(3, region.size());

    // With bug 41340, this is reusing an oplog id.
    region.put("7", "7");
    // region.close();

    // Force another recovery
    GemFireCacheImpl.getInstance().close();
    cache = createCache();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    // Make sure we didn't lose the entry
    assertEquals(4, region.size());
    assertEquals("7", region.get("7"));
    region.close();
  }

  @Test
  public void testRecoverValuesFalse() {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRecoverValuesFalse");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      region.put(new Integer(1), new Integer(1));
      assertEquals(1, region.size());
      region.close();

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      assertEquals(null, ((LocalRegion) region).getValueInVM(new Integer(1)));
      assertEquals(new Integer(1), region.get(new Integer(1)));
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testRecoverValuesTrue() {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRecoverValuesTrue");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      region.put(new Integer(1), new Integer(1));
      assertEquals(1, region.size());
      region.close();

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      assertNotNull(((LocalRegion) region).getValueInVM(new Integer(1)));
      assertEquals(new Integer(1), region.get(new Integer(1)));
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testBug41119() {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRecoverValuesFalse");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      region.put(new Integer(1), new Integer(1));
      region.invalidate(new Integer(1));
      region.put(new Integer(1), new Integer(2));
      region.close();

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      assertEquals(null, ((LocalRegion) region).getValueInVM(new Integer(1)));
      assertEquals(new Integer(2), region.get(new Integer(1)));
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  /**
   * Make sure recovery correctly sets stats. Currently this test only checks inVM vs onDisk. It
   * could be enhanced to also check bucketStats. See bug 41849 for the bug that motivated this unit
   * test.
   */
  private void basicVerifyStats(boolean recovValues) {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "" + recovValues);
    try {
      diskProps.setDiskDirs(dirs);
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("basicVerifyStats");
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      region.put(new Integer(1), new Integer(1));
      region.put(new Integer(1), new Integer(2));
      region.close();

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      DiskRegion dr = ((LocalRegion) region).getDiskRegion();
      if (recovValues) {
        waitForInVMToBe(dr, 1);
        assertEquals(0, dr.getNumOverflowOnDisk());
      } else {
        assertEquals(0, dr.getNumEntriesInVM());
        assertEquals(1, dr.getNumOverflowOnDisk());
      }

      region.clear();
      assertEquals(0, dr.getNumEntriesInVM());
      assertEquals(0, dr.getNumOverflowOnDisk());

      region.put(new Integer(1), new Integer(1));
      region.put(new Integer(1), new Integer(2));
      region.localInvalidate(new Integer(1));
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      dr = ((LocalRegion) region).getDiskRegion();
      assertEquals(1, region.size());
      // invalid entries are not inVM since they have no value
      assertEquals(0, dr.getNumEntriesInVM());
      assertEquals(0, dr.getNumOverflowOnDisk());

      region.clear();
      assertEquals(0, dr.getNumEntriesInVM());
      assertEquals(0, dr.getNumOverflowOnDisk());

      region.create(new Integer(1), null);
      region.put(new Integer(1), new Integer(2));
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      dr = ((LocalRegion) region).getDiskRegion();
      if (recovValues) {
        waitForInVMToBe(dr, 1);
        assertEquals(0, dr.getNumOverflowOnDisk());
      } else {
        assertEquals(0, dr.getNumEntriesInVM());
        assertEquals(1, dr.getNumOverflowOnDisk());
      }

      region.clear();
      assertEquals(0, dr.getNumEntriesInVM());
      assertEquals(0, dr.getNumOverflowOnDisk());

      region.create(new Integer(1), null);
      region.localInvalidate(new Integer(1));
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      dr = ((LocalRegion) region).getDiskRegion();
      // invalid entries have not value so the are not inVM or onDisk
      assertEquals(0, dr.getNumEntriesInVM());
      assertEquals(0, dr.getNumOverflowOnDisk());

      region.clear();
      assertEquals(0, dr.getNumEntriesInVM());
      assertEquals(0, dr.getNumOverflowOnDisk());

      region.create(new Integer(1), null);
      region.put(new Integer(1), new Integer(2));
      region.destroy(new Integer(1));
      region.close(); // No KRF generated, force multiple reads from CRF for same key
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      dr = ((LocalRegion) region).getDiskRegion();
      // entry destroyed so not inVM or onDisk
      assertEquals(0, dr.getNumEntriesInVM());
      assertEquals(0, dr.getNumOverflowOnDisk());

      region.clear();
      assertEquals(0, dr.getNumEntriesInVM());
      assertEquals(0, dr.getNumOverflowOnDisk());

      region.create(new Integer(1), null);
      region.put(new Integer(1), new Integer(2));
      region.destroy(new Integer(1));
      region.put(new Integer(1), new Integer(2)); // recreate
      region.invalidate(new Integer(1));
      region.close(); // No KRF generated, force multiple reads from CRF for same key
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      dr = ((LocalRegion) region).getDiskRegion();
      // entry invalidated so not inVM or onDisk
      assertEquals(0, dr.getNumEntriesInVM());
      assertEquals(0, dr.getNumOverflowOnDisk());
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  private void waitForInVMToBe(final DiskRegion dr, final int expected) {
    // values are recovered async from disk
    Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS)
        .atMost(30, TimeUnit.SECONDS).until(() -> assertEquals(expected, dr.getNumEntriesInVM()));
  }

  @Test
  public void testVerifyStatsWithValues() {
    basicVerifyStats(true);
  }

  @Test
  public void testVerifyStatsNoValues() {
    basicVerifyStats(false);
  }

}

