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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.junit.Test;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.versions.VersionTag;

public class DiskRandomOperationsAndRecoveryJUnitTest extends DiskRegionTestingBase {

  private static final int ENTRY_SIZE = 1024;

  private static final byte[] valueBytes = new byte[ENTRY_SIZE];
  static {
    Arrays.fill(valueBytes, (byte) 32);
  }

  private static final Object value = new String(valueBytes);

  private static int testId = 0;

  private DiskRegionProperties diskProps = new DiskRegionProperties();

  @Override
  protected final void postSetUp() throws Exception {
    diskProps.setDiskDirs(dirs);
    testId++;
  }

  private static final int ITERATIONS = 4;
  private static final long MAX_OPLOG_SIZE_IN_BYTES = 1024 * 16;
  /**
   * Need to limit the max open oplogs so that we don't run out of file descriptors
   */
  private static final int MAX_OPEN_OPLOGS = 400;
  private static final long RECORDS_PER_OPLOG =
      MAX_OPLOG_SIZE_IN_BYTES / (ENTRY_SIZE + 24/* for key and record overhead */);
  /**
   * Maximum number of ops a test can do w/o running out of file descriptors due to open oplogs
   */
  private static final long OPS_PER_TEST = RECORDS_PER_OPLOG * MAX_OPEN_OPLOGS;
  private static final long OPS_PER_ITERATION = OPS_PER_TEST / ITERATIONS;

  @Test
  public void testRollingDisabledRecoverValuesFalsePersistOnly() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRollingDisabledRecoverValuesFalsePersistOnly");
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE_IN_BYTES);
      diskProps.setRolling(false);
      int previousRegionSize = 0;
      HashMap<String, VersionTag> tagmapInCache = null;
      // Num time start / close cycles
      for (int i = 0; i < ITERATIONS; ++i) {
        long t1 = System.currentTimeMillis();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
        long t2 = System.currentTimeMillis();
        System.out.println("Time taken to recover = " + (t2 - t1) + " for total number of entries= "
            + region.size() + " with recover values as false  and rolling disabled ");
        if ((t2 - t1) > 0) {
          System.out
              .println("Recovery rate is= " + region.size() / (t2 - t1) + " per milliseconds");
        }
        int startKey = this.processRegionData();
        assertEquals(previousRegionSize, region.size());
        HashMap<String, VersionTag> tagmapFromRecover = saveVersionTags((LocalRegion) region);
        if (tagmapInCache != null) {
          compareVersionTags(tagmapInCache, tagmapFromRecover);
        }
        int thisRegionSize = startOperations(startKey, value);
        previousRegionSize = thisRegionSize;
        tagmapInCache = saveVersionTags((LocalRegion) region);
        region.close();
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
  public void testRollingDisabledRecoverValuesTruePersistOnly() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    try {
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRollingDisabledRecoverValuesTruePersistOnly");
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE_IN_BYTES);
      diskProps.setRolling(false);
      int previousRegionSize = 0;
      HashMap<String, VersionTag> tagmapInCache = null;
      // Num time start / close cycles
      for (int i = 0; i < ITERATIONS; ++i) {
        long t1 = System.currentTimeMillis();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
        long t2 = System.currentTimeMillis();
        System.out.println("Time taken to recover = " + (t2 - t1) + " for total number of entries= "
            + region.size() + " with recover values as false  and rolling disabled ");
        if ((t2 - t1) > 0) {
          System.out
              .println("Recovery rate is= " + region.size() / (t2 - t1) + " per milliseconds");
        }
        int startKey = this.processRegionData();
        assertEquals(previousRegionSize, region.size());
        HashMap<String, VersionTag> tagmapFromRecover = saveVersionTags((LocalRegion) region);
        if (tagmapInCache != null) {
          compareVersionTags(tagmapInCache, tagmapFromRecover);
        }
        int thisRegionSize = startOperations(startKey, value);
        previousRegionSize = thisRegionSize;
        tagmapInCache = saveVersionTags((LocalRegion) region);
        region.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  ////////////////////////////////////////////////////////////// Set 1 Begin //////////////////
  @Test
  public void testRollingEnabledRecoverValuesFalsePersistOnlyWithEarlyTerminationOfRoller()
      throws Exception {
    System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "false");
    // System.setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "true");
    try {
      this.rollingEnabledRecoverValuesFalsePersistOnly();
    } finally {
      // System.setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "false");
    }
  }

  @Test
  public void testRollingEnabledRecoverValuesFalsePersistOnlyWithRollerTerminationComplete()
      throws Exception {
    try {
      System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME,
          "true");
      // System.setProperty(DiskRegion.ASSERT_ON_RECOVERY_PROPERTY_NAME, "true");
      this.rollingEnabledRecoverValuesFalsePersistOnly();
    } finally {
      System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME,
          "false");
      // System.setProperty(DiskRegion.ASSERT_ON_RECOVERY_PROPERTY_NAME, "false");
    }
  }

  private void rollingEnabledRecoverValuesFalsePersistOnly() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRollingEnabledRecoverValuesFalsePersistOnly");
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE_IN_BYTES);
      diskProps.setRolling(true);
      int previousRegionSize = 0;
      HashMap<String, VersionTag> tagmapInCache = null;
      // Num time start / close cycles
      for (int i = 0; i < ITERATIONS; ++i) {
        long t1 = System.currentTimeMillis();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
        long t2 = System.currentTimeMillis();
        System.out.println("Time taken to recover = " + (t2 - t1) + " for total number of entries= "
            + region.size() + " with recover values as false and rolling disabled ");
        if ((t2 - t1) > 0) {
          System.out
              .println("Recovery rate is= " + region.size() / (t2 - t1) + " per milliseconds");
        }
        int startKey = this.processRegionData();
        assertEquals(previousRegionSize, region.size());
        HashMap<String, VersionTag> tagmapFromRecover = saveVersionTags((LocalRegion) region);
        if (tagmapInCache != null) {
          compareVersionTags(tagmapInCache, tagmapFromRecover);
        }
        int thisRegionSize = startOperations(startKey, value);
        previousRegionSize = thisRegionSize;
        tagmapInCache = saveVersionTags((LocalRegion) region);
        region.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  ////////////////////////////////////////////////////////////// Set 1 END //////////////////


  ////////////////////////////////////////////////////////////// Set 2 Begin //////////////////
  @Test
  public void testRollingEnabledRecoverValuesTruePersistOnlyWithEarlyTerminationOfRoller()
      throws Exception {
    System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "false");
    // System.setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "true");
    try {
      this.rollingEnabledRecoverValuesTruePersistOnly();
    } finally {
      // System.setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "false");
    }
  }

  @Test
  public void testRollingEnabledRecoverValuesTruePersistOnlyWithRollerTerminationComplete()
      throws Exception {
    try {
      System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME,
          "true");
      // System.setProperty(DiskRegion.ASSERT_ON_RECOVERY_PROPERTY_NAME, "true");
      this.rollingEnabledRecoverValuesTruePersistOnly();
    } finally {
      System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME,
          "false");
      // System.setProperty(DiskRegion.ASSERT_ON_RECOVERY_PROPERTY_NAME, "false");
    }
  }

  private void rollingEnabledRecoverValuesTruePersistOnly() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    try {
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRollingEnabledRecoverValuesTruePersistOnly");
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE_IN_BYTES);
      diskProps.setRolling(true);
      int previousRegionSize = 0;
      HashMap<String, VersionTag> tagmapInCache = null;
      // Num time start / close cycles
      for (int i = 0; i < ITERATIONS; ++i) {
        long t1 = System.currentTimeMillis();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
        long t2 = System.currentTimeMillis();
        System.out.println("Time taken to recover = " + (t2 - t1) + " for total number of entries= "
            + region.size() + " with recover values as false  and rolling disabled ");
        if ((t2 - t1) > 0) {
          System.out
              .println("Recovery rate is= " + region.size() / (t2 - t1) + " per milliseconds");
        }
        int startKey = this.processRegionData();
        assertEquals(previousRegionSize, region.size());
        HashMap<String, VersionTag> tagmapFromRecover = saveVersionTags((LocalRegion) region);
        if (tagmapInCache != null) {
          compareVersionTags(tagmapInCache, tagmapFromRecover);
        }
        int thisRegionSize = startOperations(startKey, value);
        previousRegionSize = thisRegionSize;
        tagmapInCache = saveVersionTags((LocalRegion) region);
        region.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  ////////////////////////////////////////////////////////////// Set 2 end //////////////////

  @Test
  public void testRollingDisabledRecoverValuesFalsePersistWithOverFlow() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRollingDisabledRecoverValuesFalsePersistWithOverFlow");
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE_IN_BYTES);
      diskProps.setRolling(false);
      diskProps.setOverFlowCapacity(100);
      int previousRegionSize = 0;
      HashMap<String, VersionTag> tagmapInCache = null;
      // Num time start / close cycles
      for (int i = 0; i < ITERATIONS; ++i) {
        long t1 = System.currentTimeMillis();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
        long t2 = System.currentTimeMillis();
        System.out.println("Time taken to recover = " + (t2 - t1) + " for total number of entries= "
            + region.size() + " with recover values as false  and rolling disabled ");
        if ((t2 - t1) > 0) {
          System.out
              .println("Recovery rate is= " + region.size() / (t2 - t1) + " per milliseconds");
        }
        int startKey = this.processRegionData();
        assertEquals(previousRegionSize, region.size());
        HashMap<String, VersionTag> tagmapFromRecover = saveVersionTags((LocalRegion) region);
        if (tagmapInCache != null) {
          compareVersionTags(tagmapInCache, tagmapFromRecover);
        }
        int thisRegionSize = startOperations(startKey, value);
        previousRegionSize = thisRegionSize;
        tagmapInCache = saveVersionTags((LocalRegion) region);
        region.close();
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
  public void testRollingDisabledRecoverValuesTruePersistWithOverFlow() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    try {
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRollingDisabledRecoverValuesTruePersistWithOverFlow");
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE_IN_BYTES);
      diskProps.setRolling(false);
      diskProps.setOverFlowCapacity(100);
      int previousRegionSize = 0;
      HashMap<String, VersionTag> tagmapInCache = null;
      // Num time start / close cycles
      for (int i = 0; i < ITERATIONS; ++i) {
        long t1 = System.currentTimeMillis();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
        long t2 = System.currentTimeMillis();
        System.out.println("Time taken to recover = " + (t2 - t1) + " for total number of entries= "
            + region.size() + " with recover values as false  and rolling disabled ");
        if ((t2 - t1) > 0) {
          System.out
              .println("Recovery rate is= " + region.size() / (t2 - t1) + " per milliseconds");
        }
        int startKey = this.processRegionData();
        assertEquals(previousRegionSize, region.size());
        HashMap<String, VersionTag> tagmapFromRecover = saveVersionTags((LocalRegion) region);
        if (tagmapInCache != null) {
          compareVersionTags(tagmapInCache, tagmapFromRecover);
        }
        int thisRegionSize = startOperations(startKey, value);
        previousRegionSize = thisRegionSize;
        tagmapInCache = saveVersionTags((LocalRegion) region);
        region.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }


  /////////////////////////////////////// Set 3 Begin /////////////////
  @Test
  public void testRollingEnabledRecoverValuesFalsePersistWithOverFlowWithEarlyTerminationOfRoller()
      throws Exception {
    System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "false");
    // System.setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "true");
    try {
      this.rollingEnabledRecoverValuesFalsePersistWithOverFlow();
    } finally {
      // System.setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "false");
    }
  }

  @Test
  public void testRollingEnabledRecoverValuesFalsePersistWithOverFlowWithRollerTerminatingAfterCompletion()
      throws Exception {
    System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "true");
    // System.setProperty(DiskRegion.ASSERT_ON_RECOVERY_PROPERTY_NAME, "true");
    try {
      this.rollingEnabledRecoverValuesFalsePersistWithOverFlow();
    } finally {
      System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME,
          "false");
      // System.setProperty(DiskRegion.ASSERT_ON_RECOVERY_PROPERTY_NAME, "false");
    }
  }

  private void rollingEnabledRecoverValuesFalsePersistWithOverFlow() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRollingDisabledRecoverValuesFalsePersistWithOverFlow");
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE_IN_BYTES);
      diskProps.setRolling(true);
      diskProps.setOverFlowCapacity(100);
      int previousRegionSize = 0;
      HashMap<String, VersionTag> tagmapInCache = null;
      // Num time start / close cycles
      for (int i = 0; i < ITERATIONS; ++i) {
        long t1 = System.currentTimeMillis();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
        long t2 = System.currentTimeMillis();
        System.out.println("Time taken to recover = " + (t2 - t1) + " for total number of entries= "
            + region.size() + " with recover values as false and rolling disabled.");
        if ((t2 - t1) > 0) {
          System.out
              .println("Recovery rate is= " + region.size() / (t2 - t1) + " per milliseconds");
        }
        int startKey = this.processRegionData();
        assertEquals(previousRegionSize, region.size());
        HashMap<String, VersionTag> tagmapFromRecover = saveVersionTags((LocalRegion) region);
        if (tagmapInCache != null) {
          compareVersionTags(tagmapInCache, tagmapFromRecover);
        }
        int thisRegionSize = startOperations(startKey, value);
        previousRegionSize = thisRegionSize;
        tagmapInCache = saveVersionTags((LocalRegion) region);
        region.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }
  /////////////////////////////////////// Set 3 End /////////////////



  ////////////////////////////////////////// Set 4 begin/////////////////
  @Test
  public void testRollingEnabledRecoverValuesTruePersistWithOverFlowWithEarlyTerminationOfRoller()
      throws Exception {
    System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "false");
    // System.setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "true");
    try {
      this.rollingEnabledRecoverValuesTruePersistWithOverFlow();
    } finally {
      // System.setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "false");
    }
  }

  @Test
  public void testRollingEnabledRecoverValuesTruePersistWithOverFlowWithRollerTerminatingAfterCompletion()
      throws Exception {
    System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "true");
    // System.setProperty(DiskRegion.ASSERT_ON_RECOVERY_PROPERTY_NAME, "true");
    try {
      this.rollingEnabledRecoverValuesTruePersistWithOverFlow();
    } finally {
      System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME,
          "false");
      // System.setProperty(DiskRegion.ASSERT_ON_RECOVERY_PROPERTY_NAME, "false");
    }
  }

  private void rollingEnabledRecoverValuesTruePersistWithOverFlow() throws Exception {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    try {
      diskProps.setPersistBackup(true);
      diskProps.setRegionName("testRollingDisabledRecoverValuesTruePersistWithOverFlow");
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE_IN_BYTES);
      diskProps.setRolling(true);
      diskProps.setOverFlowCapacity(100);
      int previousRegionSize = 0;
      HashMap<String, VersionTag> tagmapInCache = null;
      // Num time start / close cycles
      for (int i = 0; i < ITERATIONS; ++i) {
        long t1 = System.currentTimeMillis();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
        long t2 = System.currentTimeMillis();
        System.out.println("Time taken to recover = " + (t2 - t1) + " for total number of entries= "
            + region.size() + " with recover values as false  and rolling disabled ");
        if ((t2 - t1) > 0) {
          System.out
              .println("Recovery rate is= " + region.size() / (t2 - t1) + " per milliseconds");
        }
        int startKey = this.processRegionData();
        assertEquals(previousRegionSize, region.size());
        HashMap<String, VersionTag> tagmapFromRecover = saveVersionTags((LocalRegion) region);
        if (tagmapInCache != null) {
          compareVersionTags(tagmapInCache, tagmapFromRecover);
        }
        int thisRegionSize = startOperations(startKey, value);
        previousRegionSize = thisRegionSize;
        tagmapInCache = saveVersionTags((LocalRegion) region);
        region.close();
      }
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  ////////////////////////////////////////// Set 4 end/////////////////


  @Test
  public void testRollingDisabledRecoverValuesFalseWithNotifyToRollAPICall() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testRollingDisabledRecoverValuesFalseWithNotifyToRoll");
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE_IN_BYTES);
    diskProps.setRolling(false);
    int previousRegionSize = 0;
    HashMap<String, VersionTag> tagmapInCache = null;
    final boolean[] run = new boolean[] {true};
    // Num time start / close cycles
    for (int i = 0; i < ITERATIONS; ++i) {
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      int startKey = this.processRegionData();
      assertEquals(previousRegionSize, region.size());
      HashMap<String, VersionTag> tagmapFromRecover = saveVersionTags((LocalRegion) region);
      if (tagmapInCache != null) {
        compareVersionTags(tagmapInCache, tagmapFromRecover);
      }
      Thread th = new Thread(new Runnable() {
        public void run() {
          while (run[0]) {
            try {
              Thread.sleep(1 * 1000);
            } catch (InterruptedException ie) {
              ie.printStackTrace();
            }
            ((LocalRegion) region).notifyToRoll();

          }
        }
      });
      int thisRegionSize = startOperations(startKey, value);
      previousRegionSize = thisRegionSize;
      run[0] = false;
      th.join();
      tagmapInCache = saveVersionTags((LocalRegion) region);
      region.close();
    }

  }

  public int processRegionData() throws Exception {
    System.out.println("Total entries in region at start = " + region.size());
    int startKey = 0;
    Iterator entryItr = region.entrySet().iterator();
    startKey = 0;

    while (entryItr.hasNext()) {
      Region.Entry entry = (Region.Entry) entryItr.next();
      String key = (String) entry.getKey();
      int indx = key.lastIndexOf('_');
      int temp = Integer.parseInt(key.substring(indx + 1));
      if (temp > startKey) {
        startKey = temp;
      }
      if (entry.getValue() instanceof String) {
        String val = (String) entry.getValue();
        if (!val.equals(value)) {
          throw new IllegalStateException("Values do not match");
        }
      } else if (entry.getValue() instanceof byte[]) {
        byte[] val = (byte[]) entry.getValue();
        // System.out.println("Entry " + key + " had a byte array of size " + val.length
        // + " whose first byte was " + val[0]);
        assertEquals(((byte[]) value).length, val.length);
        for (int i = 0; i < val.length; i++) {
          assertEquals("at offset " + i, ((byte[]) value)[i], val[i]);
        }
      } else {
        assertEquals(value, entry.getValue());
      }
    }
    return ++startKey;


  }


  /**
   * Disk region recovery test for Persist only with sync writes. Test has four steps : STEP 1:
   * Create cache. Create Region. Put entries. Close cache. STEP 2: Create cache. Create Region with
   * the same name as that of in STEP 1. Delete some entries. 3) Recreate the deleted entries Close
   * the Cache * 3: Again Create cache. 4) check if the region creation is successful
   *
   */
  public int startOperations(final int startKey, final Object value) throws Exception {


    final int NUM_THREADS = 5;
    final long OPS_PER_THREAD = OPS_PER_ITERATION / NUM_THREADS;
    Thread operations[] = new Thread[NUM_THREADS];
    System.out.println(
        "Starting " + NUM_THREADS + " threads to do each do " + OPS_PER_THREAD + " operations");
    for (int i = 0; i < NUM_THREADS; ++i) {
      operations[i] = new Operation(i, region, value, startKey, OPS_PER_THREAD);
    }

    for (int i = 0; i < NUM_THREADS; ++i) {
      operations[i].start();
    }
    // synchronized(this) {
    // this.wait(operationTimeInSec*1000);
    // }

    // for (int i = 0; i < NUM_THREADS; ++i) {
    // ((Operation)operations[i]).signalHalt();
    // }

    for (int i = 0; i < NUM_THREADS; ++i) {
      operations[i].join();
    }
    int regionSize = region.size();
    System.out.println("Total Region Size at end = " + region.size());
    return regionSize;
  }

  class Operation extends Thread {
    final int id;

    volatile boolean run = true;

    int createKeyID = 1;

    final Region rgn;

    Object value;

    volatile int totalEntries = 0;
    final long maxOpCount;

    public Operation(int id, Region rgn, Object value, int startKey, long maxOpCount) {
      this.id = id;
      this.rgn = rgn;
      this.value = value;
      this.createKeyID = startKey;
      this.maxOpCount = maxOpCount;
    }

    // public void signalHalt()
    // {
    // this.run = false;
    // }

    public void run() {
      Random opRandom = new Random();
      Random keyRandom = new Random();
      long opCount = 0;
      while (opCount < this.maxOpCount) {
        int optype = opRandom.nextInt(3);
        switch (optype) {
          case 0:
            this.rgn.create(testId + "_" + id + "_" + createKeyID, value);
            opCount++;
            ++createKeyID;
            ++totalEntries;
            break;
          case 1: {
            int key = keyRandom.nextInt(createKeyID);
            try {
              this.rgn.put(testId + "_" + id + "_" + key, value);
              opCount++;
            } catch (EntryNotFoundException enfe) {

            } catch (EntryDestroyedException ede) {

            }
          }
            break;
          case 2: {
            int key = keyRandom.nextInt(createKeyID);
            try {
              this.rgn.destroy(testId + "_" + id + "_" + key);
              // don't count these as ops; they are tiny
              --totalEntries;
            } catch (EntryNotFoundException enfe) {

            } catch (EntryDestroyedException ede) {

            }
          }
            break;
          default:
            throw new IllegalStateException();

        }
      }
    }
  }

}
