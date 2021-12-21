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
package org.apache.geode.internal.cache.diskPerf;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.DiskRegionHelperFactory;
import org.apache.geode.internal.cache.DiskRegionProperties;
import org.apache.geode.internal.cache.DiskRegionTestingBase;
import org.apache.geode.test.junit.categories.PerformanceTest;

/**
 * Consolidated Disk Region Perftest. Overflow, Persist, OverflowWithPersist modes are tested for
 * Sync, AsyncWithBuffer and AsyncWithoutBufer writes.
 */
@Category(PerformanceTest.class)
@Ignore("Tests have no assertions")
public class DiskRegionPerfJUnitPerformanceTest extends DiskRegionTestingBase {

  private static final int counter = 0;

  private LogWriter log = null;

  private String stats = null;

  private String stats_ForSameKeyputs = null;

  private final DiskRegionProperties diskProps = new DiskRegionProperties();

  @Override
  protected final void postSetUp() throws Exception {
    diskProps.setDiskDirs(dirs);
  }

  @Test
  public void testOverflowSync1() throws Exception {
    try {
      // Create four Dirs for Disk Dirs
      File file1 = new File("testOverflowSync1Dir1");
      file1.mkdir();
      file1.deleteOnExit();
      File file2 = new File("testOverflowSync1Dir2");
      file2.mkdir();
      file2.deleteOnExit();
      File file3 = new File("testOverflowSync1Dir3");
      file3.mkdir();
      file3.deleteOnExit();
      File file4 = new File("testOverflowSync1Dir4");
      file4.mkdir();
      file4.deleteOnExit();
      dirs = new File[4];
      dirs[0] = file1;
      dirs[1] = file2;
      dirs[2] = file3;
      dirs[3] = file4;

      diskProps.setOverFlowCapacity(1000);
      region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);

    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed in testOverflowSync1 ");
    }
    // Perf test for 1kb writes
    populateData();
    System.out.println("OverflowWithSync1:: Stats for 1 kb writes :" + stats);
    // Perf test for 1kb writes. Puting values on the same KEY
    // populateDataPutOnSameKey();
    // System.out.println("OverflowWithSync1:: Stats for 1 kb writes :"+
    // stats_ForSameKeyputs);
    // Deleting all the files and logs created during the test...
    deleteFiles();

  } // end of testOverflowSync1

  @Test
  public void testOverflowASyncWithBuffer2() {
    try {
      // Create four Dirs for Disk Dirs
      File file1 = new File("testOverflowASyncWithBuffer2Dir1");
      file1.mkdir();
      file1.deleteOnExit();
      File file2 = new File("testOverflowASyncWithBuffer2Dir2");
      file2.mkdir();
      file2.deleteOnExit();
      File file3 = new File("testOverflowASyncWithBuffer2Dir3");
      file3.mkdir();
      file3.deleteOnExit();
      File file4 = new File("testOverflowASyncWithBuffer2Dir4");
      file4.mkdir();
      file4.deleteOnExit();
      dirs = new File[4];
      dirs[0] = file1;
      dirs[1] = file2;
      dirs[2] = file3;
      dirs[3] = file4;
      diskProps.setTimeInterval(15000l);
      diskProps.setBytesThreshold(10000l);
      diskProps.setOverFlowCapacity(1000);
      region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed in testOverflowASyncWithBuffer2 ");
    }
    // Perf test for 1kb writes
    populateData();
    System.out.println("OverflowASyncWithBuffer2:: Stats for 1 kb writes :" + stats);
    // Perf test for 1kb writes. Puting values on the same KEY
    // populateDataPutOnSameKey();
    // System.out.println("OverflowASyncWithBuffer2:: Stats for 1 kb writes :"+
    // stats_ForSameKeyputs);
    // Deleting all the files and logs created during the test...
    deleteFiles();

  } // end of testOverflowASyncWithBuffer2

  @Test
  public void testOverflowASyncWithoutBuffer3() {
    try {
      // Create four Dirs for Disk Dirs
      File file1 = new File("testOverflowASyncWithoutBuffer3Dir1");
      file1.mkdir();
      file1.deleteOnExit();
      File file2 = new File("testOverflowASyncWithoutBuffer3Dir2");
      file2.mkdir();
      file2.deleteOnExit();
      File file3 = new File("testOverflowASyncWithoutBuffer3Dir3");
      file3.mkdir();
      file3.deleteOnExit();
      File file4 = new File("testOverflowASyncWithoutBuffer3Dir4");
      file4.mkdir();
      file4.deleteOnExit();
      dirs = new File[4];
      dirs[0] = file1;
      dirs[1] = file2;
      dirs[2] = file3;
      dirs[3] = file4;

      diskProps.setTimeInterval(1000l);
      diskProps.setBytesThreshold(0l);
      diskProps.setOverFlowCapacity(1000);
      region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed in testOverflowASyncWithoutBuffer3");
    }
    // Perf test for 1kb writes
    populateData();
    System.out.println(
        "OverflowASyncWITHOUTBuffer3 (with DiskWriteAttributes Time-out of 1 Second):: Stats for 1 kb writes :"
            + stats);
    // Perf test for 1kb writes. Puting values on the same KEY
    // populateDataPutOnSameKey();
    // System.out.println("OverflowASyncWITHOUTBuffer3:: Stats for 1 kb writes
    // :"+ stats_ForSameKeyputs);
    // Deleting all the files and logs created during the test...
    deleteFiles();

  } // end of testOverflowASyncWithoutBuffer3

  @Test
  public void testpersistSync4() {
    try {
      // Create four Dirs for Disk Dirs
      File file1 = new File("testpersistSync4Dir1");
      file1.mkdir();
      file1.deleteOnExit();
      File file2 = new File("testpersistSync4Dir2");
      file2.mkdir();
      file2.deleteOnExit();
      File file3 = new File("testpersistSync4Dir3");
      file3.mkdir();
      file3.deleteOnExit();
      File file4 = new File("testpersistSync4Dir4");
      file4.mkdir();
      file4.deleteOnExit();
      dirs = new File[4];
      dirs[0] = file1;
      dirs[1] = file2;
      dirs[2] = file3;
      dirs[3] = file4;

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed in testpersistSync4");
    }
    // Perf test for 1kb writes
    populateData();
    System.out.println("PersistOnlySync4:: Stats for 1 kb writes :" + stats);
    // Perf test for 1kb writes. Puting values on the same KEY
    // populateDataPutOnSameKey();
    // System.out.println("PersistOnlySync4:: Stats for 1 kb writes :"+
    // stats_ForSameKeyputs);
    // Deleting all the files and logs created during the test...
    deleteFiles();
    // closeDown();

  } // end of testPersistSync4

  @Test
  public void testpersistASyncWithBuffer5() {
    try {
      // Create four Dirs for Disk Dirs
      File file1 = new File("testpersistASyncWithBuffer5Dir1");
      file1.mkdir();
      file1.deleteOnExit();
      File file2 = new File("testpersistASyncWithBuffer5Dir2");
      file2.mkdir();
      file2.deleteOnExit();
      File file3 = new File("testpersistASyncWithBuffer5Dir3");
      file3.mkdir();
      file3.deleteOnExit();
      File file4 = new File("testpersistASyncWithBuffer5Dir4");
      file4.mkdir();
      file4.deleteOnExit();
      dirs = new File[4];
      dirs[0] = file1;
      dirs[1] = file2;
      dirs[2] = file3;
      dirs[3] = file4;

      diskProps.setTimeInterval(15000l);
      diskProps.setBytesThreshold(10000l);
      region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed in testpersistASyncWithBuffer5");
    }
    // Perf test for 1kb writes
    populateData();
    System.out.println("PersistASyncWithBuffer5:: Stats for 1 kb writes :" + stats);
    // Perf test for 1kb writes. Puting values on the same KEY
    // populateDataPutOnSameKey();
    // System.out.println("OverflowASyncWithBuffer5:: Stats for 1 kb writes :"+
    // stats_ForSameKeyputs);
    // Deleting all the files and logs created during the test...
    deleteFiles();
    // closeDown();

  } // end of testPersistASyncWithBuffer5

  @Test
  public void testPersistASyncWithoutBuffer6() {
    try {
      // Create four Dirs for Disk Dirs
      File file1 = new File("testPersistASyncWithoutBuffer6Dir1");
      file1.mkdir();
      file1.deleteOnExit();
      File file2 = new File("testPersistASyncWithoutBuffer6Dir2");
      file2.mkdir();
      file2.deleteOnExit();
      File file3 = new File("testPersistASyncWithoutBuffer6Dir3");
      file3.mkdir();
      file3.deleteOnExit();
      File file4 = new File("testPersistASyncWithoutBuffer6Dir4");
      file4.mkdir();
      file4.deleteOnExit();
      dirs = new File[4];
      dirs[0] = file1;
      dirs[1] = file2;
      dirs[2] = file3;
      dirs[3] = file4;

      diskProps.setTimeInterval(15000l);
      diskProps.setBytesThreshold(0l);
      region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed in testPersistASyncWithoutBuffer6");
    }
    // Perf test for 1kb writes
    populateData();
    System.out.println(
        "PersistASyncWITHOUTBuffer6(with DiskWriteAttributes Time-out of 1 Second):: Stats for 1 kb writes :"
            + stats);
    // Perf test for 1kb writes. Puting values on the same KEY
    // populateDataPutOnSameKey();
    // System.out.println("OverflowASyncWITHOUTBuffer6:: Stats for 1 kb writes
    // :"+ stats_ForSameKeyputs);
    // Deleting all the files and logs created during the test...
    deleteFiles();
    // closeDown();


  } // end of testPersistASyncWithoutBuffer

  @Test
  public void testPersistOverflowSync7() {
    try {
      // Create four Dirs for Disk Dirs
      File file1 = new File("testPersistOverflowSync7Dir1");
      file1.mkdir();
      file1.deleteOnExit();
      File file2 = new File("testPersistOverflowSync7Dir2");
      file2.mkdir();
      file2.deleteOnExit();
      File file3 = new File("testPersistOverflowSync7Dir3");
      file3.mkdir();
      file3.deleteOnExit();
      File file4 = new File("testPersistOverflowSync7Dir4");
      file4.mkdir();
      file4.deleteOnExit();
      dirs = new File[4];
      dirs[0] = file1;
      dirs[1] = file2;
      dirs[2] = file3;
      dirs[3] = file4;

      diskProps.setOverFlowCapacity(1000);
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed in testPersistOverflowSync7 ");
    }
    // Perf test for 1kb writes
    populateData();
    System.out.println("PersistOverflowWithSync7:: Stats for 1 kb writes :" + stats);
    // Perf test for 1kb writes. Puting values on the same KEY
    // populateDataPutOnSameKey();
    // System.out.println("PersistOverflowWithSync7:: Stats for 1 kb writes :"+
    // stats_ForSameKeyputs);
    // Deleting all the files and logs created during the test...
    deleteFiles();
    // closeDown();

  } // end of testPersistOverflowSync

  @Test
  public void testPersistOverflowASyncWithBuffer8() {
    try {
      // Create four Dirs for Disk Dirs
      File file1 = new File("testPersistOverflowASyncWithBuffer8Dir1");
      file1.mkdir();
      file1.deleteOnExit();
      File file2 = new File("testPersistOverflowASyncWithBuffer8Dir2");
      file2.mkdir();
      file2.deleteOnExit();
      File file3 = new File("testPersistOverflowASyncWithBuffer8Dir3");
      file3.mkdir();
      file3.deleteOnExit();
      File file4 = new File("testPersistOverflowASyncWithBuffer8Dir4");
      file4.mkdir();
      file4.deleteOnExit();
      dirs = new File[4];
      dirs[0] = file1;
      dirs[1] = file2;
      dirs[2] = file3;
      dirs[3] = file4;

      diskProps.setTimeInterval(15000l);
      diskProps.setBytesThreshold(10000l);
      diskProps.setOverFlowCapacity(1000);
      region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, diskProps);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed in testPersistOverflowASyncWithBuffer8");
    }
    // Perf test for 1kb writes
    populateData();
    System.out.println("PersistOverflowASyncWithBuffer8:: Stats for 1 kb writes :" + stats);
    // Perf test for 1kb writes. Puting values on the same KEY
    // populateDataPutOnSameKey();
    // System.out.println("PersistOverflowASyncWithBuffer8:: Stats for 1 kb
    // writes :"+ stats_ForSameKeyputs);
    // Deleting all the files and logs created during the test...
    deleteFiles();
    // closeDown();

  } // end of testpersistOverflowASyncWithBuffer8

  @Test
  public void testPersistOverflowASyncWithoutBuffer9() {
    try {
      // Create four Dirs for Disk Dirs
      File file1 = new File("testPersistOverflowASyncWithoutBuffer9Dir1");
      file1.mkdir();
      file1.deleteOnExit();
      File file2 = new File("testPersistOverflowASyncWithoutBuffer9Dir2");
      file2.mkdir();
      file2.deleteOnExit();
      File file3 = new File("testPersistOverflowASyncWithoutBuffer9Dir3");
      file3.mkdir();
      file3.deleteOnExit();
      File file4 = new File("testPersistOverflowASyncWithoutBuffer9Dir4");
      file4.mkdir();
      file4.deleteOnExit();
      dirs = new File[4];
      dirs[0] = file1;
      dirs[1] = file2;
      dirs[2] = file3;
      dirs[3] = file4;

      diskProps.setTimeInterval(15000l);
      diskProps.setBytesThreshold(0l);
      diskProps.setOverFlowCapacity(1000);
      region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, diskProps);
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed in testPersistOverflowASyncWithoutBuffer9");
    }
    // Perf test for 1kb writes
    populateData();
    System.out.println(
        "Persist-OverflowASyncWITHOUTBuffer9(with DiskWriteAttributes Time-out of 1 Second):: Stats for 1 kb writes :"
            + stats);
    // Perf test for 1kb writes. Puting values on the same KEY
    // populateDataPutOnSameKey();
    // System.out.println("Persist-OverflowASyncWITHOUTBuffer9:: Stats for 1 kb
    // writes :"+ stats_ForSameKeyputs);
    // Deleting all the files and logs created during the test...
    deleteFiles();
    // closeDown();

  } // end of testPersistOverflowASyncWithoutBuffer9

  public static int ENTRY_SIZE = 1024;

  /**
   * OP_COUNT can be increased/decrease as per the requirement. If required to be set as higher
   * value such as 1000000 one needs to set the VM heap size accordingly. (For example:Default
   * setting in build.xml is <jvmarg value="-Xmx256M"/>
   */
  public static int OP_COUNT = 100;

  public static boolean UNIQUE_KEYS = Boolean.getBoolean("DRP.UNIQUE_KEYS");

  public void populateData() {
    // Put for validation.
    putForValidation(region);

    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte) 77);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < OP_COUNT; i++) {
      region.put("" + i, value);
      // System.out.println(i);
    }
    long endTime = System.currentTimeMillis();
    System.out.println(" done with putting");
    // validate put operation
    validatePut(region);
    region.destroyRegion();// closes disk file which will flush all buffers
    float et = endTime - startTime;
    float etSecs = et / 1000f;
    float opPerSec = etSecs == 0 ? 0 : (OP_COUNT / (et / 1000f));
    float bytesPerSec = etSecs == 0 ? 0 : ((OP_COUNT * ENTRY_SIZE) / (et / 1000f));
    stats = "et=" + et + "ms writes/sec=" + opPerSec + " bytes/sec=" + bytesPerSec;
    log = ds.getLogWriter();
    log.info(stats);
  }

  public void populateDataPutOnSameKey() {
    // Put for validation.
    putForValidation(region);
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte) 77);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < OP_COUNT; i++) {
      region.put("K", value);
    }
    long endTime = System.currentTimeMillis();
    System.out.println(" done with putting");
    // validate put operation
    validatePut(region);
    region.close(); // closes disk file which will flush all buffers
    float et = endTime - startTime;
    float etSecs = et / 1000f;
    float opPerSec = etSecs == 0 ? 0 : (OP_COUNT / (et / 1000f));
    float bytesPerSec = etSecs == 0 ? 0 : ((OP_COUNT * ENTRY_SIZE) / (et / 1000f));
    stats_ForSameKeyputs = "et=" + et + "ms writes/sec=" + opPerSec + " bytes/sec=" + bytesPerSec;
    log = ds.getLogWriter();
    log.info(stats_ForSameKeyputs);
  }

  @Override
  protected void deleteFiles() {
    for (int i = 0; i < 4; i++) {
      File[] files = dirs[i].listFiles();
      for (int j = 0; j < files.length; j++) {
        files[j].delete();
      }
    }
  }

}// end of DiskRegionPerfJUnitTest
