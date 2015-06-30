/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.diskPerf;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.cache.DiskRegionHelperFactory;
import com.gemstone.gemfire.internal.cache.DiskRegionProperties;
import com.gemstone.gemfire.internal.cache.DiskRegionTestingBase;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Disk region Perf test for Overflow only with Sync writes. 1) Performance of
 * get operation for entry in memory.
 *  
 */
@Category(IntegrationTest.class)
public class DiskRegOverflowSyncGetInMemPerfJUnitPerformanceTest extends DiskRegionTestingBase
{

  LogWriter log = null;

  static int counter = 0;

  DiskRegionProperties diskProps = new DiskRegionProperties();

  @Before
  public void setUp() throws Exception
  {
    diskProps.setDiskDirs(dirs);
    super.setUp();

    diskProps.setOverFlowCapacity(100000);
    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    
    log = ds.getLogWriter();
  }

  @After
  public void tearDown() throws Exception
  {
    super.tearDown();
    if (cache != null) {
      cache.close();
    }
    if (ds != null) {
      ds.disconnect();
    }
  }

  

  private static int ENTRY_SIZE = 1024;

  private static int OP_COUNT = 10000;

  @Test
  public void testPopulatefor1Kbwrites()
  {
//    RegionAttributes ra = region.getAttributes();
//    final String key = "K";
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte)77);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < OP_COUNT; i++) {
      region.put("" + (i + 10000), value);
    }
    long endTime = System.currentTimeMillis();
    System.out.println(" done with putting");
    //Now get all the entries which are in memory.
    long startTimeGet = System.currentTimeMillis();
    for (int i = 0; i < OP_COUNT; i++) {
      region.get("" + (i + 10000));
  
    }
    long endTimeGet = System.currentTimeMillis();
    System.out.println(" done with getting");

    region.close(); // closes disk file which will flush all buffers
    float et = endTime - startTime;
    float etSecs = et / 1000f;
    float opPerSec = etSecs == 0 ? 0 : (OP_COUNT / (et / 1000f));
    float bytesPerSec = etSecs == 0 ? 0
        : ((OP_COUNT * ENTRY_SIZE) / (et / 1000f));

    String stats = "et=" + et + "ms writes/sec=" + opPerSec + " bytes/sec="
        + bytesPerSec;
    log.info(stats);
    System.out.println("Stats for 1 kb writes:" + stats);
    // Perf stats for get op
    float etGet = endTimeGet - startTimeGet;
    float etSecsGet = etGet / 1000f;
    float opPerSecGet = etSecsGet == 0 ? 0 : (OP_COUNT / (etGet / 1000f));
    float bytesPerSecGet = etSecsGet == 0 ? 0
        : ((OP_COUNT * ENTRY_SIZE) / (etGet / 1000f));

    String statsGet = "et=" + etGet + "ms gets/sec=" + opPerSecGet
        + " bytes/sec=" + bytesPerSecGet;
    log.info(statsGet);
    System.out.println("Perf Stats of get which is in memory :" + statsGet);

  }

}

