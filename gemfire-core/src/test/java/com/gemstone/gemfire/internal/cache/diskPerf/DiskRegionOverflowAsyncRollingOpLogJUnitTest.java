/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.diskPerf;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 1) Performance of Get Operation for Entry faulting in from current Op Log 2)
 * Performance of Get operation for Entry faulting in from previous Op Log 3)
 * Performance of Get operation for Entry faulting in from H Tree
 * 
 * @author Vikram Jadhav
 *  
 */
@Category(IntegrationTest.class)
public class DiskRegionOverflowAsyncRollingOpLogJUnitTest extends
    DiskRegionTestingBase
{


  
  LogWriter log = null;

  static int counter = 0;

  private static int ENTRY_SIZE = 1024;

  DiskRegionProperties diskProps = new DiskRegionProperties();

  @Before
  public void setUp() throws Exception
  {
    super.setUp();
    diskProps.setRegionName("OverflowAsyncRollingOpLogRegion");
    diskProps.setDiskDirs(dirs);
    this.log = ds.getLogWriter();
    diskProps.setTimeInterval(1000l);
    diskProps.setBytesThreshold(10000l);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setMaxOplogSize(10485760l);
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache,
        diskProps);

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

  

  @Test
  public void testGetPerfRollingOpog()
  {
    populateFirst0k_10Kbwrites();
    populateSecond10kto20kwrites();
    
  }

  public void populateFirst0k_10Kbwrites()
  {
    
   final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte)77);

    for (int i = 0; i < 10000; i++) {
      region.put("" + i, value);
    }

    //Now get 0-9999 entries
    long startTimeGet = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      region.get("" + i);
    }
    long endTimeGet = System.currentTimeMillis();
    if(debug) System.out
        .println(" done with getting 0-9999 entries fuatling in from current oplog");

    // Perf stats for get op
    float etGet = endTimeGet - startTimeGet;
    float etSecsGet = etGet / 1000f;
    float opPerSecGet = etSecsGet == 0 ? 0 : (10000 / (etGet / 1000f));
    float bytesPerSecGet = etSecsGet == 0 ? 0
        : ((10000 * ENTRY_SIZE) / (etGet / 1000f));

    String statsGet = "etGet=" + etGet + "ms gets/sec=" + opPerSecGet
        + " bytes/sec=" + bytesPerSecGet;
    log.info(statsGet);
    if(debug) System.out
        .println("Perf Stats of get which is fauting in from current Oplog :"
            + statsGet);

  }
  
  protected volatile boolean afterHavingCompacted = false;
  public void populateSecond10kto20kwrites()
  {
    afterHavingCompacted = false;
    DiskRegionTestingBase.setCacheObserverCallBack();
    CacheObserverHolder.setInstance(new CacheObserverAdapter(){
      public void afterHavingCompacted()
      {
        afterHavingCompacted = true;
      }
    });
    
    // put another 10000-19999 entries
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte)77);

    for (int i = 10000; i < 20000; i++) {
      region.put("" + i, value);
    }
    //  Now get 10000-19999 which will fault in from second oplog
    long startTimeGet2 = System.currentTimeMillis();
    for (int i = 10000; i < 20000; i++) {
      region.get("" + i);
    }
    long endTimeGet2 = System.currentTimeMillis();
    if(debug) System.out
        .println(" done with getting 10000-19999 which will fault in from second oplog");
    //to fault-in entries from H-tree first verify that the rolling of oplog is
    // over
    if (((LocalRegion)region).getDiskRegion().isBackup()) {
      WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return afterHavingCompacted;
          }
          public String description() {
            return null;
          }
        };
      Wait.waitForCriterion(ev, 30 * 1000, 200, true);
    }

    //Now get 0-9999 entries
    long startTimeGet1 = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      region.get("" + i);
    }
    long endTimeGet1 = System.currentTimeMillis();
    if(debug) System.out.println(" done with getting 0-9999 entries from H-tree");

    region.close(); // closes disk file which will flush all buffers

    // Perf stats for get op (fauting in from H-tree)
    float etGet1 = endTimeGet1 - startTimeGet1;
    float etSecsGet1 = etGet1 / 1000f;
    float opPerSecGet1 = etSecsGet1 == 0 ? 0 : (10000 / (etGet1 / 1000f));
    float bytesPerSecGet1 = etSecsGet1 == 0 ? 0
        : ((10000 * ENTRY_SIZE) / (etGet1 / 1000f));

    String statsGet1 = "etGet=" + etGet1 + "ms gets/sec=" + opPerSecGet1
        + " bytes/sec=" + bytesPerSecGet1;
    log.info(statsGet1);
    if(debug) System.out.println("Perf Stats of get which is fauting in from H-tree  :"
        + statsGet1);

    //  Perf stats for get op (fauting in from second op log)
    float etGet2 = endTimeGet2 - startTimeGet2;
    float etSecsGet2 = etGet2 / 1000f;
    float opPerSecGet2 = etSecsGet2 == 0 ? 0 : (10000 / (etGet2 / 1000f));
    float bytesPerSecGet2 = etSecsGet2 == 0 ? 0
        : ((10000 * ENTRY_SIZE) / (etGet2 / 1000f));

    String statsGet2 = "etGet=" + etGet2 + "ms gets/sec=" + opPerSecGet2
        + " bytes/sec=" + bytesPerSecGet2;
    log.info(statsGet2);
    if(debug) System.out
        .println("Perf Stats of get which is fauting in from Second OpLog  :"
            + statsGet2);
    DiskRegionTestingBase.unSetCacheObserverCallBack();
  }
/**
 * getLRUStats
 * @param region1
 * @return
 */
  protected LRUStatistics getLRUStats(Region region1)
  {
    return ((LocalRegion)region1).getEvictionController().getLRUHelper()
        .getStats();

  }

}

