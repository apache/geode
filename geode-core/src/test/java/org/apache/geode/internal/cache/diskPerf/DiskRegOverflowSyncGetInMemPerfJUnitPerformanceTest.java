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

import java.util.Arrays;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.cache.DiskRegionHelperFactory;
import org.apache.geode.internal.cache.DiskRegionProperties;
import org.apache.geode.internal.cache.DiskRegionTestingBase;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Disk region Perf test for Overflow only with Sync writes. 1) Performance of get operation for
 * entry in memory.
 */
@Category(IntegrationTest.class)
public class DiskRegOverflowSyncGetInMemPerfJUnitPerformanceTest extends DiskRegionTestingBase {

  private static int ENTRY_SIZE = 1024;

  private static int OP_COUNT = 10000;

  private static int counter = 0;

  private LogWriter log = null;

  private DiskRegionProperties diskProps = new DiskRegionProperties();

  @Override
  protected final void preSetUp() throws Exception {
    diskProps.setDiskDirs(dirs);
  }

  @Override
  protected final void postSetUp() throws Exception {
    diskProps.setOverFlowCapacity(100000);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);

    log = ds.getLogWriter();
  }

  @Override
  protected final void postTearDown() throws Exception {
    if (cache != null) {
      cache.close();
    }
    if (ds != null) {
      ds.disconnect();
    }
  }

  @Test
  public void testPopulatefor1Kbwrites() {
    // RegionAttributes ra = region.getAttributes();
    // final String key = "K";
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte) 77);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < OP_COUNT; i++) {
      region.put("" + (i + 10000), value);
    }
    long endTime = System.currentTimeMillis();
    System.out.println(" done with putting");
    // Now get all the entries which are in memory.
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
    float bytesPerSec = etSecs == 0 ? 0 : ((OP_COUNT * ENTRY_SIZE) / (et / 1000f));

    String stats = "et=" + et + "ms writes/sec=" + opPerSec + " bytes/sec=" + bytesPerSec;
    log.info(stats);
    System.out.println("Stats for 1 kb writes:" + stats);
    // Perf stats for get op
    float etGet = endTimeGet - startTimeGet;
    float etSecsGet = etGet / 1000f;
    float opPerSecGet = etSecsGet == 0 ? 0 : (OP_COUNT / (etGet / 1000f));
    float bytesPerSecGet = etSecsGet == 0 ? 0 : ((OP_COUNT * ENTRY_SIZE) / (etGet / 1000f));

    String statsGet = "et=" + etGet + "ms gets/sec=" + opPerSecGet + " bytes/sec=" + bytesPerSecGet;
    log.info(statsGet);
    System.out.println("Perf Stats of get which is in memory :" + statsGet);
  }
}
