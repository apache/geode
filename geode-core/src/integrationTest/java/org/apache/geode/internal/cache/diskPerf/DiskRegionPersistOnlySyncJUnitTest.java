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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.DiskRegionHelperFactory;
import org.apache.geode.internal.cache.DiskRegionProperties;
import org.apache.geode.internal.cache.DiskRegionTestingBase;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Disk region perf test for Persist only with sync writes.
 */
public class DiskRegionPersistOnlySyncJUnitTest extends DiskRegionTestingBase {

  private LogWriter log = null;

  private final DiskRegionProperties diskProps = new DiskRegionProperties();

  @Override
  protected final void postSetUp() throws Exception {
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    log = ds.getLogWriter();
  }

  private static int ENTRY_SIZE = 1024;

  /*
   * OP_COUNT can be increased/decrease as per the requirement. If required to be set as higher
   * value such as 1000000 one needs to set the VM heap size accordingly. (For example:Default
   * setting in build.xml is <jvmarg value="-Xmx256M"/>
   */
  private static int OP_COUNT = 1000;

  private static boolean UNIQUE_KEYS = Boolean.getBoolean("DRP.UNIQUE_KEYS");

  @Test
  public void testPopulate1kbwrites() {
    RegionAttributes ra = region.getAttributes();
    // final String key = "K";
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte) 77);
    String config = "ENTRY_SIZE=" + ENTRY_SIZE + " OP_COUNT=" + OP_COUNT + " UNIQUE_KEYS="
        + UNIQUE_KEYS + " opLogEnabled="
        + !Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "disableOpLog") + " syncWrites="
        + Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "syncWrites");
    if (ra.getDiskStoreName() != null) {
      config += " diskStoreName=" + ra.getDiskStoreName();
    } else {
      config += " [" + ra.getDiskWriteAttributes() + "]";
    }
    log.info(config);
    // for recovery test:
    region.put("5", "5");
    region.put("3000", "3000");
    region.put("7000", "7000");
    region.put("100", "100");
    region.put("9999", "9999");
    region.put("794", "794");
    region.put("123", "123");
    region.put("4768", "4768");
    region.put("987", "987");

    long startTime = System.currentTimeMillis();
    if (UNIQUE_KEYS) {
      for (int i = 0; i < OP_COUNT; i++) {

        region.put(i, value);
      }
    } else {
      for (int i = 0; i < OP_COUNT; i++) {
        region.put("" + (i + 10000), value);
      }
    }

    long endTime = System.currentTimeMillis();
    long et = endTime - startTime;
    long etSecs = et / 1000;
    long opPerSec = etSecs == 0 ? 0 : (OP_COUNT / (et / 1000));
    long bytesPerSec = etSecs == 0 ? 0 : ((OP_COUNT * ENTRY_SIZE) / (et / 1000));

    String stats = "et=" + et + "ms writes/sec=" + opPerSec + " bytes/sec=" + bytesPerSec;
    log.info(stats);
    System.out.println("Stats for 1kb writes:" + stats);
    // close the cache after that create it again and then get the values to
    // test recovery.
    if (cache != null) {
      cache.close();
      System.out.println("Cache closed");
    }

    // create the cache
    try {
      cache = createCache();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      System.out.println("Cache created to test the recovery..");
    } catch (Exception e) {
      e.printStackTrace();
      fail("failed while creating the cache ");
    }
    // Get all the test values:
    System.out.println(region.get("5"));
    System.out.println(region.get("3000"));
    System.out.println(region.get("7000"));
    System.out.println(region.get("100"));
    System.out.println(region.get("9999"));
    System.out.println(region.get("794"));
    System.out.println(region.get("123"));
    System.out.println(region.get("4768"));
    System.out.println(region.get("987"));
    // Verifying the get operation:
    assertTrue((region.get("5").toString()).equals("5"));
    assertTrue((region.get("3000").toString()).equals("3000"));
    assertTrue((region.get("7000").toString()).equals("7000"));
    assertTrue((region.get("100").toString()).equals("100"));
    assertTrue((region.get("9999").toString()).equals("9999"));
    assertTrue((region.get("794").toString()).equals("794"));
    assertTrue((region.get("123").toString()).equals("123"));
    assertTrue((region.get("4768").toString()).equals("4768"));
    assertTrue((region.get("987").toString()).equals("987"));
    closeDown();
  }

  @Test
  public void testPopulate5kbwrites() {
    ENTRY_SIZE = 1024 * 5;

    /*
     * OP_COUNT can be increased/decrease as per the requirement. If required to be set as higher
     * value such as 1000000 one needs to set the VM heap size accordingly. (For example:Default
     * setting in build.xml is <jvmarg value="-Xmx256M"/>
     *
     */
    OP_COUNT = 1000;
    UNIQUE_KEYS = Boolean.getBoolean("DRP.UNIQUE_KEYS");
    RegionAttributes ra = region.getAttributes();
    // final String key = "K";
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte) 77);
    String config = "ENTRY_SIZE=" + ENTRY_SIZE + " OP_COUNT=" + OP_COUNT + " UNIQUE_KEYS="
        + UNIQUE_KEYS + " opLogEnabled="
        + !Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "disableOpLog") + " syncWrites="
        + Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "syncWrites");
    if (ra.getDiskStoreName() != null) {
      config += " diskStoreName=" + ra.getDiskStoreName();
    } else {
      config += " [" + ra.getDiskWriteAttributes() + "]";
    }
    log.info(config);

    long startTime = System.currentTimeMillis();
    if (UNIQUE_KEYS) {
      for (int i = 0; i < OP_COUNT; i++) {

        region.put(i, value);
      }
    } else {
      for (int i = 0; i < OP_COUNT; i++) {
        region.put("" + (i + 10000), value);
      }
    }

    long endTime = System.currentTimeMillis();

    // region.close(); // closes disk file which will flush all buffers
    ((LocalRegion) region).forceFlush();

    long et = endTime - startTime;
    long etSecs = et / 1000;
    long opPerSec = etSecs == 0 ? 0 : (OP_COUNT / (et / 1000));
    long bytesPerSec = etSecs == 0 ? 0 : ((OP_COUNT * ENTRY_SIZE) / (et / 1000));

    String stats = "et=" + et + "ms writes/sec=" + opPerSec + " bytes/sec=" + bytesPerSec;
    log.info(stats);
    System.out.println("Stats for 5kb writes :" + stats);
  }

}
