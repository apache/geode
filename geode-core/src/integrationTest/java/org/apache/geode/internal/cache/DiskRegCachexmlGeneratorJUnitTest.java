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

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;

/**
 * This test is for testing Disk attributes set programmatically The generated cacheXml is used to
 * create a cache and the region properties retested.
 */
public class DiskRegCachexmlGeneratorJUnitTest extends DiskRegionTestingBase {

  private final DiskRegionProperties[] diskRegionProperties = new DiskRegionProperties[12];

  private final Region[] regions = new Region[12];

  @Override
  protected final void postSetUp() throws Exception {
    diskDirSize = new int[4];
    diskDirSize[0] = Integer.MAX_VALUE;
    diskDirSize[1] = Integer.MAX_VALUE;
    diskDirSize[2] = 1073741824;
    diskDirSize[3] = 2073741824;

    for (int i = 0; i < diskRegionProperties.length; i++) {
      diskRegionProperties[i] = new DiskRegionProperties();
      if (i == 0) {
        diskRegionProperties[i].setDiskDirsAndSizes(dirs, diskDirSize);
      } else {
        diskRegionProperties[i].setDiskDirs(dirs);
      }
    }
  }

  public void createCacheXML() throws IOException {
    // create the regions[0] which is SyncPersistOnly and set DiskWriteAttibutes
    diskRegionProperties[0].setRolling(true);
    diskRegionProperties[0].setMaxOplogSize(1073741824L);
    diskRegionProperties[0].setRegionName("regions1");
    regions[0] = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskRegionProperties[0],
        Scope.LOCAL);

    // create the regions[1] which is SyncPersistOnly and set DiskWriteAttibutes

    diskRegionProperties[1].setRolling(false);
    diskRegionProperties[1].setRegionName("regions2");
    regions[1] = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskRegionProperties[1],
        Scope.LOCAL);

    // create the regions[2] which AsyncPersistOnly, No buffer and Rolling oplog
    diskRegionProperties[2].setRolling(true);
    diskRegionProperties[2].setMaxOplogSize(1073741824L);
    diskRegionProperties[2].setRegionName("regions3");
    regions[2] = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties[2]);

    // create the regions[3] which is AsynchPersistonly, No buffer and fixed oplog
    diskRegionProperties[3].setRolling(false);
    diskRegionProperties[3].setRegionName("regions4");
    regions[3] = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties[3]);

    // create the regions[4] which is SynchOverflowOnly, Rolling oplog
    diskRegionProperties[4].setRolling(true);
    diskRegionProperties[4].setMaxOplogSize(1073741824L);
    diskRegionProperties[4].setRegionName("regions5");
    regions[4] = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskRegionProperties[4]);

    // create the regions[5] which is SyncOverflowOnly, Fixed oplog
    diskRegionProperties[5].setRolling(false);
    diskRegionProperties[5].setRegionName("regions6");
    regions[5] = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskRegionProperties[5]);

    // create the regions[6] which is AsyncOverflow, with Buffer and rolling oplog
    diskRegionProperties[6].setRolling(true);
    diskRegionProperties[6].setMaxOplogSize(1073741824L);
    diskRegionProperties[6].setBytesThreshold(10000l);
    diskRegionProperties[6].setTimeInterval(15l);
    diskRegionProperties[6].setRegionName("regions7");
    regions[6] = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskRegionProperties[6]);

    // create the regions[7] which is AsyncOverflow ,Time base buffer-zero byte
    // buffer
    // and Fixed oplog
    diskRegionProperties[7].setRolling(false);
    diskRegionProperties[7].setTimeInterval(15l);
    diskRegionProperties[7].setBytesThreshold(0l);
    diskRegionProperties[7].setRegionName("regions8");
    regions[7] = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskRegionProperties[7]);

    // create the regions[8] which is SyncPersistOverflow, Rolling oplog
    diskRegionProperties[8].setRolling(true);
    diskRegionProperties[8].setMaxOplogSize(1073741824L);
    diskRegionProperties[8].setRegionName("regions9");
    regions[8] =
        DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskRegionProperties[8]);

    // create the regions[9] which is Sync PersistOverflow, fixed oplog
    diskRegionProperties[9].setRolling(false);
    diskRegionProperties[9].setRegionName("regions10");
    regions[9] =
        DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskRegionProperties[9]);
    // create the regions[10] which is Async Overflow Persist ,with buffer and
    // rollong
    // oplog
    diskRegionProperties[10].setRolling(true);
    diskRegionProperties[10].setMaxOplogSize(1073741824L);
    diskRegionProperties[10].setBytesThreshold(10000l);
    diskRegionProperties[10].setTimeInterval(15l);
    diskRegionProperties[10].setRegionName("regions11");
    regions[10] =
        DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, diskRegionProperties[10]);

    // create the regions[11] which is Async Persist Overflow with time based
    // buffer
    // and Fixed oplog
    diskRegionProperties[11].setRolling(false);
    diskRegionProperties[11].setBytesThreshold(0l);
    diskRegionProperties[11].setTimeInterval(15l);
    diskRegionProperties[11].setRegionName("regions12");
    regions[11] =
        DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, diskRegionProperties[11]);

    // cacheXmlGenerator: generates cacheXml file
    FileWriter fw = new FileWriter(new File(getClass().getSimpleName() + ".xml"));
    PrintWriter pw = new PrintWriter(fw);
    CacheXmlGenerator.generate(cache, pw);
  }

  @Test
  public void testVerifyCacheXml() throws Exception {
    createCacheXML();
    ds.disconnect();
    // Connect to the GemFire distributed system
    Properties props = new Properties();
    props.setProperty(NAME, getClass().getSimpleName());
    props.setProperty(MCAST_PORT, "0");
    String path = getClass().getSimpleName() + ".xml";
    props.setProperty(CACHE_XML_FILE, path);
    ds = DistributedSystem.connect(props);
    // Create the cache which causes the cache-xml-file to be parsed
    cache = CacheFactory.create(ds);

    // Get the regions[0]
    verify((LocalRegion) cache.getRegion("regions1"), diskRegionProperties[0]);

    // Get the regions[1]
    verify((LocalRegion) cache.getRegion("regions2"), diskRegionProperties[1]);

    // Get the regions[2]
    verify((LocalRegion) cache.getRegion("regions3"), diskRegionProperties[2]);

    // Get the regions[3]
    verify((LocalRegion) cache.getRegion("regions4"), diskRegionProperties[3]);

    // Get the regions[4]
    verify((LocalRegion) cache.getRegion("regions5"), diskRegionProperties[4]);

    // Get the regions[5]
    verify((LocalRegion) cache.getRegion("regions6"), diskRegionProperties[5]);

    // Get the regions[6]
    verify((LocalRegion) cache.getRegion("regions7"), diskRegionProperties[6]);

    // Get the regions[7]
    verify((LocalRegion) cache.getRegion("regions8"), diskRegionProperties[7]);

    // Get the regions[8]
    verify((LocalRegion) cache.getRegion("regions9"), diskRegionProperties[8]);

    // Get the regions[9]
    verify((LocalRegion) cache.getRegion("regions10"), diskRegionProperties[9]);

    // Get the regions[10]
    verify((LocalRegion) cache.getRegion("regions11"), diskRegionProperties[10]);

    // Get the regions[11]
    verify((LocalRegion) cache.getRegion("regions12"), diskRegionProperties[11]);
  }

}
