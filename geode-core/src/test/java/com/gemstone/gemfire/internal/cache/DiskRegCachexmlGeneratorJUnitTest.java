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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.xmlcache.*;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This test is for testing Disk attributes set programmatically
 * The generated cacheXml is used to create a cache and teh region
 * properties retested.
 * 
 */
@Category(IntegrationTest.class)
public class DiskRegCachexmlGeneratorJUnitTest extends DiskRegionTestingBase
{
  PrintWriter pw;

  DiskRegionProperties diskProps = new DiskRegionProperties();

  DiskRegionProperties diskProps1 = new DiskRegionProperties();

  DiskRegionProperties diskProps2 = new DiskRegionProperties();

  DiskRegionProperties diskProps3 = new DiskRegionProperties();

  DiskRegionProperties diskProps4 = new DiskRegionProperties();

  DiskRegionProperties diskProps5 = new DiskRegionProperties();

  DiskRegionProperties diskProps6 = new DiskRegionProperties();

  DiskRegionProperties diskProps7 = new DiskRegionProperties();

  DiskRegionProperties diskProps8 = new DiskRegionProperties();

  DiskRegionProperties diskProps9 = new DiskRegionProperties();

  DiskRegionProperties diskProps10 = new DiskRegionProperties();

  DiskRegionProperties diskProps11 = new DiskRegionProperties();

  DiskRegionProperties diskProps12 = new DiskRegionProperties();

  Region region1;

  Region region2;

  Region region3;

  Region region4;

  Region region5;

  Region region6;

  Region region7;

  Region region8;

  Region region9;

  Region region10;

  Region region11;

  Region region12;

  @Before
  public void setUp() throws Exception
  {
    super.setUp();
    diskDirSize = new int[4];
    diskDirSize[0] = Integer.MAX_VALUE;
    diskDirSize[1] = Integer.MAX_VALUE;
    diskDirSize[2] = 1073741824;
    diskDirSize[3] = 2073741824;
    diskProps1.setDiskDirsAndSizes(dirs, diskDirSize);
    diskProps2.setDiskDirs(dirs);
    diskProps3.setDiskDirs(dirs);
    diskProps4.setDiskDirs(dirs);
    diskProps5.setDiskDirs(dirs);
    diskProps6.setDiskDirs(dirs);
    diskProps7.setDiskDirs(dirs);
    diskProps8.setDiskDirs(dirs);
    diskProps9.setDiskDirs(dirs);
    diskProps10.setDiskDirs(dirs);
    diskProps11.setDiskDirs(dirs);
    diskProps12.setDiskDirs(dirs);
  }

  @After
  public void tearDown() throws Exception
  {
    super.tearDown();

  }

  public void createCacheXML()
  {
    // create the region1 which is SyncPersistOnly and set DiskWriteAttibutes
    diskProps1.setRolling(true);
    diskProps1.setMaxOplogSize(1073741824L);
    diskProps1.setRegionName("region1");
    region1 = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps1, Scope.LOCAL);

    // create the region2 which is SyncPersistOnly and set DiskWriteAttibutes

    diskProps2.setRolling(false);
    diskProps2.setRegionName("region2");
    region2 = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps2, Scope.LOCAL);

    // create the region3 which AsyncPersistOnly, No buffer and Rolling oplog
    diskProps3.setRolling(true);
    diskProps3.setMaxOplogSize(1073741824L);
    diskProps3.setRegionName("region3");
    region3 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskProps3);

    // create the region4 which is AsynchPersistonly, No buffer and fixed oplog
    diskProps4.setRolling(false);
    diskProps4.setRegionName("region4");
    region4 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskProps4);

    // create the region5 which is SynchOverflowOnly, Rolling oplog
    diskProps5.setRolling(true);
    diskProps5.setMaxOplogSize(1073741824L);
    diskProps5.setRegionName("region5");
    region5 = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache,
        diskProps5);

    // create the region6 which is SyncOverflowOnly, Fixed oplog
    diskProps6.setRolling(false);
    diskProps6.setRegionName("region6");
    region6 = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache,
        diskProps6);

    // create the region7 which is AsyncOverflow, with Buffer and rolling oplog
    diskProps7.setRolling(true);
    diskProps7.setMaxOplogSize(1073741824L);
    diskProps7.setBytesThreshold(10000l);
    diskProps7.setTimeInterval(15l);
    diskProps7.setRegionName("region7");
    region7 = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache,
        diskProps7);

    // create the region8 which is AsyncOverflow ,Time base buffer-zero byte
    // buffer
    // and Fixed oplog
    diskProps8.setRolling(false);
    diskProps8.setTimeInterval(15l);
    diskProps8.setBytesThreshold(0l);
    diskProps8.setRegionName("region8");
    region8 = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache,
        diskProps8);

    // create the region9 which is SyncPersistOverflow, Rolling oplog
    diskProps9.setRolling(true);
    diskProps9.setMaxOplogSize(1073741824L);
    diskProps9.setRegionName("region9");
    region9 = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps9);

    // create the region10 which is Sync PersistOverflow, fixed oplog
    diskProps10.setRolling(false);
    diskProps10.setRegionName("region10");
    region10 = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps10);
    // create the region11 which is Async Overflow Persist ,with buffer and
    // rollong
    // oplog
    diskProps11.setRolling(true);
    diskProps11.setMaxOplogSize(1073741824L);
    diskProps11.setBytesThreshold(10000l);
    diskProps11.setTimeInterval(15l);
    diskProps11.setRegionName("region11");
    region11 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        diskProps11);

    // create the region12 which is Async Persist Overflow with time based
    // buffer
    // and Fixed oplog
    diskProps12.setRolling(false);
    diskProps12.setBytesThreshold(0l);
    diskProps12.setTimeInterval(15l);
    diskProps12.setRegionName("region12");
    region12 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        diskProps12);


    //cacheXmlGenerator: generates cacheXml file
    try {
      FileWriter fw = new FileWriter(new File("DiskRegCachexmlGeneratorJUnitTest.xml"));
      PrintWriter pw = new PrintWriter(fw);
      CacheXmlGenerator.generate(cache, pw);
    }
    catch (Exception ex) {
      logWriter.error("Exception occured",ex);
      fail("FAILED While cache xml generation");
    }

  }
  
  @Test
  public void testVerifyCacheXml() throws Exception
  {
    createCacheXML();
    ds.disconnect();
    // Connect to the GemFire distributed system
    Properties props = new Properties();
    props.setProperty(DistributionConfig.NAME_NAME, "DiskRegCachexmlGeneratorJUnitTest");
    props.setProperty("mcast-port", "0");
    String path = "DiskRegCachexmlGeneratorJUnitTest.xml";
    props.setProperty("cache-xml-file", path);
    ds = DistributedSystem.connect(props);
    // Create the cache which causes the cache-xml-file to be parsed
    cache = CacheFactory.create(ds);

    // Get the region1 
    region1 = cache.getRegion("region1");
    verify((LocalRegion)region1, diskProps1);

    // Get the region2
    Region region2 = cache.getRegion("region2");
    verify((LocalRegion)region2, diskProps2);

    // Get the region3 
    Region region3 = cache.getRegion("region3");
    verify((LocalRegion)region3, diskProps3);

    // Get the region4 
    Region region4 = cache.getRegion("region4");
    verify((LocalRegion)region4, diskProps4);
    
    // Get the region5 
    Region region5 = cache.getRegion("region5");
    verify((LocalRegion)region5, diskProps5);

    // Get the region6 
    Region region6 = cache.getRegion("region6");
    verify((LocalRegion)region6, diskProps6);
    
    // Get the region7 
    Region region7 = cache.getRegion("region7");
    verify((LocalRegion)region7, diskProps7);

    // Get the region8 
    Region region8 = cache.getRegion("region8");
    verify((LocalRegion)region8, diskProps8);

    // Get the region9 
    Region region9 = cache.getRegion("region9");
    verify((LocalRegion)region9, diskProps9);

    // Get the region10 
    Region region10 = cache.getRegion("region10");
    verify((LocalRegion)region10, diskProps10);

    // Get the region11
    Region region11 = cache.getRegion("region11");
    verify((LocalRegion)region11, diskProps11);

    // Get the region12 
    Region region12 = cache.getRegion("region12");
    verify((LocalRegion)region12, diskProps12);
  }

}// end of DiskRegCachexmlGeneratorJUnitTest

