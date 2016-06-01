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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.test.TestUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test is for testing Disk attributes set via xml.
 * 
 * A cache and region are created using an xml. The regions are then verified to make sure
 * that all the attributes have been correctly set
 * 
 * @since GemFire 5.1
 */
@Category(IntegrationTest.class)
public class DiskRegCacheXmlJUnitTest
{
  Cache cache = null;

  DistributedSystem ds = null;

  protected static File[] dirs = null;

  public void mkDirAndConnectDs()
  {
    File file1 = new File("d1");
    file1.mkdir();
    file1.deleteOnExit();
    File file2 = new File("d2");
    file2.mkdir();
    file2.deleteOnExit();
    File file3 = new File("d3");
    file3.mkdir();
    file3.deleteOnExit();
    dirs = new File[3];
    dirs[0] = file1;
    dirs[1] = file2;
    dirs[2] = file3;
    // Connect to the GemFire distributed system
    Properties props = new Properties();
    props.setProperty(NAME, "test");
    String path = TestUtil.getResourcePath(getClass(), "DiskRegCacheXmlJUnitTest.xml");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(CACHE_XML_FILE, path);
    ds = DistributedSystem.connect(props);
    try {
      // Create the cache which causes the cache-xml-file to be parsed
      cache = CacheFactory.create(ds);
    }
    catch (Exception ex) {
      ds.getLogWriter().error("Exception occured",ex);
      fail("failed to create cache due to "+ex);
    }
  }

  @Test
  public void testDiskRegCacheXml()
  {
    mkDirAndConnectDs();
    // Get the region1 which is a subregion of /root
    Region region1 = cache.getRegion("/root1/PersistSynchRollingOplog1");
    DiskStore ds = ((LocalRegion)region1).getDiskStore();
    if (ds != null) {
      if (!Arrays.equals(dirs, ds.getDiskDirs())) {
        fail("expected=" + Arrays.toString(dirs)
             + " actual=" + ds.getDiskDirs());
      }
    } else {
      if (!Arrays.equals(dirs, region1.getAttributes().getDiskDirs())) {
        fail("expected=" + Arrays.toString(dirs)
            + " actual=" + region1.getAttributes().getDiskDirs());
      }
    }

    RegionAttributes ra1 = ((LocalRegion)region1).getAttributes();
    assertTrue(ra1.isDiskSynchronous() == true);
    DiskStore ds1 = cache.findDiskStore(((LocalRegion)region1).getDiskStoreName());
    assertTrue(ds1 != null);
    assertTrue(ds1.getAutoCompact() == true);
    assertTrue(ds1.getMaxOplogSize() == 2);
    
    // Get the region2 which is a subregion of /root
    Region region2 = cache.getRegion("/root2/PersistSynchFixedOplog2");
    RegionAttributes ra2 = ((LocalRegion)region2).getAttributes();
    assertTrue(ra2.isDiskSynchronous() == true);
    DiskStore ds2 = cache.findDiskStore(((LocalRegion)region2).getDiskStoreName());
    assertTrue(ds2 != null);
    assertTrue(ds2.getAutoCompact() == false);
    assertTrue(ds2.getMaxOplogSize() == 0);

    // Get the region3 which is a subregion of /root
    Region region3 = cache.getRegion("/root3/PersistASynchBufferRollingOplog3");
    RegionAttributes ra3 = ((LocalRegion)region3).getAttributes();
    assertTrue(ra3.isDiskSynchronous() == false);
    DiskStore ds3 = cache.findDiskStore(((LocalRegion)region3).getDiskStoreName());
    assertTrue(ds3 != null);
    assertTrue(ds3.getAutoCompact() == true);
    assertTrue(ds3.getMaxOplogSize() == 2);
    assertTrue(ds3.getQueueSize() == 10000);
    assertTrue(ds3.getTimeInterval() == 15);

    // Get the region4 which is a subregion of /root
    Region region4 = cache.getRegion("/root4/PersistASynchNoBufferFixedOplog4");
    RegionAttributes ra4 = ((LocalRegion)region4).getAttributes();
    assertTrue(ra4.isDiskSynchronous() == false);
    DiskStore ds4 = cache.findDiskStore(((LocalRegion)region4).getDiskStoreName());
    assertTrue(ds4 != null);
    assertTrue(ds4.getAutoCompact() == false);
    assertTrue(ds4.getMaxOplogSize() == 2);
    assertTrue(ds4.getQueueSize() == 0);

    // Get the region5 which is a subregion of /root
    Region region5 = cache.getRegion("/root5/OverflowSynchRollingOplog5");
    RegionAttributes ra5 = ((LocalRegion)region5).getAttributes();
    assertTrue(ra5.isDiskSynchronous() == true);
    DiskStore ds5 = cache.findDiskStore(((LocalRegion)region5).getDiskStoreName());
    assertTrue(ds5 != null);
    assertTrue(ds5.getAutoCompact() == true);
    assertTrue(ds5.getMaxOplogSize() == 2);

    // Get the region6 which is a subregion of /root
    Region region6 = cache.getRegion("/root6/OverflowSynchFixedOplog6");
    RegionAttributes ra6 = ((LocalRegion)region6).getAttributes();
    assertTrue(ra6.isDiskSynchronous() == true);
    DiskStore ds6 = cache.findDiskStore(((LocalRegion)region6).getDiskStoreName());
    assertTrue(ds6 != null);
    assertTrue(ds6.getAutoCompact() == false);
    assertTrue(ds6.getMaxOplogSize() == 0);

    // Get the region7 which is a subregion of /root
    Region region7 = cache
        .getRegion("/root7/OverflowASynchBufferRollingOplog7");
    RegionAttributes ra7 = ((LocalRegion)region7).getAttributes();
    assertTrue(ra7.isDiskSynchronous() == false);
    DiskStore ds7 = cache.findDiskStore(((LocalRegion)region7).getDiskStoreName());
    assertTrue(ds7 != null);
    assertTrue(ds7.getAutoCompact() == true);
    assertTrue(ds7.getMaxOplogSize() == 2);

    // Get the region8 which is a subregion of /root
    Region region8 = cache
        .getRegion("/root8/OverflowASynchNoBufferFixedOplog8");
    RegionAttributes ra8 = ((LocalRegion)region8).getAttributes();
    assertTrue(ra8.isDiskSynchronous() == false);
    DiskStore ds8 = cache.findDiskStore(((LocalRegion)region8).getDiskStoreName());
    assertTrue(ds8 != null);
    assertTrue(ds8.getAutoCompact() == false);

    // Get the region9 which is a subregion of /root
    Region region9 = cache
        .getRegion("/root9/PersistOverflowSynchRollingOplog9");
    RegionAttributes ra9 = ((LocalRegion)region9).getAttributes();
    assertTrue(ra9.isDiskSynchronous() == true);
    DiskStore ds9 = cache.findDiskStore(((LocalRegion)region9).getDiskStoreName());
    assertTrue(ds9 != null);
    assertTrue(ds9.getAutoCompact() == true);
    assertTrue(ds9.getMaxOplogSize() == 2);

    // Get the region10 which is a subregion of /root
    Region region10 = cache
        .getRegion("/root10/PersistOverflowSynchFixedOplog10");
    RegionAttributes ra10 = ((LocalRegion)region10).getAttributes();
    assertTrue(ra10.isDiskSynchronous() == true);
    DiskStore ds10 = cache.findDiskStore(((LocalRegion)region10).getDiskStoreName());
    assertTrue(ds10 != null);
    assertTrue(ds10.getAutoCompact() == false);

    // Get the region11 which is a subregion of /root
    Region region11 = cache
        .getRegion("/root11/PersistOverflowASynchBufferRollingOplog11");
    RegionAttributes ra11 = ((LocalRegion)region11).getAttributes();
    assertTrue(ra11.isDiskSynchronous() == false);
    DiskStore ds11 = cache.findDiskStore(((LocalRegion)region11).getDiskStoreName());
    assertTrue(ds11 != null);
    assertTrue(ds11.getAutoCompact() == true);
    assertTrue(ds11.getMaxOplogSize() == 2);

    // Get the region12 which is a subregion of /root
    Region region12 = cache
        .getRegion("/root12/PersistOverflowASynchNoBufferFixedOplog12");
    RegionAttributes ra12 = ((LocalRegion)region12).getAttributes();
    assertTrue(ra12.isDiskSynchronous() == false);
    DiskStore ds12 = cache.findDiskStore(((LocalRegion)region12).getDiskStoreName());
    assertTrue(ds12 != null);
    assertTrue(ds12.getTimeInterval() == 15);
    assertTrue(ds12.getQueueSize() == 0);

    deleteFiles();
  }

  private static void deleteFiles()
  {
    for (int i = 0; i < dirs.length; i++) {
      File[] files = dirs[i].listFiles();
      for (int j = 0; j < files.length; j++) {
        files[j].delete();
      }
    }

  }
  
  
  @After
  public void tearDown() throws Exception
  {

    /*if (cache != null && !cache.isClosed()) {
      cache.close();
    }*/
    try {
      if (cache != null && !cache.isClosed()) {
        for (Iterator itr = cache.rootRegions().iterator(); itr.hasNext();) {
          Region root = (Region)itr.next();
//          String name = root.getName();
					if(root.isDestroyed() || root instanceof HARegion) {
            continue;
        	}
          try {
            root.localDestroyRegion("teardown");
          }
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable t) {
            ds.getLogWriter().error(t);
          }
        }
      }
    }
    finally {
      try {
        closeCache();
      }
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        ds.getLogWriter().error("Error in closing the cache ", t);
        
      }
    }
  }
  
  
  /** Close the cache */
  private  synchronized final void closeCache() {
    if (cache != null) {
      try {
        if (!cache.isClosed()) {
          CacheTransactionManager txMgr = cache.getCacheTransactionManager();
          if (txMgr != null) {
            if (txMgr.exists()) {
              // make sure we cleanup this threads txid stored in a thread local
              txMgr.rollback();
            }
          }
          cache.close();
        }
      } finally {
        cache = null;
      }
    }
  }
}// end of DiskRegCacheXmlJUnitTest

