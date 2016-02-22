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
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheXmlException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This test tests Illegal arguements being passed to 
 * create disk regions. The creation of the DWA object should
 * throw a relevant exception if the arguements specified are incorrect.
 * 
 *
 */
@Category(IntegrationTest.class)
public class DiskRegionIllegalCacheXMLvaluesJUnitTest
{

  public void createRegion(String path)
  {
    DistributedSystem ds = null;
    try {
      boolean exceptionOccured = false;
      File dir = new File("testingDirectoryForXML");
      dir.mkdir();
      dir.deleteOnExit();
      Properties props = new Properties();
      props.setProperty("mcast-port", "0");
      props.setProperty("cache-xml-file", TestUtil.getResourcePath(getClass(), path));
      ds = DistributedSystem.connect(props);
      try {
       
        CacheFactory.create(ds);
      }
      catch (IllegalArgumentException ex) {
        exceptionOccured = true;
        System.out.println("ExpectedStrings: Received expected IllegalArgumentException:"+ex.getMessage());
      }
      catch (CacheXmlException ex) {
         exceptionOccured = true;
         System.out.println("ExpectedStrings: Received expected CacheXmlException:"+ex.getMessage());
      }
      catch (Exception e) {
        e.printStackTrace();
        fail("test failed due to " + e);
      }

      if (!exceptionOccured) {
        fail(" exception did not occur although was expected");
      }
    }
    finally {
      if (ds != null && ds.isConnected()) {
        ds.disconnect();
        ds = null;
      }
    }
  }
 
  
  /**
   * test Illegal max oplog size
   */

  @Test
  public void testMaxOplogSize()
  {
    createRegion("faultyDiskXMLsForTesting/incorrect_max_oplog_size.xml");
  }

  @Test
  public void testSynchronous()
  {}

  @Test
  public void testIsRolling()
  {
    createRegion("faultyDiskXMLsForTesting/incorrect_roll_oplogs_value.xml");
  }

  @Test
  public void testDiskDirSize()
  {
    createRegion("faultyDiskXMLsForTesting/incorrect_dir_size.xml");
  }

  @Test
  public void testDiskDirs()
  {
    createRegion("faultyDiskXMLsForTesting/incorrect_dir.xml");
  }

  @Test
  public void testBytesThreshold()
  {
    createRegion("faultyDiskXMLsForTesting/incorrect_bytes_threshold.xml");
  }

  @Test
  public void testTimeInterval()
  {
    createRegion("faultyDiskXMLsForTesting/incorrect_time_interval.xml");
  }

  @Test
  public void testMixedDiskStoreWithDiskDir()
  {
    createRegion("faultyDiskXMLsForTesting/mixed_diskstore_diskdir.xml");
  }
  @Test
  public void testMixedDiskStoreWithDWA()
  {
    createRegion("faultyDiskXMLsForTesting/mixed_diskstore_diskwriteattrs.xml");
  }
}
