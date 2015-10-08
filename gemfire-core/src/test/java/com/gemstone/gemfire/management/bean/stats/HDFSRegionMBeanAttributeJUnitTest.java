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
package com.gemstone.gemfire.management.bean.stats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHDFSQueuePersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.BucketMovedException;
import com.gemstone.gemfire.internal.cache.persistence.soplog.HFileStoreStatistics;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.internal.cache.versions.DiskVersionTag;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

/**
 * Test for verifying HDFS related MBean attributes
 * @author rishim
 *
 */
@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSRegionMBeanAttributeJUnitTest extends TestCase {

  public static final String HDFS_STORE_NAME = "HDFSMBeanJUnitTestStore";
  public static final String REGION_NAME = "HDFSMBeanJUnitTest_Region";
  protected Path testDataDir;
  protected Cache cache;

  protected HDFSStoreFactory hsf;
  protected HDFSStoreImpl hdfsStore;
  protected Region<Object, Object> region;
  SortedOplogStatistics stats;
  HFileStoreStatistics storeStats;
  BlockCache blockCache;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");
    testDataDir = new Path("test-case");

    cache = createCache();

    configureHdfsStoreFactory();
    hdfsStore = (HDFSStoreImpl) hsf.create(HDFS_STORE_NAME);

    RegionFactory<Object, Object> regionfactory = cache.createRegionFactory(RegionShortcut.PARTITION_HDFS);
    regionfactory.setHDFSStoreName(HDFS_STORE_NAME);

    // regionfactory.setCompressionCodec("Some");
    PartitionAttributesFactory fac = new PartitionAttributesFactory();
    fac.setTotalNumBuckets(10);

    regionfactory.setPartitionAttributes(fac.create());
    region = regionfactory.create(REGION_NAME);

  }

  protected void configureHdfsStoreFactory() throws Exception {
    hsf = this.cache.createHDFSStoreFactory();
    hsf.setHomeDir(testDataDir.toString());
  }

  protected Cache createCache() {
    CacheFactory cf = new CacheFactory().set("mcast-port", "0").set("log-level", "info");
    cache = cf.create();
    return cache;
  }

  @Override
  protected void tearDown() throws Exception {
    hdfsStore.getFileSystem().delete(testDataDir, true);
    cache.close();
    super.tearDown();
  }

  public void testStoreUsageStats() throws Exception {

    PartitionedRegion parRegion = (PartitionedRegion)region;
   

      ArrayList<TestEvent> items = new ArrayList<TestEvent>();
      for (int i = 0; i < 100; i++) {
        String key = ("key-" + (i * 100 + i));
        String value = ("value-" + System.nanoTime());
        parRegion.put(key, value);
        
        items.add(new TestEvent(key, value));
      }

    //Dont want to create
    Set<BucketRegion> localPrimaryBucketRegions = parRegion.getDataStore().getAllLocalPrimaryBucketRegions();
    BucketRegion flushingBucket=  localPrimaryBucketRegions.iterator().next();
    HoplogOrganizer hoplogOrganizer = getOrganizer(parRegion,flushingBucket.getId());
    hoplogOrganizer.flush(items.iterator(), 100);
    
    GemFireCacheImpl cache = GemFireCacheImpl.getExisting();
    ManagementService service = ManagementService.getExistingManagementService(cache);
    RegionMXBean bean = service.getLocalRegionMBean(region.getFullPath());
    

    //assertTrue(bean.getEntryCount() == ManagementConstants.ZERO);
    assertTrue(bean.getEntrySize() == ManagementConstants.NOT_AVAILABLE_LONG);
    assertTrue(0 < bean.getDiskUsage());
    
  }
  
  
  private HoplogOrganizer getOrganizer(PartitionedRegion region, int bucketId) {
    BucketRegion br = region.getDataStore().getLocalBucketById(bucketId);
    if (br == null) {
      // got rebalanced or something
      throw new BucketMovedException("Bucket region is no longer available. BucketId: " + 
          bucketId +  " RegionPath: "  +  region.getFullPath());
    }

    return br.getHoplogOrganizer();
  }
 
  
  public static class TestEvent extends SortedHDFSQueuePersistedEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    
    Object key;
    
    public TestEvent(String k, String v) throws Exception {
      this(k, v, Operation.PUT_IF_ABSENT);
    }

    public TestEvent(String k, String v, Operation op) throws Exception {
      super(v, op, (byte) 0x02, false, new DiskVersionTag(), BlobHelper.serializeToBlob(k), 0);
      this.key = k; 
    }
  }


}
