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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.*;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class PersistentPartitionedRegionJUnitTest {

  private File dir;
  private Cache cache;

  @Before
  public void setUp() throws Exception {
    dir = new File("diskDir");
    dir.mkdir();
  }
  
  @After
  public void tearDown() throws Exception {
    if(cache != null && !cache.isClosed()) {
      cache.close();
    }
    FileUtil.delete(dir);
  }
  
  @Test
  public void testChangeTTL() throws InterruptedException { 
    Region region = createRegion(-1);
    region.put("A", "B");
    cache.close();
    region = createRegion(60000);
    assertEquals("B", region.get("A"));
  }
  
  @Test
  public void testStatsPersistAttributesChangeNoRecovery() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      PartitionedRegion region = (PartitionedRegion) createRegion(-1);
      region.put("A", "B");
      cache.close();
      region = (PartitionedRegion) createRegion(60000);
      
      BucketRegion bucket = region.getBucketRegion("A");
      assertEquals(1, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(0, bucket.getDiskRegion().getStats().getNumEntriesInVM());
      assertEquals("B", region.get("A"));
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    }
  }
  
  @Test
  public void testStatsPersistAttributesChangeSyncRecovery() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "true");
    try {
      PartitionedRegion region = (PartitionedRegion) createRegion(-1);
      region.put("A", "B");
      cache.close();
      region = (PartitionedRegion) createRegion(60000);
      
      BucketRegion bucket = region.getBucketRegion("A");
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
      assertEquals("B", region.get("A"));
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "false");
    }
  }
  
  @Test
  public void testStatsPersistAttributesChangeAsyncRecovery() throws InterruptedException {
    PartitionedRegion region = (PartitionedRegion) createRegion(-1);
    for(int i=0; i < 1000; i++) {
      region.put(i, "B");
    }
    cache.close();
    region = (PartitionedRegion) createRegion(60000);

    BucketRegion bucket = region.getBucketRegion("A");
    for(int i=0; i < 1000; i++) {
      region.get(i);
    }
    //There is a race where the async value recovery thread may be handling
    //the stats for the last entry (even though it is already faulted in)
    int count =0;
    while(0 != bucket.getDiskRegion().getStats().getNumOverflowOnDisk()) {
      Thread.sleep(50);
      if(++count==20) {
        break;
      }
    }
    assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
    assertEquals(1000, bucket.getDiskRegion().getStats().getNumEntriesInVM());
  }
  
  @Test
  public void testStatsPersistNoRecovery() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      PartitionedRegion region = (PartitionedRegion) createRegion(-1);
      region.put("A", "B");
      cache.close();
      region = (PartitionedRegion) createRegion(-1);
      
      BucketRegion bucket = region.getBucketRegion("A");
      assertEquals(1, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(0, bucket.getDiskRegion().getStats().getNumEntriesInVM());
      assertEquals("B", region.get("A"));
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    }
  }
  
  @Test
  public void testStatsChangeSyncRecovery() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "true");
    try {
      PartitionedRegion region = (PartitionedRegion) createRegion(-1);
      region.put("A", "B");
      cache.close();
      region = (PartitionedRegion) createRegion(-1);
      
      BucketRegion bucket = region.getBucketRegion("A");
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
      assertEquals("B", region.get("A"));
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "false");
    }
  }
  
  @Test
  public void testStatsPersistAsyncRecovery() throws InterruptedException {
    PartitionedRegion region = (PartitionedRegion) createRegion(-1);
    for(int i=0; i < 1000; i++) {
      region.put(i, "B");
    }
    cache.close();
    region = (PartitionedRegion) createRegion(-1);

    BucketRegion bucket = region.getBucketRegion("A");
    for(int i=0; i < 1000; i++) {
      region.get(i);
    }
    
    //There is a race where the async value recovery thread may be handling
    //the stats for the last entry (even though it is already faulted in)
    int count =0;
    while(0 != bucket.getDiskRegion().getStats().getNumOverflowOnDisk()) {
      Thread.sleep(50);
      if(++count==20) {
        break;
      }
    }
    assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
    assertEquals(1000, bucket.getDiskRegion().getStats().getNumEntriesInVM());
  }

  private Region createRegion(int ttl) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "info");
//    props.setProperty("log-file", "junit.log");
    cache = new CacheFactory(props).create();
    cache.createDiskStoreFactory()
        .setMaxOplogSize(1)
        .setDiskDirs(new File[] { dir} )
        .create("disk");
    
    
    RegionFactory<Object, Object> rf = cache.createRegionFactory()
        .setDataPolicy(DataPolicy.PERSISTENT_PARTITION)
        .setDiskStoreName("disk");
    rf.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(1).create());
    if(ttl > 0) {
      rf.setEntryTimeToLive(new ExpirationAttributes(ttl, ExpirationAction.DESTROY));
    }
    Region region = rf
        .create("region");
    return region;
  }
}
