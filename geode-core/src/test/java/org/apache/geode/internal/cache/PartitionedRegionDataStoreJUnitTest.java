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
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserverAdapter;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserverHolder;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * This test checks functionality of the PartitionedRegionDatastore on a sinle
 * node.
 * 
 * Created on Dec 23, 2005
 * 
 *  
 */
@Category(IntegrationTest.class)
public class PartitionedRegionDataStoreJUnitTest
{
  static DistributedSystem sys;

  static Cache cache;

  byte obj[] = new byte[10240];

  String regionName = "DataStoreRegion";


  @Before
  public void setUp() { 
    Properties dsProps = new Properties();
    dsProps.setProperty(MCAST_PORT, "0");

    //  Connect to a DS and create a Cache.
    sys = DistributedSystem.connect(dsProps);
    cache = CacheFactory.create(sys);
  }
    
  @After
  public void tearDown() {
    sys.disconnect();
  }

  @Test
  public void testRemoveBrokenNode() throws Exception
  {

    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    PartitionAttributes pa = paf.setRedundantCopies(0)
      .setLocalMaxMemory(0).create();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(pa);
    RegionAttributes ra = af.create();

    PartitionedRegion pr = null;
    pr = (PartitionedRegion)cache.createRegion("PR2", ra);
    paf.setLocalProperties(null)
        .create();
    /* PartitionedRegionDataStore prDS = */ new PartitionedRegionDataStore(pr);
   /* PartitionedRegionHelper.removeGlobalMetadataForFailedNode(PartitionedRegion.node,
        prDS.partitionedRegion.getRegionIdentifier(), prDS.partitionedRegion.cache);*/
  }

  @Test
  public void testLocalPut() throws Exception
  {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    Properties globalProps = new Properties();
    globalProps.put(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY,
        "100");

    PartitionAttributes pa = paf.setRedundantCopies(0)
      .setLocalMaxMemory(100).create();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(pa);
    RegionAttributes ra = af.create();

    PartitionedRegion pr = null;
    pr = (PartitionedRegion)cache.createRegion("PR3", ra);

    String key = "User";
    String value = "1";

    pr.put(key, value);
    assertEquals(pr.get(key), value);

  }
  
  @Test
  public void testChangeCacheLoaderDuringBucketCreation() throws Exception
  {
    final PartitionedRegion pr = (PartitionedRegion)cache.createRegionFactory(RegionShortcut.PARTITION)
        .create("testChangeCacheLoaderDuringBucketCreation");

    //Add an observer which will block bucket creation and wait for a loader to be added
    final CountDownLatch loaderAdded = new CountDownLatch(1);
    final CountDownLatch bucketCreated = new CountDownLatch(1);
    PartitionedRegionObserverHolder.setInstance(new PartitionedRegionObserverAdapter() {
      @Override
      public void beforeAssignBucket(PartitionedRegion partitionedRegion, int bucketId) {
        try {
          //Indicate that the bucket has been created
          bucketCreated.countDown();
          
          //Wait for the loader to be added. if the synchronization
          //is correct, this would wait for ever because setting the
          //cache loader will wait for this method. So time out after
          //1 second, which should be good enough to cause a failure
          //if the synchronization is broken.
          loaderAdded.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted");
        }
      }
    });
    
    Thread createBuckets = new Thread() {
      public void run() {
        PartitionRegionHelper.assignBucketsToPartitions(pr);
      }
    };
    
    createBuckets.start();
    
    CacheLoader loader = new CacheLoader() {
      @Override
      public void close() {
      }

      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        return null;
      }
    };
    
    bucketCreated.await();
    pr.getAttributesMutator().setCacheLoader(loader);
    loaderAdded.countDown();
    createBuckets.join();
    

    //Assert that all buckets have received the cache loader
    for(BucketRegion bucket: pr.getDataStore().getAllLocalBucketRegions()) {
      assertEquals(loader, bucket.getCacheLoader()); 
    }
  }

  /**
   * This method checks whether the canAccomodateMoreBytesSafely returns false
   * after reaching the localMax memory.
   *  
   */
  @Test
  public void testCanAccommodateMoreBytesSafely() throws Exception
  {
    int key = 0;
    final int numMBytes = 5;

    final PartitionedRegion regionAck = (PartitionedRegion)
      new RegionFactory()
      .setPartitionAttributes(
          new PartitionAttributesFactory()
          .setRedundantCopies(0)
          .setLocalMaxMemory(numMBytes)
          .create())
      .create(this.regionName);

    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));

    int numk = numMBytes * 1024;
    int num = numk * 1024;
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(num-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num+1));
    final int OVERHEAD = CachedDeserializableFactory.getByteSize(new byte[0]);
    for (key = 0; key < numk; key++) {
      regionAck.put(new Integer(key), new byte[1024-OVERHEAD]);
    }
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1));
    regionAck.invalidate(new Integer(--key));
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(1023));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1024));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1025));
    regionAck.put(new Integer(key), new byte[1024-OVERHEAD]);
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1));
    regionAck.destroy(new Integer(key));
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(1023));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1024));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1025));
    regionAck.put(new Integer(key), new byte[1023-OVERHEAD]);
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(2));

    for (key = 0; key < numk; key++) {
      regionAck.destroy(new Integer(key));
    }
    assertEquals(0, regionAck.size());
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(num-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num+1));

    for (key = 0; key < numk; key++) {
      regionAck.put(new Integer(key), "foo");
    }
  }
}
