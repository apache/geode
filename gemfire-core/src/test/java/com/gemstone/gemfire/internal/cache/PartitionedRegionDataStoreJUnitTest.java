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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This test checks functionality of the PartitionedRegionDatastore on a sinle
 * node.
 * 
 * Created on Dec 23, 2005
 * 
 * @author rreja, modified by Girish
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
    dsProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");

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
