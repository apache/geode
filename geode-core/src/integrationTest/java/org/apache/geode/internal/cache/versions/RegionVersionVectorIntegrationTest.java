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
package org.apache.geode.internal.cache.versions;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;

public class RegionVersionVectorIntegrationTest {

  private final Properties props = new Properties();
  private InternalCache cache = null;
  private final String REGION_NAME = "region";
  private Region region = null;

  @Before
  public void setup() {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    createCache();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  private void createCache() {
    cache = (InternalCache) new CacheFactory(props).create();
  }

  private void createData() {
    // create buckets
    for (int i = 0; i < 10; i++) {
      region.put(i, "value");
    }
  }

  @Test
  public void partitionedRegionDoesNotCreateRegionVersionVector() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(10);
    region = cache.createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(paf.create()).create(REGION_NAME);
    createData();
    assertNull(((LocalRegion) region).getVersionVector());
  }

  @Test
  public void persistPartitionedRegionDoesNotCreateRegionVersionVector() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(10);
    region = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
        .setPartitionAttributes(paf.create()).create(REGION_NAME);
    createData();
    assertNull(((LocalRegion) region).getVersionVector());
  }

  @Test
  public void bucketRegionCreatesRegionVersionVector() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(10);
    region = cache.createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(paf.create()).create(REGION_NAME);
    createData();
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    Set<BucketRegion> bucketRegions = partitionedRegion.getDataStore().getAllLocalBucketRegions();
    for (BucketRegion bucketRegion : bucketRegions) {
      assertNotNull(bucketRegion.getVersionVector());
    }
  }

  @Test
  public void persistBucketRegionCreatesRegionVersionVector() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(10);
    region = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
        .setPartitionAttributes(paf.create()).create(REGION_NAME);
    createData();
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    Set<BucketRegion> bucketRegions = partitionedRegion.getDataStore().getAllLocalBucketRegions();
    for (BucketRegion bucketRegion : bucketRegions) {
      assertNotNull(bucketRegion.getVersionVector());
    }
  }

  @Test
  public void bucketRegionOnPartitionedRegionWithConcurrencyCheckDisabledDoesNotCreateRegionVersionVector() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(10);
    region = cache.createRegionFactory(RegionShortcut.PARTITION).setConcurrencyChecksEnabled(false)
        .setPartitionAttributes(paf.create()).create(REGION_NAME);
    createData();
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    Set<BucketRegion> bucketRegions = partitionedRegion.getDataStore().getAllLocalBucketRegions();
    for (BucketRegion bucketRegion : bucketRegions) {
      assertNull(bucketRegion.getVersionVector());
    }
  }

  @Test
  public void distributedRegionCreatesRegionVersionVector() {
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
    assertNotNull(((LocalRegion) region).getVersionVector());
  }

  @Test
  public void persistentDistributedRegionWithPersistenceCreateRegionVersionVector() {
    region = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).create(REGION_NAME);
    assertNotNull(((LocalRegion) region).getVersionVector());
  }

  @Test
  public void distributedRegionWithConcurrencyCheckDisabledDoesNotCreateRegionVersionVector() {
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).setConcurrencyChecksEnabled(false)
        .create(REGION_NAME);
    assertNull(((LocalRegion) region).getVersionVector());
  }

  @Test
  public void localRegionCreateRegionVersionVector() {
    region = cache.createRegionFactory(RegionShortcut.LOCAL).create(REGION_NAME);
    assertNotNull(((LocalRegion) region).getVersionVector());
  }

  @Test
  public void localPersistentRegionCreateRegionVersionVector() {
    region = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).create(REGION_NAME);
    assertNotNull(((LocalRegion) region).getVersionVector());
  }

  @Test
  public void localRegionDisableConcurrencyCheckDoesNotCreateRegionVersionVector() {
    region = cache.createRegionFactory(RegionShortcut.LOCAL).setConcurrencyChecksEnabled(false)
        .create(REGION_NAME);
    assertNull(((LocalRegion) region).getVersionVector());
  }

}
