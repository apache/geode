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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.distributed.DistributedSystem;

/**
 * Tests memory allocation operations on a PartitionedRegion on a single node.
 */
public class PRDataStoreMemoryJUnitTest {

  private static DistributedSystem sys;

  private static Cache cache;

  @Before
  public void setUp() throws Exception {
    // Connect to a DS and create a Cache.
    sys = DistributedSystem.connect(getDistributedSystemProperties());
    cache = CacheFactory.create(sys);
  }

  @After
  public void tearDown() throws Exception {
    sys.disconnect();
  }

  protected Properties getDistributedSystemProperties() {
    Properties dsProps = new Properties();
    dsProps.setProperty(MCAST_PORT, "0");
    return dsProps;
  }

  protected PartitionAttributes<?, ?> definePartitionAttributes() {
    return new PartitionAttributesFactory().setRedundantCopies(0).setLocalMaxMemory(10).create();
  }

  protected RegionFactory<?, ?> defineRegionFactory() {
    return new RegionFactory().setPartitionAttributes(definePartitionAttributes());
  }

  @Test
  public void testCurrentAllocatedMemory() throws Exception {
    PartitionedRegion regionAck1 =
        (PartitionedRegion) defineRegionFactory().create("testCurrentAllocatedemory");

    assertEquals(0, regionAck1.getDataStore().currentAllocatedMemory());

    Integer val1 = 16;
    regionAck1.put(1, val1);
    Object storedVal =
        regionAck1.getBucketRegion(1).getRegionEntry(1).getValue();
    final int size1 = CachedDeserializableFactory.calcMemSize(storedVal);
    int size = size1;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    byte[] val2 = new byte[1000];
    regionAck1.put(2, val2);
    storedVal =
        regionAck1.getBucketRegion(2).getRegionEntry(2).getValue();
    final int size2 = CachedDeserializableFactory.calcMemSize(storedVal);
    size += size2;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    String val3 = "0123456789";
    regionAck1.put(3, val3);
    storedVal =
        regionAck1.getBucketRegion(3).getRegionEntry(3).getValue();
    final int size3 = CachedDeserializableFactory.calcMemSize(storedVal);
    size += size3;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    long beforeSize = regionAck1.getDataStore().currentAllocatedMemory();
    regionAck1.invalidate(3);
    size -= size3;
    assertEquals(
        "beforeSize=" + beforeSize + " expectedSize=" + size + " afterSize="
            + regionAck1.getDataStore().currentAllocatedMemory(),
        size, regionAck1.getDataStore().currentAllocatedMemory());
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(3);
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    regionAck1.invalidate(2);
    size -= size2;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(2);
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    regionAck1.invalidate(1);
    size -= size1;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(1);
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    assertEquals(0, size);

    // Perform the destroy operations w/o the invalidate
    regionAck1.put(2, val2);
    size += size2;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(2);
    size -= size2;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    regionAck1.put(3, val3);
    size += size3;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(3);
    size -= size3;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    assertEquals(0, size);
  }

}
