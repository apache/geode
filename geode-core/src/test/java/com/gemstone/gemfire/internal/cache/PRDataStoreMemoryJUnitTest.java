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

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests memory allocation operations on a PartitionedRegion on a single node.
 */
@Category(IntegrationTest.class)
public class PRDataStoreMemoryJUnitTest {
  
  private static DistributedSystem sys;
  
  private static Cache cache;

  @Before
  public void setUp() throws Exception {
    //  Connect to a DS and create a Cache.
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
    return new PartitionAttributesFactory()
        .setRedundantCopies(0)
        .setLocalMaxMemory(10)
        .create();
  }
  
  protected RegionFactory<?, ?> defineRegionFactory() {
    return new RegionFactory()
        .setPartitionAttributes(definePartitionAttributes());
  }
  
  @Test
  public void testCurrentAllocatedMemory() throws Exception {
    PartitionedRegion regionAck1 = (PartitionedRegion)defineRegionFactory()
      .create("testCurrentAllocatedemory");

    assertEquals(0, regionAck1.getDataStore().currentAllocatedMemory());

    Integer val1 = new Integer(16);
    regionAck1.put(new Integer(1), val1);
    Object storedVal = regionAck1.getBucketRegion(new Integer(1)).getRegionEntry(new Integer(1))._getValue();
    final int size1 = CachedDeserializableFactory.calcMemSize(storedVal);
    int size = size1;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    byte[] val2 =  new byte[1000];
    regionAck1.put(new Integer(2), val2);
    storedVal = regionAck1.getBucketRegion(new Integer(2)).getRegionEntry(new Integer(2))._getValue();
    final int size2 = CachedDeserializableFactory.calcMemSize(storedVal);
    size += size2;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    String val3 = "0123456789";
    regionAck1.put(new Integer(3), val3);
    storedVal = regionAck1.getBucketRegion(new Integer(3)).getRegionEntry(new Integer(3))._getValue();
    final int size3 = CachedDeserializableFactory.calcMemSize(storedVal);
    size += size3;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    long beforeSize = regionAck1.getDataStore().currentAllocatedMemory();
    regionAck1.invalidate(new Integer(3));
    size -= size3;
    assertEquals("beforeSize=" + beforeSize
                 + " expectedSize=" + size
                 + " afterSize=" + regionAck1.getDataStore().currentAllocatedMemory(),
                 size, regionAck1.getDataStore().currentAllocatedMemory());
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(new Integer(3));
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    regionAck1.invalidate(new Integer(2));
    size -= size2;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(new Integer(2));
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    regionAck1.invalidate(new Integer(1));
    size -= size1;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(new Integer(1));
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    assertEquals(0, size);

    // Perform the destroy operations w/o the invalidate
    regionAck1.put(new Integer(2), val2);
    size += size2;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(new Integer(2));
    size -= size2;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    regionAck1.put(new Integer(3), val3);
    size += size3;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());
    regionAck1.destroy(new Integer(3));
    size -= size3;
    assertEquals(size, regionAck1.getDataStore().currentAllocatedMemory());

    assertEquals(0, size);
  }

}
