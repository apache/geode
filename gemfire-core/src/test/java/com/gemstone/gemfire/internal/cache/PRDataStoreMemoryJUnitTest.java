/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests memory allocation operations on a PartitionedRegion on a single node.
 *
 * @author rreja, modified by Girish
 */
@Category(IntegrationTest.class)
public class PRDataStoreMemoryJUnitTest {
  
  static DistributedSystem sys;
  
  static Cache cache;

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
    dsProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
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
  public void testCurrentAllocatedMemory() throws Exception
  {
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
