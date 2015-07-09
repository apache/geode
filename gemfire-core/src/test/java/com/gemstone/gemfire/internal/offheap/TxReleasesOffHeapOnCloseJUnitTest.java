package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class TxReleasesOffHeapOnCloseJUnitTest {
  
  protected Cache cache;
  
  protected void createCache() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("off-heap-memory-size", "1m");
    cache = new CacheFactory(props).create();
  }
  
  // Start a tx and have it modify an entry on an offheap region.
  // Close the cache and verify that the offheap memory was released
  // even though the tx was not commited or rolled back.
  @Test
  public void testTxReleasesOffHeapOnClose() {
    createCache();
    SimpleMemoryAllocatorImpl sma = SimpleMemoryAllocatorImpl.getAllocator();
    RegionFactory rf = cache.createRegionFactory();
    rf.setOffHeap(true);
    Region r = rf.create("testTxReleasesOffHeapOnClose");
    r.put("key", "value");
    CacheTransactionManager txmgr = cache.getCacheTransactionManager();
    txmgr.begin();
    r.put("key", "value2");
    cache.close();
    assertEquals(0, sma.getUsedMemory());
  }
}
