package com.gemstone.gemfire.disttx;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.internal.offheap.TxReleasesOffHeapOnCloseJUnitTest;
import com.gemstone.gemfire.test.junit.categories.DistributedTransactionsTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;


/**
 * Same tests as that of {@link TxReleasesOffHeapOnCloseJUnitTest} after setting
 * "distributed-transactions" property to true
 */
@Category({IntegrationTest.class, DistributedTransactionsTest.class})
public class DistTXReleasesOffHeapOnCloseJUnitTest extends
    TxReleasesOffHeapOnCloseJUnitTest {

  public DistTXReleasesOffHeapOnCloseJUnitTest() {
  }
  
  @Override
  protected void createCache() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("off-heap-memory-size", "1m");
    props.put("distributed-transactions", "true");
    cache = new CacheFactory(props).create();
    CacheTransactionManager txmgr = cache.getCacheTransactionManager();
    assert(txmgr.isDistributed());
  }

}
