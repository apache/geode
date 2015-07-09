package com.gemstone.gemfire.disttx;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache30.TXOrderDUnitTest;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.DistributedTransactionsTest;


/**
 * Same tests as that of {@link TXOrderDUnitTest} after setting
 * "distributed-transactions" property to true
 */
@Category({DistributedTransactionsTest.class})
public class DistTXOrderDUnitTest extends TXOrderDUnitTest {

  public DistTXOrderDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DistributionConfig.DISTRIBUTED_TRANSACTIONS_NAME, "true");
//    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
    return props;
  }
  
  @Override
  @Ignore
  public void testFarSideOrder() throws CacheException {
    //[DISTTX] TODO fix this test
  }
  
  @Override
  @Ignore
  public void testInternalRegionNotExposed() {
    //[DISTTX] TODO fix this test
  }
}
