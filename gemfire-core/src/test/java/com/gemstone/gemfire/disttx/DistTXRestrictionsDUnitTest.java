package com.gemstone.gemfire.disttx;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache30.TXRestrictionsDUnitTest;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.DistributedTransactionsTest;


/**
 * Same tests as that of {@link TXRestrictionsDUnitTest} after setting
 * "distributed-transactions" property to true
 */
@Category({DistributedTransactionsTest.class})
public class DistTXRestrictionsDUnitTest extends TXRestrictionsDUnitTest {

  public DistTXRestrictionsDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
//    props.put("distributed-transactions", "true");
    props.setProperty(DistributionConfig.DISTRIBUTED_TRANSACTIONS_NAME, "true");
//    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
    return props;
  }
}
