package com.gemstone.gemfire.disttx;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.TransactionsWithDeltaDUnitTest;

public class DistTXWithDeltaDUnitTest extends TransactionsWithDeltaDUnitTest {

  public DistTXWithDeltaDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DistributionConfig.DISTRIBUTED_TRANSACTIONS_NAME, "true");
    // props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
    return props;
  }

}
