package com.gemstone.gemfire.disttx;

import java.util.Properties;

import org.junit.Ignore;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.partitioned.PersistentPartitionedRegionWithTransactionDUnitTest;


/**
 * Same tests as that of
 * {@link PersistentPartitionedRegionWithTransactionDUnitTest} after setting
 * "distributed-transactions" property to true
 */
public class PersistentPartitionedRegionWithDistTXDUnitTest extends
    PersistentPartitionedRegionWithTransactionDUnitTest {

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DistributionConfig.DISTRIBUTED_TRANSACTIONS_NAME, "true");
//    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
    return props;
  }
  
  public PersistentPartitionedRegionWithDistTXDUnitTest(String name) {
    super(name);
  }
}
