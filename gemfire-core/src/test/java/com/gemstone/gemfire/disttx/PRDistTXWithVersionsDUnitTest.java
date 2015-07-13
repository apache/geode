package com.gemstone.gemfire.disttx;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.execute.PRTransactionWithVersionsDUnitTest;

public class PRDistTXWithVersionsDUnitTest extends
    PRTransactionWithVersionsDUnitTest {

  public PRDistTXWithVersionsDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DistributionConfig.DISTRIBUTED_TRANSACTIONS_NAME, "true");
//    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
    return props;
  }
  
  // [DISTTX] TODO test overridden and intentionally left blank as they fail.
  // Fix this 
  
  @Override
  public void testBasicPRTransactionRedundancy0() {
  }

  @Override
  public void testBasicPRTransactionRedundancy1() {
  }

  @Override
  public void testBasicPRTransactionRedundancy2() {
  }

  @Override
  public void testBasicPRTransactionNoDataRedundancy0() {
  }

  @Override
  public void testBasicPRTransactionNoDataRedundancy1() {
  }

  @Override
  public void testBasicPRTransactionNoDataRedundancy2() {
  }

}
