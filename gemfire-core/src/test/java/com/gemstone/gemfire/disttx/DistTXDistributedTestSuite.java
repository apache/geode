package com.gemstone.gemfire.disttx;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  CacheMapDistTXDUnitTest.class,
  DistributedTransactionDUnitTest.class,
  DistTXDebugDUnitTest.class,
  DistTXOrderDUnitTest.class,
  DistTXPersistentDebugDUnitTest.class,
  DistTXRestrictionsDUnitTest.class,
  DistTXWithDeltaDUnitTest.class,
  PersistentPartitionedRegionWithDistTXDUnitTest.class,
  PRDistTXDUnitTest.class,
  PRDistTXWithVersionsDUnitTest.class
})

/**
 * Suite of tests for distributed transactions dunit tests
 * @author shirishd
 */
public class DistTXDistributedTestSuite {

}
