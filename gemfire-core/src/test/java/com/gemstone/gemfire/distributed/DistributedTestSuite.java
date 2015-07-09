package com.gemstone.gemfire.distributed;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
   DistributedMemberDUnitTest.class,
   DistributedSystemDUnitTest.class,
   LocatorDUnitTest.class,
   RoleDUnitTest.class,
   SystemAdminDUnitTest.class
})
/**
 * Suite of tests for distributed membership dunit tests.
 * 
 * @author Kirk Lund
 */
public class DistributedTestSuite {
}
