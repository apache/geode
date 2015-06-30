/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.pulse;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagementTestBase;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * This is for testing running functions
 * 
 * @author ajayp
 * 
 */
public class TestFunctionsDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestFunctionsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();

  }

  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  public static Integer getNumOfRunningFunction() {

    final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override
      public boolean done() {
        final ManagementService service = getManagementService();
        final DistributedSystemMXBean bean = service
            .getDistributedSystemMXBean();
        if (bean != null) {
          if (bean.getNumRunningFunctions() > 0) {
            return true;
          } else {
            return false;
          }
        }
        return false;
      }

      @Override
      public String description() {
        return "wait for getNumOfRunningFunction to complete and get results";
      }
    };

    waitForCriterion(waitCriteria, 2 * 60 * 1000, 3000, true);
    final DistributedSystemMXBean bean = getManagementService()
        .getDistributedSystemMXBean();
    assertNotNull(bean);
    return Integer.valueOf(bean.getNumRunningFunctions());
  }

  public void testNumOfRunningFunctions() throws Exception {
    initManagement(false);
    VM client = managedNodeList.get(2);    
    client.invokeAsync(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();        
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_RUNNING_FOR_LONG_TIME);
        Execution execution = FunctionService.onMember(cache
            .getDistributedSystem().getDistributedMember());
        for (int i = 0; i < 100; i++) {
          execution.execute(function);
        }
      }
    });
    Integer numOfRunningFunctions = (Integer) managingNode.invoke(
        TestFunctionsDUnitTest.class, "getNumOfRunningFunction");
    getLogWriter().info(
        "TestNumOfFunctions numOfRunningFunctions= " + numOfRunningFunctions);
    assertTrue(numOfRunningFunctions > 0 ? true : false);
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}