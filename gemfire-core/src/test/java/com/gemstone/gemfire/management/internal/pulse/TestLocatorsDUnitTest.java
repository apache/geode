/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.pulse;

import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagementTestBase;

/**
 * This is for testing locators from MBean
 * @author ajayp
 * 
 */

public class TestLocatorsDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestLocatorsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();

  }

  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  public static int getNumOfLocatorFromMBean() {

    final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override
      public boolean done() {
        final ManagementService service = getManagementService();        
        final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        if (bean != null) {
          if(bean.getLocatorCount() > 0){
            return true;
          }
        }
        return false;
      }

      @Override
      public String description() {
        return "wait for getNumOfLocatorFromMBean to complete and get results";
      }
    };
    waitForCriterion(waitCriteria, 2 * 60 * 1000, 2000, true);    
    final DistributedSystemMXBean bean = getManagementService().getDistributedSystemMXBean();
    assertNotNull(bean);    
    return bean.getLocatorCount();
  }

  public void testLocatorsDUnitTest() throws Exception {
    initManagement(false);
    int locatorCount = ((Number) managingNode.invoke(
        TestLocatorsDUnitTest.class, "getNumOfLocatorFromMBean")).intValue();
    getLogWriter().info("TestLocatorsDUnitTest locatorCount =" + locatorCount);
    assertEquals(1, locatorCount);

  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}