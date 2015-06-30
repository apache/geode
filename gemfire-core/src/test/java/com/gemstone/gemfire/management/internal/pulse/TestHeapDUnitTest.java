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

import dunit.VM;

/**
 * This is for testing heap size from Mbean  
 * @author ajayp
 * 
 */

public class TestHeapDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestHeapDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();

  }

  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  public static long getHeapSizeOfClient() {    
    return (Runtime.getRuntime().totalMemory() -   Runtime.getRuntime().freeMemory());
  }

  public static long getHeapSizeOfDS() {
    final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override
      public boolean done() {
        final ManagementService service = getManagementService();
        final DistributedSystemMXBean bean = service
            .getDistributedSystemMXBean();
        if (bean != null) {
          if (bean.getTotalHeapSize() > 0) {
            return true;
          }
        }
        return false;
      }
      @Override
      public String description() {
        return "wait for getHeapSizeOfDS to complete and get results";
      }
    };

    waitForCriterion(waitCriteria, 2 * 60 * 1000, 3000, true);
    final DistributedSystemMXBean bean = getManagementService()
        .getDistributedSystemMXBean();
    assertNotNull(bean);
    return bean.getTotalHeapSize() * 1000;
  }

  public void testTotalHeapSize() throws Exception {
    initManagement(false);
    long totalHeapSizeOnAll = 0;
    for (VM vm : managedNodeList) {
      totalHeapSizeOnAll = totalHeapSizeOnAll
          + ((Number) vm.invoke(TestHeapDUnitTest.class, "getHeapSizeOfClient"))
              .longValue();
            }
    long totalHeapSizeFromMXBean = ((Number) managingNode.invoke(
        TestHeapDUnitTest.class, "getHeapSizeOfDS")).intValue();

    getLogWriter().info(
        "testTotalHeapSize totalHeapSizeFromMXBean = "
            + totalHeapSizeFromMXBean + " totalHeapSizeOnAll = "
            + totalHeapSizeOnAll);

    assertNotSame(0, totalHeapSizeFromMXBean);
    assertNotSame(0, totalHeapSizeOnAll);
    assertNotSame(
        0,
        totalHeapSizeFromMXBean - totalHeapSizeOnAll > 0 ? (totalHeapSizeFromMXBean - totalHeapSizeOnAll)
            : (-1 * (totalHeapSizeFromMXBean - totalHeapSizeOnAll)));
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}