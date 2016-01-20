/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.pulse;

import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryDUnitTest;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagementTestBase;
import dunit.VM;

/**
 * This is for testing server count details from MBean
 * @author ajayp
 * 
 */

public class TestServerDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestServerDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();

  }

  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  public static int getNumOfServersFromMBean() {
   
   final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override
      public boolean done() {
        final ManagementService service = getManagementService();        
        final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        if(bean !=null){
          if (bean.listCacheServers().length > 0) {
            return true;
          }
        }
        return false;
      }
      @Override
      public String description() {
        return "wait for getDistributedSystemMXBean to complete and get results";
      }
    };

    waitForCriterion(waitCriteria, 2 * 60 * 1000, 3000, true);    
    final DistributedSystemMXBean bean = getManagementService().getDistributedSystemMXBean();
    assertNotNull(bean);
    return bean.listCacheServers().length;
   
  }

  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest(
      "CqDataDUnitTest");

  public void testNumOfServersDUnitTest() throws Exception {
    initManagement(false);
    VM server = managedNodeList.get(1);    
    int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cqDUnitTest.createServer(server, serverPort);    
    int serverCount = ((Number) managingNode.invoke(TestServerDUnitTest.class,
        "getNumOfServersFromMBean")).intValue();
    getLogWriter().info("TestServerDUnitTest serverCount =" + serverCount);
    cqDUnitTest.closeServer(server);
    assertEquals(1, serverCount);
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}