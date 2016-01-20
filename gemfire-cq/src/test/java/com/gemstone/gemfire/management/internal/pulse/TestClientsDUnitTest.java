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
 * This is for testing Number of clients and can be extended for relevant test
 * addition
 * 
 * @author ajayp
 * 
 */

public class TestClientsDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestClientsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();

  }

  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  public static Integer getNumOfClients() {
    final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override
      public boolean done() {
        final ManagementService service = getManagementService();
        final DistributedSystemMXBean bean = service
            .getDistributedSystemMXBean();
        if (bean != null) {
          if (bean.getNumClients() > 0) {
            return true;
          }
        }
        return false;
      }

      @Override
      public String description() {
        return "wait for getNumOfClients bean to complete and get results";
      }
    };

    waitForCriterion(waitCriteria, 2 * 60 * 1000, 3000, true);
    final DistributedSystemMXBean bean = getManagementService()
        .getDistributedSystemMXBean();
    assertNotNull(bean);
    return Integer.valueOf(bean.getNumClients());
  }

  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest(
      "CqDataDUnitTest");

  public void testNumOfClients() throws Exception {
    initManagement(false);
    VM server = managedNodeList.get(1);
    VM client = managedNodeList.get(2);
    int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cqDUnitTest.createServer(server, serverPort);
    final int port = server.invokeInt(CqQueryDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());
    cqDUnitTest.createClient(client, port, host0);
    Integer numOfClients = (Integer) managingNode.invoke(
        TestClientsDUnitTest.class, "getNumOfClients");
    getLogWriter().info("testNumOfClients numOfClients = " + numOfClients);
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
    assertEquals(1, numOfClients.intValue());
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}