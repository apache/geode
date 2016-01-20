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
 * This is for testing continuous query.
 * @author ajayp
 * 
 */

public class TestCQDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;
  private static final String queryName = "testClientWithFeederAndCQ_0";
  private static final String queryName2 = "testClientWithFeederAndCQ_3";

  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest(
      "CqDataDUnitTest");

  public TestCQDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();

  }

  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  public static long getNumOfCQ() {
    
    final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override
      public boolean done() {
        final ManagementService service = getManagementService();        
        final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        if (bean != null) {
          if(bean.getActiveCQCount() > 0){
            return true;
          }
        }
        return false;
      }

      @Override
      public String description() {
        return "wait for getNumOfCQ to complete and get results";
      }
    };
    waitForCriterion(waitCriteria, 2 * 60 * 1000, 3000, true);    
    final DistributedSystemMXBean bean = getManagementService().getDistributedSystemMXBean();
    assertNotNull(bean);
    return bean.getActiveCQCount();
  }

  public void testNumOfCQ() throws Exception {
    initManagement(false);
    getLogWriter().info("started testNumOfCQ");

    VM server = managedNodeList.get(1);
    VM client = managedNodeList.get(2);    
    
    final String host0 = getServerHostName(server.getHost());

    int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cqDUnitTest.createServer(server, serverPort);

    final int port = server.invokeInt(CqQueryDUnitTest.class,
        "getCacheServerPort");

    cqDUnitTest.createClient(client, port, host0);    
    cqDUnitTest.createCQ(client, queryName, cqDUnitTest.cqs[0]);    
    cqDUnitTest.executeCQ(client, queryName, false, null);    
    
    cqDUnitTest.createCQ(client, queryName2, cqDUnitTest.cqs[3]);    
    cqDUnitTest.executeCQ(client, queryName2, false, null);

    final int size = 1000;    
    cqDUnitTest.createValues(client, cqDUnitTest.regions[0], size);
    cqDUnitTest.waitForCreated(client, queryName, CqQueryDUnitTest.KEY + size);
    cqDUnitTest.waitForCreated(client, queryName2, CqQueryDUnitTest.KEY + size );
    
    
    cqDUnitTest.validateCQ(client, queryName,
    /* resultSize: */CqQueryDUnitTest.noTest,
    /* creates: */size,
    /* updates: */0,
    /* deletes; */0,
    /* queryInserts: */size,
    /* queryUpdates: */0,
    /* queryDeletes: */0,
    /* totalEvents: */size);    
    
    cqDUnitTest.validateCQ(client, queryName2,
        /* resultSize: */CqQueryDUnitTest.noTest,
        /* creates: */size,
        /* updates: */0,
        /* deletes; */0,
        /* queryInserts: */size,
        /* queryUpdates: */0,
        /* queryDeletes: */0,
        /* totalEvents: */size);   
    
    long numOfCQ = ((Number) managingNode.invoke(TestCQDUnitTest.class,
        "getNumOfCQ")).intValue();

    getLogWriter().info("testNumOfCQ numOfCQ= " + numOfCQ);

    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);

    assertTrue(numOfCQ > 0 ? true : false);
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}