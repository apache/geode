/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.pulse;

import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryDUnitTest;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagementTestBase;
import com.gemstone.gemfire.test.dunit.VM;

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