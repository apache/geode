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
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

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

    Wait.waitForCriterion(waitCriteria, 2 * 60 * 1000, 3000, true);
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
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    cqDUnitTest.createClient(client, port, host0);
    Integer numOfClients = (Integer) managingNode.invoke(
        TestClientsDUnitTest.class, "getNumOfClients");
    LogWriterUtils.getLogWriter().info("testNumOfClients numOfClients = " + numOfClients);
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
    assertEquals(1, numOfClients.intValue());
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}