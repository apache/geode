/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.pulse;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.query.cq.dunit.CqQueryDUnitTest;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * This is for testing Number of clients and can be extended for relevant test addition
 * 
 * 
 */

@Category(DistributedTest.class)
public class TestClientsDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestClientsDUnitTest() {
    super();
  }

  public static Integer getNumOfClients() {
    final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override
      public boolean done() {
        final ManagementService service = getManagementService();
        final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
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
    final DistributedSystemMXBean bean = getManagementService().getDistributedSystemMXBean();
    assertNotNull(bean);
    return Integer.valueOf(bean.getNumClients());
  }

  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest();

  @Test
  public void testNumOfClients() throws Exception {
    initManagement(false);
    VM server = managedNodeList.get(1);
    VM client = managedNodeList.get(2);
    int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cqDUnitTest.createServer(server, serverPort);
    final int port = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    cqDUnitTest.createClient(client, port, host0);
    Integer numOfClients =
        (Integer) managingNode.invoke(() -> TestClientsDUnitTest.getNumOfClients());
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
