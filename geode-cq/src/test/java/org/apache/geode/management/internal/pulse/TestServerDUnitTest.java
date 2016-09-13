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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryDUnitTest;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagementTestBase;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * This is for testing server count details from MBean
 * 
 */

@Category(DistributedTest.class)
public class TestServerDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestServerDUnitTest() {
    super();
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

    Wait.waitForCriterion(waitCriteria, 2 * 60 * 1000, 3000, true);    
    final DistributedSystemMXBean bean = getManagementService().getDistributedSystemMXBean();
    assertNotNull(bean);
    return bean.listCacheServers().length;
   
  }

  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest();

  @Test
  public void testNumOfServersDUnitTest() throws Exception {
    initManagement(false);
    VM server = managedNodeList.get(1);    
    int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cqDUnitTest.createServer(server, serverPort);    
    int serverCount = ((Number) managingNode.invoke(() -> TestServerDUnitTest.getNumOfServersFromMBean())).intValue();
    LogWriterUtils.getLogWriter().info("TestServerDUnitTest serverCount =" + serverCount);
    cqDUnitTest.closeServer(server);
    assertEquals(1, serverCount);
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}