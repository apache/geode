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
package org.apache.geode.management.internal.pulse;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * This is for testing running functions
 * 
 * 
 */
@Category(DistributedTest.class)
public class TestFunctionsDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestFunctionsDUnitTest() {
    super();
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

    Wait.waitForCriterion(waitCriteria, 2 * 60 * 1000, 3000, true);
    final DistributedSystemMXBean bean = getManagementService()
        .getDistributedSystemMXBean();
    assertNotNull(bean);
    return Integer.valueOf(bean.getNumRunningFunctions());
  }

  @Test
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
    Integer numOfRunningFunctions = (Integer) managingNode.invoke(() -> TestFunctionsDUnitTest.getNumOfRunningFunction());
    LogWriterUtils.getLogWriter().info(
        "TestNumOfFunctions numOfRunningFunctions= " + numOfRunningFunctions);
    assertTrue(numOfRunningFunctions > 0 ? true : false);
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}
