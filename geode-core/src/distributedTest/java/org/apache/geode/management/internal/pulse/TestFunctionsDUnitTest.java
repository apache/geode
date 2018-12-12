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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 * This is for testing running functions
 */


class SimpleWaitFunction<T> implements Function<T> {
  private static CountDownLatch countDownLatch;

  public static void setLatch() {
    countDownLatch = new CountDownLatch(1);
  }

  public static void clearLatch() {
    countDownLatch.countDown();
  }

  @Override
  public void execute(FunctionContext<T> context) {
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}


public class TestFunctionsDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestFunctionsDUnitTest() {
    super();
  }

  private static Integer getNumOfRunningFunction() {

    await().until(() -> {
      final ManagementService service = getManagementService();
      final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
      if (bean != null) {
        return bean.getNumRunningFunctions() > 0;
      }
      return false;
    });

    final DistributedSystemMXBean bean = getManagementService().getDistributedSystemMXBean();
    assertNotNull(bean);

    return bean.getNumRunningFunctions();
  }

  @Test
  public void testNumOfRunningFunctions() {
    initManagement(false);
    VM client = managedNodeList.get(2);
    client.invokeAsync(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        SimpleWaitFunction<Boolean> simpleWaitFunction = new SimpleWaitFunction();
        simpleWaitFunction.setLatch();
        Execution execution =
            FunctionService.onMember(cache.getDistributedSystem().getDistributedMember());
        execution.setArguments(Boolean.TRUE).execute(simpleWaitFunction);
      }
    });

    Integer numOfRunningFunctions =
        managingNode.invoke(TestFunctionsDUnitTest::getNumOfRunningFunction);

    assertTrue(numOfRunningFunctions > 0);

    client.invoke(new SerializableRunnable() {
      public void run() {
        SimpleWaitFunction.clearLatch();
      }
    });
  }

}
