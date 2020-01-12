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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import org.junit.Test;

import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * This is for testing heap size from Mbean
 *
 */


public class TestHeapDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public TestHeapDUnitTest() {
    super();
  }

  public static long getHeapSizeOfClient() {
    return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
  }

  public static long getHeapSizeOfDS() {
    final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override
      public boolean done() {
        final ManagementService service = getManagementService();
        final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
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

    GeodeAwaitility.await().untilAsserted(waitCriteria);
    final DistributedSystemMXBean bean = getManagementService().getDistributedSystemMXBean();
    assertNotNull(bean);
    return bean.getTotalHeapSize() * 1000;
  }

  @Test
  public void testTotalHeapSize() throws Exception {
    initManagement(false);
    long totalHeapSizeOnAll = 0;
    for (VM vm : managedNodeList) {
      totalHeapSizeOnAll = totalHeapSizeOnAll
          + ((Number) vm.invoke(() -> TestHeapDUnitTest.getHeapSizeOfClient())).longValue();
    }
    long totalHeapSizeFromMXBean =
        ((Number) managingNode.invoke(() -> TestHeapDUnitTest.getHeapSizeOfDS())).intValue();

    LogWriterUtils.getLogWriter().info("testTotalHeapSize totalHeapSizeFromMXBean = "
        + totalHeapSizeFromMXBean + " totalHeapSizeOnAll = " + totalHeapSizeOnAll);

    assertNotSame(0, totalHeapSizeFromMXBean);
    assertNotSame(0, totalHeapSizeOnAll);
    assertNotSame(0,
        totalHeapSizeFromMXBean - totalHeapSizeOnAll > 0
            ? (totalHeapSizeFromMXBean - totalHeapSizeOnAll)
            : (-1 * (totalHeapSizeFromMXBean - totalHeapSizeOnAll)));
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }

}
