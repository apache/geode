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
package com.gemstone.gemfire.management.internal.cli.commands;

import static com.gemstone.gemfire.test.dunit.LogWriterUtils.*;
import static com.gemstone.gemfire.test.dunit.Wait.*;
import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
@SuppressWarnings("deprecated")
public class DataCommandsOverHttpDistributedTest extends CliCommandTestBase {

  private static final String REBALANCE_REGION_NAME = DataCommandsOverHttpDistributedTest.class.getSimpleName() + "Region";

  @ClassRule
  public static ProvideSystemProperty provideSystemProperty = new ProvideSystemProperty(CliCommandTestBase.USE_HTTP_SYSTEM_PROPERTY, "true");

  @Test
  public void testSimulateForEntireDSWithTimeout() {
    setupTestRebalanceForEntireDS();
    //check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);

    getLogWriter().info("testSimulateForEntireDS verified MBean and executing command");

    String command = "rebalance --simulate=true --time-out=-1";

    CommandResult cmdResult = executeCommand(command);

    getLogWriter().info("testSimulateForEntireDS just after executing " + cmdResult);

    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testSimulateForEntireDS stringResult : " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceForIncludeRegionFunction failed as did not get CommandResult");
    }
  }

  SerializableRunnable checkRegionMBeans = new SerializableRunnable() {
    @Override
    public void run() {
      final WaitCriterion waitForMaangerMBean = new WaitCriterion() {
        @Override
        public boolean done() {
          final ManagementService service = ManagementService.getManagementService(getCache());
          final DistributedRegionMXBean bean = service.getDistributedRegionMXBean(
            Region.SEPARATOR + REBALANCE_REGION_NAME);
          if (bean == null) {
            getLogWriter().info("Still probing for checkRegionMBeans ManagerMBean");
            return false;
          } else {
            // verify that bean is proper before executing tests
            if (bean.getMembers() != null && bean.getMembers().length > 1 && bean.getMemberCount() > 0 && service.getDistributedSystemMXBean().listRegions().length >= 2) {
              return true;
            } else {
              return false;
            }
          }
        }

        @Override
        public String description() {
          return "Probing for testRebalanceCommandForSimulateWithNoMember ManagerMBean";
        }
      };
      waitForCriterion(waitForMaangerMBean, 2 * 60 * 1000, 2000, true);
      DistributedRegionMXBean bean = ManagementService.getManagementService(getCache()).getDistributedRegionMXBean(
        "/" + REBALANCE_REGION_NAME);
      assertNotNull(bean);
    }
  };

  void setupTestRebalanceForEntireDS() {
    final VM vm1 = Host.getHost(0).getVM(1);
    final VM vm2 = Host.getHost(0).getVM(2);
    setUpJmxManagerOnVm0ThenConnect(null);

    vm1.invoke(new SerializableRunnable() {
      public void run() {

        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create(REBALANCE_REGION_NAME);
        for (int i = 0; i < 10; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
        region = dataRegionFactory.create(REBALANCE_REGION_NAME + "Another1");
        for (int i = 0; i < 100; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });

    vm2.invoke(new SerializableRunnable() {
      public void run() {

        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create(REBALANCE_REGION_NAME);
        for (int i = 0; i < 100; i++) {
          region.put("key" + (i + 400), "value" + (i + 400));
        }
        region = dataRegionFactory.create(REBALANCE_REGION_NAME + "Another2");
        for (int i = 0; i < 10; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });
  }

}
