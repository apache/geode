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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class RebalanceCommandDistributedTest extends CliCommandTestBase {

  private static final String REBALANCE_REGION_NAME =
      RebalanceCommandDistributedTest.class.getSimpleName() + "Region";

  @ClassRule
  public static ProvideSystemProperty provideSystemProperty =
      new ProvideSystemProperty(CliCommandTestBase.USE_HTTP_SYSTEM_PROPERTY, "false");

  @Before
  public void before() throws Exception {
    setUpJmxManagerOnVm0ThenConnect(null);
    setupTestRebalanceForEntireDS();
  }

  @Test
  public void testSimulateForEntireDSWithTimeout() {
    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(() -> checkRegionMBeans());

    getLogWriter().info("testSimulateForEntireDS verified MBean and executing command");

    String command = "rebalance --simulate=true --time-out=-1";

    CommandResult cmdResult = executeCommand(command);

    getLogWriter().info("testSimulateForEntireDS just after executing " + cmdResult);

    assertThat(cmdResult).isNotNull();

    String stringResult = commandResultToString(cmdResult);
    getLogWriter().info("testSimulateForEntireDS stringResult : " + stringResult);
    assertThat(cmdResult.getStatus()).isEqualTo(Result.Status.OK);
  }

  private void checkRegionMBeans() {
    WaitCriterion waitForManagerMBean = new WaitCriterion() {
      @Override
      public boolean done() {
        final ManagementService service = ManagementService.getManagementService(getCache());
        final DistributedRegionMXBean bean =
            service.getDistributedRegionMXBean(Region.SEPARATOR + REBALANCE_REGION_NAME);
        if (bean == null) {
          getLogWriter().info("Still probing for checkRegionMBeans ManagerMBean");
          return false;
        } else {
          // verify that bean is proper before executing tests
          return bean.getMembers() != null && bean.getMembers().length > 1
              && bean.getMemberCount() > 0
              && service.getDistributedSystemMXBean().listRegions().length >= 2;
        }
      }

      @Override
      public String description() {
        return "Probing for testRebalanceCommandForSimulateWithNoMember ManagerMBean";
      }
    };

    waitForCriterion(waitForManagerMBean, 2 * 60 * 1000, 2000, true);

    DistributedRegionMXBean bean = ManagementService.getManagementService(getCache())
        .getDistributedRegionMXBean("/" + REBALANCE_REGION_NAME);
    assertThat(bean).isNotNull();
  }

  private void setupTestRebalanceForEntireDS() {
    VM vm1 = Host.getHost(0).getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    vm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {

        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
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
      @Override
      public void run() {

        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
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
