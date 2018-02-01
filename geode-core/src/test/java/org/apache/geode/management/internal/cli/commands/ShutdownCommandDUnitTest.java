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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.http;
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category(DistributedTest.class)
@RunWith(Parameterized.class)
public class ShutdownCommandDUnitTest {
  private static final String MANAGER_NAME = "Manager";
  private static final String SERVER1_NAME = "Server1";
  private static final String SERVER2_NAME = "Server2";
  private static final String GROUP0 = "Group0";
  private static final String GROUP1 = "Group1";
  private static final String GROUP2 = "Group2";

  private MemberVM manager;
  private MemberVM server1;
  private MemberVM server2;


  @Parameterized.Parameter
  public static boolean useHttp;

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] {true, false};
  }

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();


  @Before
  public void setup() throws Exception {
    Properties managerProps = new Properties();
    managerProps.setProperty(NAME, MANAGER_NAME);
    managerProps.setProperty(GROUPS, GROUP0);
    managerProps.setProperty(LOG_FILE, "someLog.log");

    manager = clusterStartupRule.startLocatorVM(0, managerProps);

    Properties server1Props = new Properties();
    server1Props.setProperty(NAME, SERVER1_NAME);
    server1Props.setProperty(GROUPS, GROUP1);
    server1 = clusterStartupRule.startServerVM(1, server1Props, manager.getPort());

    Properties server2Props = new Properties();
    server2Props.setProperty(NAME, SERVER2_NAME);
    server2Props.setProperty(GROUPS, GROUP2);
    server2 = clusterStartupRule.startServerVM(2, server2Props, manager.getPort());

    if (useHttp) {
      gfsh.connectAndVerify(manager.getHttpPort(), http);
    } else {
      gfsh.connectAndVerify(manager.getJmxPort(), jmxManager);
    }
  }

  @Test
  public void testShutdownServers() {
    String command = "shutdown";

    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Shutdown is triggered");

    verifyShutDown(server1, server2);

    // Make sure the locator is still running
    gfsh.executeAndAssertThat("list members").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains(MANAGER_NAME);
  }

  @Test
  public void testShutdownAll() {
    String command = "shutdown --include-locators=true";

    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Shutdown is triggered");

    verifyShutDown(server1, server2, manager);
  }

  private void verifyShutDown(MemberVM... members) {
    SerializableCallableIF<Boolean> isCacheOpenInThisVM = () -> {
      boolean cacheExists;
      try {
        Cache cacheInstance = CacheFactory.getAnyInstance();
        cacheExists = cacheInstance.getDistributedSystem().isConnected();
      } catch (CacheClosedException e) {
        cacheExists = false;
      }
      return cacheExists;
    };

    for (MemberVM member : members) {
      Callable<Boolean> isMemberShutDown = () -> !member.getVM().invoke(isCacheOpenInThisVM);

      Awaitility.await().atMost(2, TimeUnit.MINUTES).until(isMemberShutDown);
    }
  }
}
