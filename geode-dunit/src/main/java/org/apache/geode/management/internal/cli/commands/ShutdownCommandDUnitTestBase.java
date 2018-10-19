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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class ShutdownCommandDUnitTestBase {

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();


  @Before
  public void setup() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0, l -> l.withHttpService());
    server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, locator.getPort());
    connect(locator);
  }

  void connect(MemberVM locator) throws Exception {
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testShutdownServers() {
    String command = "shutdown";

    gfsh.executeAndAssertThat(command).statusIsSuccess().containsOutput("Shutdown is triggered");
    verifyShutDown(server1, server2);

    // Make sure the locator is still running
    gfsh.executeAndAssertThat("list members").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("locator-0");
  }

  @Test
  public void testShutdownAll() {
    String command = "shutdown --include-locators=true";

    gfsh.executeAndAssertThat(command).statusIsSuccess().containsOutput("Shutdown is triggered");
    verifyShutDown(server1, server2, locator);
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

      await().until(isMemberShutDown);
    }
  }
}
