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
package org.apache.geode.management.internal.security;


import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.Member;

@Category({SecurityTest.class})
public class MultiGfshDUnitTest {

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  private Member server;

  @Before
  public void setup() throws Exception {
    server = lsRule.startServerVM(0,
        x -> x.withJMXManager().withSecurityManager(SimpleSecurityManager.class));
  }

  @Test
  public void testMultiUser() throws Exception {
    IgnoredException.addIgnoredException("java.util.zip.ZipException: zip file is empty");
    IgnoredException
        .addIgnoredException("java.lang.IllegalStateException: WAN service is not available.");
    int jmxPort = server.getJmxPort();

    // set up vm_1 as a gfsh vm, data-reader will login and log out constantly in this vm until the
    // test is done.
    VM vm1 = lsRule.getVM(1);
    vm1.invokeAsync("run as data-reader", () -> {
      while (true) {
        GfshCommandRule gfsh = new GfshCommandRule(server::getJmxPort, PortType.jmxManager);
        gfsh.secureConnectAndVerify(jmxPort, PortType.jmxManager, "dataRead", "dataRead");
        gfsh.close();
      }
    });

    VM vm2 = lsRule.getVM(2);
    // set up vm_2 as a gfsh vm, and then connect as "stranger" and try to execute the commands and
    // assert errors comes back are NotAuthorized
    AsyncInvocation vm2Invoke = vm2.invokeAsync("run as guest", () -> {
      GfshCommandRule gfsh = new GfshCommandRule();
      gfsh.secureConnectAndVerify(jmxPort, PortType.jmxManager, "guest", "guest");

      List<TestCommand> allCommands = TestCommand.getOnlineCommands();
      for (TestCommand command : allCommands) {
        LogService.getLogger().info("executing: " + command.getCommand());
        gfsh.executeAndAssertThat(command.getCommand()).statusIsError()
            .containsOutput("Unauthorized");
      }

      gfsh.close();
      LogService.getLogger().info("vm 2 done!");
    });


    VM vm0 = lsRule.getVM(0);
    IgnoredException
        .addIgnoredException("java.lang.IllegalArgumentException: Region does not exist: RegionA",
            vm0);
    IgnoredException.addIgnoredException("java.lang.ClassNotFoundException: myApp.myListener", vm0);

    // set up vm_3 as another gfsh vm, and then connect as "super-user" and try to execute the
    // commands and assert we don't get a NotAuthorized Exception
    VM vm3 = lsRule.getVM(3);
    AsyncInvocation vm3Invoke = vm3.invokeAsync("run as superUser", () -> {
      GfshCommandRule gfsh = new GfshCommandRule();
      gfsh.secureConnectAndVerify(jmxPort, PortType.jmxManager, "data,cluster", "data,cluster");

      List<TestCommand> allCommands = TestCommand.getOnlineCommands();
      for (TestCommand command : allCommands) {
        LogService.getLogger().info("executing: " + command.getCommand());
        gfsh.executeAndAssertThat(command.getCommand()).doesNotContainOutput("Unauthorized");
      }
      gfsh.close();
      LogService.getLogger().info("vm 3 done!");
    });

    // only wait until vm2 and vm3 are done. vm1 will close when test finishes
    vm2Invoke.join();
    vm3Invoke.join();

    vm2Invoke.checkException();
    vm3Invoke.checkException();

    IgnoredException.removeAllExpectedExceptions();
  }

}
