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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.security.SimpleTestSecurityManager;
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
        x -> x.withJMXManager().withSecurityManager(SimpleTestSecurityManager.class));
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
    AsyncInvocation vm1Invoke = vm1.invokeAsync("run as data-reader", () -> {
      while (true) {
        GfshCommandRule gfsh = new GfshCommandRule();
        gfsh.secureConnectAndVerify(jmxPort, PortType.jmxManager, "dataRead", "dataRead");

        await();
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

        CommandResult result = gfsh.executeCommand(command.getCommand());

        if (!(result.getResultData() instanceof ErrorResultData)) {
          break;
        }
        int errorCode = ((ErrorResultData) result.getResultData()).getErrorCode();

        // for some commands there are pre execution checks to check for user input error, will skip
        // those commands
        if (errorCode == ResultBuilder.ERRORCODE_USER_ERROR) {
          LogService.getLogger().info("Skip user error: " + result.getMessageFromContent());
          continue;
        }

        assertEquals("Not an expected result: " + result.toString(),
            ResultBuilder.ERRORCODE_UNAUTHORIZED,
            ((ErrorResultData) result.getResultData()).getErrorCode());
      }
      gfsh.close();
      LogService.getLogger().info("vm 2 done!");
    });


    VM vm3 = lsRule.getVM(3);
    IgnoredException
        .addIgnoredException("java.lang.IllegalArgumentException: Region doesnt exist: {0}", vm3);
    IgnoredException.addIgnoredException("java.lang.ClassNotFoundException: myApp.myListener", vm3);

    // set up vm_3 as another gfsh vm, and then connect as "super-user" and try to execute the
    // commands and assert we don't get a NotAuthorized Exception
    AsyncInvocation vm3Invoke = vm3.invokeAsync("run as superUser", () -> {
      GfshCommandRule gfsh = new GfshCommandRule();
      gfsh.secureConnectAndVerify(jmxPort, PortType.jmxManager, "data,cluster", "data,cluster");

      List<TestCommand> allCommands = TestCommand.getOnlineCommands();
      for (TestCommand command : allCommands) {
        LogService.getLogger().info("executing: " + command.getCommand());

        CommandResult result = gfsh.executeCommand(command.getCommand());
        if (result.getStatus() == Status.OK) {
          continue;
        }

        int errorResultCode;
        if (result.getResultData() instanceof ErrorResultData) {
          errorResultCode = ((ErrorResultData) result.getResultData()).getErrorCode();
        } else {
          errorResultCode = 9999; // ((ResultModel) result.getResultData()).getErrorCode();
        }
        assertNotEquals("Did not expect an Unauthorized exception: " + result.toString(),
            ResultBuilder.ERRORCODE_UNAUTHORIZED, errorResultCode);
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
