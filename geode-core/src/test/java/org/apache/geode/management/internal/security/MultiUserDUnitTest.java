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

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule.PortType;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.Member;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class MultiUserDUnitTest {

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  private Member server;

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    properties.put(NAME, MultiUserDUnitTest.class.getSimpleName());
    properties.put(SECURITY_MANAGER, TestSecurityManager.class.getName());
    properties.put("security-json",
        "org/apache/geode/management/internal/security/cacheServer.json");
    server = lsRule.startServerAsJmxManager(0, properties);
  }

  @Category(FlakyTest.class) // GEODE-1579
  @Test
  public void testMultiUser() throws IOException, JSONException, InterruptedException {

    IgnoredException.addIgnoredException("java.util.zip.ZipException: zip file is empty");
    int jmxPort = server.getJmxPort();

    // set up vm_1 as a gfsh vm, data-reader will login and log out constantly in this vm until the
    // test is done.
    VM vm1 = Host.getHost(0).getVM(1);
    AsyncInvocation vm1Invoke = vm1.invokeAsync("run as data-reader", () -> {
      GfshShellConnectionRule gfsh = new GfshShellConnectionRule();
      gfsh.secureConnectAndVerify(jmxPort, PortType.jmxManger, "data-reader", "1234567");

      Awaitility.waitAtMost(5, TimeUnit.MILLISECONDS);
      gfsh.close();
    });

    VM vm2 = Host.getHost(0).getVM(2);
    // set up vm_2 as a gfsh vm, and then connect as "stranger" and try to execute the commands and
    // assert errors comes back are NotAuthorized
    AsyncInvocation vm2Invoke = vm2.invokeAsync("run as guest", () -> {
      GfshShellConnectionRule gfsh = new GfshShellConnectionRule();
      gfsh.secureConnectAndVerify(jmxPort, PortType.jmxManger, "stranger", "1234567");

      List<TestCommand> allCommands = TestCommand.getOnlineCommands();
      for (TestCommand command : allCommands) {
        LogService.getLogger().info("executing: " + command.getCommand());

        CommandResult result = gfsh.executeCommand(command.getCommand());

        int errorCode = ((ErrorResultData) result.getResultData()).getErrorCode();

        // for some commands there are pre execution checks to check for user input error, will skip
        // those commands
        if (errorCode == ResultBuilder.ERRORCODE_USER_ERROR) {
          LogService.getLogger().info("Skip user error: " + result.getContent());
          continue;
        }

        assertEquals("Not an expected result: " + result.toString(),
            ResultBuilder.ERRORCODE_UNAUTHORIZED,
            ((ErrorResultData) result.getResultData()).getErrorCode());

      }
      gfsh.close();
      LogService.getLogger().info("vm 2 done!");
    });


    VM vm3 = Host.getHost(0).getVM(3);
    IgnoredException
        .addIgnoredException("java.lang.IllegalArgumentException: Region doesnt exist: {0}", vm3);
    IgnoredException.addIgnoredException("java.lang.ClassNotFoundException: myApp.myListener", vm3);

    // set up vm_3 as another gfsh vm, and then connect as "super-user" and try to execute the
    // commands and assert we don't get a NotAuthorized Exception
    AsyncInvocation vm3Invoke = vm3.invokeAsync("run as superUser", () -> {
      GfshShellConnectionRule gfsh = new GfshShellConnectionRule();
      gfsh.secureConnectAndVerify(jmxPort, PortType.jmxManger, "super-user", "1234567");

      List<TestCommand> allCommands = TestCommand.getOnlineCommands();
      for (TestCommand command : allCommands) {
        LogService.getLogger().info("executing: " + command.getCommand());

        CommandResult result = gfsh.executeCommand(command.getCommand());
        if (result.getResultData().getStatus() == Status.OK) {
          continue;
        }
        assertNotEquals("Did not expect an Unauthorized exception: " + result.toString(),
            ResultBuilder.ERRORCODE_UNAUTHORIZED,
            ((ErrorResultData) result.getResultData()).getErrorCode());
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
