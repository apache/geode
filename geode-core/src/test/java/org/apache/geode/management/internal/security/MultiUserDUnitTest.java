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
package com.gemstone.gemfire.management.internal.security;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.json.JSONException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.ErrorResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class MultiUserDUnitTest extends CliCommandTestBase {

  @Test
  public void testMultiUser() throws IOException, JSONException, InterruptedException {
    Properties properties = new Properties();
    properties.put(NAME, MultiUserDUnitTest.class.getSimpleName());
    properties.put(SECURITY_MANAGER, SampleSecurityManager.class.getName());

    // set up vm_0 the secure jmx manager
    Object[] results = setUpJMXManagerOnVM(0, properties, "com/gemstone/gemfire/management/internal/security/cacheServer.json");
    String gfshDir = this.gfshDir;

    // set up vm_1 as a gfsh vm, data-reader will login and log out constantly in this vm until the test is done.
    VM vm1 = Host.getHost(0).getVM(1);
    AsyncInvocation vm1Invoke = vm1.invokeAsync("run as data-reader", () -> {
      String shellId = getClass().getSimpleName() + "_vm1";
      HeadlessGfsh shell = new HeadlessGfsh(shellId, 30, gfshDir);
      while (true) {
        connect((String) results[0], (Integer) results[1], (Integer) results[2], shell, "data-reader", "1234567");
        Awaitility.waitAtMost(5, TimeUnit.MILLISECONDS);
        shell.executeCommand("disconnect");
      }
    });

    VM vm2 = Host.getHost(0).getVM(2);
    // set up vm_2 as a gfsh vm,  and then connect as "stranger" and try to execute the commands and assert errors comes back are NotAuthorized
    AsyncInvocation vm2Invoke = vm2.invokeAsync("run as guest", () -> {
      String shellId = getClass().getSimpleName() + "_vm2";
      HeadlessGfsh shell = new HeadlessGfsh(shellId, 30, gfshDir);
      connect((String) results[0], (Integer) results[1], (Integer) results[2], shell, "stranger", "1234567");

      List<TestCommand> allCommands = TestCommand.getCommands();
      for (TestCommand command : allCommands) {
        LogService.getLogger().info("executing: " + command.getCommand());
        if (command.getPermission() == null) {
          continue;
        }

        CommandResult result = executeCommand(shell, command.getCommand());

        int errorCode = ((ErrorResultData) result.getResultData()).getErrorCode();

        // for some commands there are pre execution checks to check for user input error, will skip those commands
        if (errorCode == ResultBuilder.ERRORCODE_USER_ERROR) {
          LogService.getLogger().info("Skip user error: " + result.getContent());
          continue;
        }

        assertEquals("Not an expected result: " + result.toString(), ResultBuilder.ERRORCODE_UNAUTHORIZED,
          ((ErrorResultData) result.getResultData()).getErrorCode());
        String resultMessage = result.getContent().toString();
        String permString = command.getPermission().toString();
        assertTrue(resultMessage + " does not contain " + permString, resultMessage.contains(permString));
      }
      LogService.getLogger().info("vm 2 done!");
    });


    VM vm3 = Host.getHost(0).getVM(3);
    IgnoredException.addIgnoredException("java.lang.IllegalArgumentException: Region doesnt exist: {0}", vm3);
    IgnoredException.addIgnoredException("java.lang.ClassNotFoundException: myApp.myListener", vm3);

    // set up vm_3 as another gfsh vm, and then connect as "super-user" and try to execute the commands and assert we don't get a NotAuthorized Exception
    AsyncInvocation vm3Invoke = vm3.invokeAsync("run as superUser", () -> {
      String shellId = getClass().getSimpleName() + "_vm3";
      HeadlessGfsh shell = new HeadlessGfsh(shellId, 30, gfshDir);
      connect((String) results[0], (Integer) results[1], (Integer) results[2], shell, "super-user", "1234567");

      List<TestCommand> allCommands = TestCommand.getCommands();
      for (TestCommand command : allCommands) {
        LogService.getLogger().info("executing: " + command.getCommand());
        if (command.getPermission() == null) {
          continue;
        }

        CommandResult result = executeCommand(shell, command.getCommand());
        if (result.getResultData().getStatus() == Status.OK) {
          continue;
        }

        assertNotEquals("Did not expect an Unauthorized exception: " + result.toString(), ResultBuilder.ERRORCODE_UNAUTHORIZED,
          ((ErrorResultData) result.getResultData()).getErrorCode());
      }

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
