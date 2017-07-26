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

package org.apache.geode.internal.cache.wan.wancommand;

import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class DestroyGatewaySenderCommandDUnitTest extends WANCommandTestBase {
  @Test
  public void testDestroyGatewaySender_NotCreatedSender() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    // Test Destroy Command
    String command =
        CliStrings.DESTROY_GATEWAYSENDER + " --" + CliStrings.DESTROY_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testDestroyGatewaySender_NotCreatedSender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(5, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender destroy should fail", stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyParallelGatewaySender failed as did not get CommandResult");
    }
  }

  private CommandResult executeCommandWithIgnoredExceptions(String command) {
    final IgnoredException ignored = IgnoredException.addIgnoredException("Could not connect");
    try {
      return executeCommand(command);
    } finally {
      ignored.remove();
    }
  }
}
