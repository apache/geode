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

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Properties;

import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class CreateGatewaySenderCommandDUnitTest {

  @Rule
  public LocatorServerStartupRule locatorServerStartupRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  private MemberVM locatorSite1;
  private MemberVM locatorSite2;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = locatorServerStartupRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    locatorSite2 = locatorServerStartupRule.startLocatorVM(2, props);

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locatorSite1);
  }

  /**
   * GatewaySender with given attribute values. Error scenario where dispatcher threads is set to
   * more than 1 and no order policy provided.
   */
  @Test
  public void testCreateGatewaySender_Error() throws Exception {

    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT + "=" + socketReadTimeout + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL + "=5000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD + "=100" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS + "=2";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testCreateDestroyGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation should fail", stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySender failed as did not get CommandResult");
    }
  }

  /**
   * Parallel GatewaySender with given attribute values. Provide dispatcherThreads as 2 which is not
   * valid for Parallel sender.
   */
  @Test
  public void testCreateParallelGatewaySender_Error() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT + "=" + socketReadTimeout + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL + "=5000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD + "=100" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY + "=THREAD";
    IgnoredException exp =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
      if (cmdResult != null) {
        String strCmdResult = cmdResult.toString();
        getLogWriter()
            .info("testCreateParallelGatewaySender_Error stringResult : " + strCmdResult + ">>>>");
        assertEquals(Result.Status.OK, cmdResult.getStatus());

        TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
        List<String> status = resultData.retrieveAllValues("Status");
        assertEquals(3, status.size());
        for (String stat : status) {
          assertTrue("GatewaySender creation should have failed", stat.contains("ERROR:"));
        }
      } else {
        fail("testCreateParallelGatewaySender_Error failed as did not get CommandResult");
      }
    } finally {
      exp.remove();
    }
  }

  private CommandResult executeCommandWithIgnoredExceptions(String command) {
    final IgnoredException ignored = IgnoredException.addIgnoredException("Could not connect");
    try {
      return gfsh.executeCommand(command);
    } finally {
      ignored.remove();
    }
  }
}
