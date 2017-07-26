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

import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class CreateAndDestroyGatewaySenderCommandsDUnitTest extends WANCommandTestBase {
  /**
   * GatewaySender with all default attributes
   */
  @Test
  public void testCreateDestroyGatewaySenderWithDefault() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testCreateDestroyGatewaySenderWithDefault stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(5, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySenderWithDefault failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm4.invoke(() -> verifySenderState("ln", true, false));
    vm5.invoke(() -> verifySenderState("ln", true, false));
    doDestroyAndVerifyGatewaySender("ln", null, null, "testCreateDestroyGatewaySenderWithDefault",
        Arrays.asList(vm3, vm4, vm5), 5, false);
  }

  /**
   * + * GatewaySender with given attribute values +
   */
  @Test
  public void testCreateDestroyGatewaySender() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

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
        + CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY + "=THREAD";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateDestroyGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(5, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));
    vm3.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, null, null));
    vm4.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, null, null));
    vm5.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, null, null));
    doDestroyAndVerifyGatewaySender("ln", null, null, "testCreateDestroyGatewaySender",
        Arrays.asList(vm3, vm4, vm5), 5, false);
  }

  /**
   * GatewaySender with given attribute values and event filters.
   */
  @Test
  public void testCreateDestroyGatewaySenderWithGatewayEventFilters() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

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
        + CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY + "=THREAD" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER
        + "=org.apache.geode.cache30.MyGatewayEventFilter1,org.apache.geode.cache30.MyGatewayEventFilter2";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateDestroyGatewaySenderWithGatewayEventFilters stringResult : "
          + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(5, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail(
          "testCreateDestroyGatewaySenderWithGatewayEventFilters failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));
    List<String> eventFilters = new ArrayList<String>();
    eventFilters.add("org.apache.geode.cache30.MyGatewayEventFilter1");
    eventFilters.add("org.apache.geode.cache30.MyGatewayEventFilter2");
    vm3.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, eventFilters, null));
    vm4.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, eventFilters, null));
    vm5.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, eventFilters, null));
    doDestroyAndVerifyGatewaySender("ln", null, null,
        "testCreateDestroyGatewaySenderWithGatewayEventFilters", Arrays.asList(vm3, vm4, vm5), 5,
        false);
  }

  /**
   * GatewaySender with given attribute values and transport filters.
   */
  @Test
  public void testCreateDestroyGatewaySenderWithGatewayTransportFilters() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

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
        + CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY + "=THREAD" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER
        + "=org.apache.geode.cache30.MyGatewayTransportFilter1";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter()
          .info("testCreateDestroyGatewaySenderWithGatewayTransportFilters stringResult : "
              + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(5, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail(
          "testCreateDestroyGatewaySenderWithGatewayTransportFilters failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));
    List<String> transportFilters = new ArrayList<String>();
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter1");
    vm3.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, null, transportFilters));
    vm4.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, null, transportFilters));
    vm5.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, null, transportFilters));
    doDestroyAndVerifyGatewaySender("ln", null, null,
        "testCreateDestroyGatewaySenderWithGatewayTransportFilters", Arrays.asList(vm3, vm4, vm5),
        5, false);
  }

  /**
   * GatewaySender with given attribute values on given member.
   */
  @Test
  public void testCreateDestroyGatewaySender_OnMember() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    final DistributedMember vm3Member = vm3.invoke(this::getMember);
    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.MEMBER + "=" + vm3Member.getId() + " --"
        + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
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
        + CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY + "=THREAD";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter()
          .info("testCreateDestroyGatewaySender_OnMember stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(1, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySender_OnMember failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm3.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, OrderPolicy.THREAD, null, null));
    doDestroyAndVerifyGatewaySender("ln", null, vm3Member,
        "testCreateDestroyGatewaySender_OnMember", Arrays.asList(vm3), 1, false);
  }

  /**
   * GatewaySender with given attribute values on given group
   */
  @Test
  public void testCreateDestroyGatewaySender_Group() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm4.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm5.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));

    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.GROUP + "=SenderGroup1" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=false" + " --"
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
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter()
          .info("testCreateDestroyGatewaySender_Group stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySender_Group failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm4.invoke(() -> verifySenderState("ln", true, false));
    vm5.invoke(() -> verifySenderState("ln", true, false));
    doDestroyAndVerifyGatewaySender("ln", "SenderGroup1", null,
        "testCreateDestroyGatewaySender_Group", Arrays.asList(vm3, vm4, vm5), 3, false);
  }

  /**
   * GatewaySender with given attribute values on given group. Only 2 of 3 members are part of the
   * group.
   */
  @Test
  public void testCreateDestroyGatewaySender_Group_Scenario2() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm4.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm5.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup2"));

    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.GROUP + "=SenderGroup1" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=false" + " --"
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
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testCreateDestroyGatewaySender_Group_Scenario2 stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(2, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySender_Group_Scenario2 failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm4.invoke(() -> verifySenderState("ln", true, false));
    doDestroyAndVerifyGatewaySender("ln", "SenderGroup1", null,
        "testCreateDestroyGatewaySender_Group_Scenario2", Arrays.asList(vm3, vm4), 2, false);
  }

  /**
   * + * Parallel GatewaySender with given attribute values +
   */
  @Test
  public void testCreateDestroyParallelGatewaySender() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

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
        + CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD + "=100";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter()
          .info("testCreateDestroyParallelGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(5, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyParallelGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));
    vm3.invoke(
        () -> verifySenderAttributes("ln", 2, true, true, 1000, socketReadTimeout, true, 1000, 5000,
            true, false, 1000, 100, GatewaySender.DEFAULT_DISPATCHER_THREADS, null, null, null));
    vm4.invoke(
        () -> verifySenderAttributes("ln", 2, true, true, 1000, socketReadTimeout, true, 1000, 5000,
            true, false, 1000, 100, GatewaySender.DEFAULT_DISPATCHER_THREADS, null, null, null));
    vm5.invoke(
        () -> verifySenderAttributes("ln", 2, true, true, 1000, socketReadTimeout, true, 1000, 5000,
            true, false, 1000, 100, GatewaySender.DEFAULT_DISPATCHER_THREADS, null, null, null));
    doDestroyAndVerifyGatewaySender("ln", null, null, "testCreateDestroyParallelGatewaySender",
        Arrays.asList(vm3, vm4), 5, true);
  }

  /**
   * doDestroyAndVerifyGatewaySender helper command.
   *
   * @param id if of the Gateway Sender
   * @param group Group for the GatewaySender
   * @param member Distributed Member for memeber id.
   * @param testName testName for the logging
   * @param vms list of vms where to verify the destroyed gateway sender
   * @param size command result.
   * @param isParallel true if parallel , false otherwise.
   */
  private void doDestroyAndVerifyGatewaySender(final String id, final String group,
      final DistributedMember member, final String testName, final List<VM> vms, final int size,
      final boolean isParallel) {
    String command =
        CliStrings.DESTROY_GATEWAYSENDER + " --" + CliStrings.DESTROY_GATEWAYSENDER__ID + "=" + id;
    if (group != null) {
      command += " --" + CliStrings.GROUP + "=" + group;
    }
    if (member != null) {
      command += " --" + CliStrings.MEMBER + "=" + member.getId();
    }
    final CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(testName + " stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(size, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender destroy failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail(testName + " failed as did not get CommandResult");
    }
    for (VM vm : vms) {
      vm.invoke(() -> verifySenderDestroyed(id, isParallel));
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
