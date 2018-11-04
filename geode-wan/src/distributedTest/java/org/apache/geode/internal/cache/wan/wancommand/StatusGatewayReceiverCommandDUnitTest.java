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
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.createAndStartReceiver;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMember;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewayReceiverMXBeanProxy;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
@SuppressWarnings("serial")
public class StatusGatewayReceiverCommandDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(8);

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locatorSite1;
  private MemberVM locatorSite2;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;
  private MemberVM server4;
  private MemberVM server5;

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = clusterStartupRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    locatorSite2 = clusterStartupRule.startLocatorVM(2, props);

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locatorSite1);
  }

  @Test
  public void testGatewayReceiverStatus() {
    int lnPort = locatorSite1.getPort();
    int nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    // server in Site 2 (New York)
    server4 = clusterStartupRule.startServerVM(6, nyPort);

    server1.invoke(() -> createAndStartReceiver(lnPort));
    server2.invoke(() -> createAndStartReceiver(lnPort));
    server3.invoke(() -> createAndStartReceiver(lnPort));

    server4.invoke(() -> createAndStartReceiver(nyPort));

    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server1.getVM()), true));
    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server2.getVM()), true));
    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server3.getVM()), true));

    locatorSite2.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server4.getVM()), true));

    String command = CliStrings.STATUS_GATEWAYRECEIVER;
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultModel resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE);
    List<String> status = resultData.getValuesInColumn(CliStrings.RESULT_STATUS);
    assertThat(status).hasSize(3);
    assertThat(status).doesNotContain(CliStrings.GATEWAY_NOT_RUNNING);

    server1.invoke(WANCommandUtils::stopReceivers);
    server2.invoke(WANCommandUtils::stopReceivers);
    server3.invoke(WANCommandUtils::stopReceivers);

    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server1.getVM()), false));
    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server2.getVM()), false));
    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server3.getVM()), false));

    command = CliStrings.STATUS_GATEWAYRECEIVER;
    cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE);
    status = resultData.getValuesInColumn(CliStrings.RESULT_STATUS);
    assertThat(status).hasSize(3);
    assertThat(status).doesNotContain(CliStrings.GATEWAY_RUNNING);
  }

  @Test
  public void testGatewayReceiverStatus_OnMember() {
    int lnPort = locatorSite1.getPort();
    int nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    // server in Site 2 (New York)
    server4 = clusterStartupRule.startServerVM(6, nyPort);

    server1.invoke(() -> createAndStartReceiver(lnPort));
    server2.invoke(() -> createAndStartReceiver(lnPort));
    server3.invoke(() -> createAndStartReceiver(lnPort));

    server4.invoke(() -> createAndStartReceiver(nyPort));

    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server1.getVM()), true));
    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server2.getVM()), true));
    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server3.getVM()), true));

    locatorSite2.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server4.getVM()), true));

    DistributedMember vm3Member = getMember(server1.getVM());
    String command =
        CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.MEMBER + "=" + vm3Member.getId();
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultModel resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE);
    List<String> status = resultData.getValuesInColumn(CliStrings.RESULT_STATUS);
    assertThat(status).hasSize(1);
    assertThat(status).doesNotContain(CliStrings.GATEWAY_NOT_RUNNING);

    server1.invoke(WANCommandUtils::stopReceivers);
    server2.invoke(WANCommandUtils::stopReceivers);
    server3.invoke(WANCommandUtils::stopReceivers);

    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server1.getVM()), false));
    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server2.getVM()), false));
    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server3.getVM()), false));

    command =
        CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.MEMBER + "=" + vm3Member.getId();
    cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();

    resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE);
    status = resultData.getValuesInColumn(CliStrings.RESULT_STATUS);
    assertThat(status).hasSize(1);
    assertThat(status).doesNotContain(CliStrings.GATEWAY_RUNNING);
  }

  @Test
  public void testGatewayReceiverStatus_OnGroups() {
    int lnPort = locatorSite1.getPort();
    int nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = startServerWithGroups(3, "RG1, RG2", lnPort);
    server2 = startServerWithGroups(4, "RG1, RG2", lnPort);
    server3 = startServerWithGroups(5, "RG1", lnPort);
    server4 = startServerWithGroups(6, "RG2", lnPort);

    // server in Site 2 (New York) - no group
    server5 = clusterStartupRule.startServerVM(7, nyPort);

    server1.invoke(() -> createAndStartReceiver(lnPort));
    server2.invoke(() -> createAndStartReceiver(lnPort));
    server3.invoke(() -> createAndStartReceiver(lnPort));
    server4.invoke(() -> createAndStartReceiver(lnPort));

    server5.invoke(() -> createAndStartReceiver(nyPort));

    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server1.getVM()), true));
    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server2.getVM()), true));
    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server3.getVM()), true));
    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server4.getVM()), true));

    locatorSite2.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server5.getVM()), true));

    String command = CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=RG1";
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultModel resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE);
    List<String> status = resultData.getValuesInColumn(CliStrings.RESULT_STATUS);
    assertThat(status).hasSize(3);
    assertThat(status).doesNotContain(CliStrings.GATEWAY_NOT_RUNNING);

    server1.invoke(WANCommandUtils::stopReceivers);
    server2.invoke(WANCommandUtils::stopReceivers);
    server3.invoke(WANCommandUtils::stopReceivers);

    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server1.getVM()), false));
    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server2.getVM()), false));
    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server3.getVM()), false));

    command = CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=RG1";
    cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE);
    status = resultData.getValuesInColumn(CliStrings.RESULT_STATUS);
    assertThat(status).hasSize(3);
    assertThat(status).doesNotContain(CliStrings.GATEWAY_RUNNING);
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }
}
