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
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifyReceiverCreationWithAttributes;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.management.internal.cli.commands.DestroyGatewayReceiverCommand;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({WanTest.class, GfshTest.class})
public class DestroyGatewayReceiverCommandDUnitTest {
  private MemberVM locatorSite1;
  private MemberVM server3, server4, server5;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(6);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = clusterStartupRule.startLocatorVM(1, props);

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locatorSite1);
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }


  private String createGatewayReceiverCommand(String manualStart) {
    return createGatewayReceiverCommand(manualStart, null);
  }

  private String createGatewayReceiverCommand(String manualStart, String memberOrGroup) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYRECEIVER)
        .addOption(CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART, manualStart)
        .addOption(CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS, "localhost")
        .addOption(CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT, "10000")
        .addOption(CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT, "11000")
        .addOption(CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS, "100000")
        .addOption(CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE, "512000");
    addOptionAndValue(csb, memberOrGroup);
    return csb.toString();
  }

  private void addOptionAndValue(CommandStringBuilder csb, String optionAndValue) {
    if (StringUtils.isNotBlank(optionAndValue)) {
      String[] memberOption = optionAndValue.split(":");
      if (memberOption.length == 1) {
        csb.addOption(memberOption[0]);
      } else {
        csb.addOption(memberOption[0], memberOption[1]);
      }
    }
  }

  private void verifyConfigHasGatewayReceiver(String groupName) {
    locatorSite1.invoke(() -> {
      String sharedConfigXml = ClusterStartupRule.getLocator().getConfigurationPersistenceService()
          .getConfiguration(groupName).getCacheXmlContent();
      assertThat(sharedConfigXml).contains("<gateway-receiver");
    });
  }

  private void verifyConfigDoesNotHaveGatewayReceiver(String groupName) {
    locatorSite1.invoke(() -> {
      Configuration groupConfig = ClusterStartupRule.getLocator()
          .getConfigurationPersistenceService().getConfiguration(groupName);
      String sharedConfigXml = groupConfig == null ? "" : groupConfig.getCacheXmlContent();
      // Null or emnpty XML doesn't have gateway-receiver element, so it's OK
      if (StringUtils.isNotEmpty(sharedConfigXml)) {
        assertThat(sharedConfigXml).doesNotContain("<gateway-receiver");
      }
    });
  }

  @Test
  public void destroyStartedGatewayReceiverOnAllMembers() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = clusterStartupRule.startServerVM(3, locator1Port);
    server4 = clusterStartupRule.startServerVM(4, locator1Port);
    server5 = clusterStartupRule.startServerVM(5, locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false")).statusIsSuccess()
        .hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3", "server-4", "server-5")
        .hasColumn("Message").containsExactlyInAnyOrder(
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider
        .invokeInEveryMember(
            () -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
                512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS),
            server3, server4, server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .doesNotContainOutput("change is not persisted")
        .hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3", "server-4", "server-5");
    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3, server4,
        server5);
  }

  @Test
  public void destroyStartedGatewayReceiver_destroysReceiverOnlyOnSpecificMembers() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = clusterStartupRule.startServerVM(3, locator1Port);
    server4 = clusterStartupRule.startServerVM(4, locator1Port);
    server5 = clusterStartupRule.startServerVM(5, locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false")).statusIsSuccess()
        .hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3", "server-4", "server-5")
        .hasColumn("Message").containsExactlyInAnyOrder(
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");
    verifyConfigHasGatewayReceiver("cluster");

    VMProvider
        .invokeInEveryMember(
            () -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
                512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS),
            server3, server4, server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.MEMBER, server3.getName());
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3");
    verifyConfigHasGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3);
    VMProvider.invokeInEveryMember(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000,
        "localhost", 100000, 512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS), server4,
        server5);
  }

  @Test
  public void destroyOnCluster_receiverExistsOnSubsetOfMembers_confgIsUpdated() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = clusterStartupRule.startServerVM(3, locator1Port);
    server4 = clusterStartupRule.startServerVM(4, locator1Port);
    server5 = clusterStartupRule.startServerVM(5, locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false")).statusIsSuccess()
        .hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3", "server-4", "server-5")
        .hasColumn("Message").containsExactlyInAnyOrder(
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");
    verifyConfigHasGatewayReceiver("cluster");

    VMProvider
        .invokeInEveryMember(
            () -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
                512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS),
            server3, server4, server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.MEMBER, server3.getName());
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3");
    verifyConfigHasGatewayReceiver("cluster");

    csb = new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3", "server-4", "server-5");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3, server4,
        server5);
  }

  @Test
  public void destroyOnCluster_receiverExistsInConfigButNotOnMembers_errorConfigNotUpdated() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = clusterStartupRule.startServerVM(3, locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false")).statusIsSuccess()
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3")
        .hasColumn("Message").containsExactlyInAnyOrder(
            "GatewayReceiver created on member \"server-3\"");
    verifyConfigHasGatewayReceiver("cluster");

    VMProvider
        .invokeInEveryMember(
            () -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
                512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS),
            server3/* , server4, server5 */);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.MEMBER, server3.getName());
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3");
    verifyConfigHasGatewayReceiver("cluster");

    csb = new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER);
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .doesNotContainOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3");
    verifyConfigHasGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3);
  }

  @Test
  public void destroyOnCluster_receiverExistsInConfigButNotOnMembers_ifExistsConfigIsUpdated() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = clusterStartupRule.startServerVM(3, locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false")).statusIsSuccess()
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3")
        .hasColumn("Message").containsExactlyInAnyOrder(
            "GatewayReceiver created on member \"server-3\"");
    verifyConfigHasGatewayReceiver("cluster");

    VMProvider
        .invokeInEveryMember(
            () -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
                512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS),
            server3/* , server4, server5 */);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.MEMBER, server3.getName());
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3");
    verifyConfigHasGatewayReceiver("cluster");

    csb = new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
        .addOption(CliStrings.IFEXISTS);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .doesNotContainOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3);
  }

  @Test
  public void destroyUnstartedGatewayReceiver_destroysReceiverOnlyOnSpecificMembers() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = clusterStartupRule.startServerVM(3, locator1Port);
    server4 = clusterStartupRule.startServerVM(4, locator1Port);
    server5 = clusterStartupRule.startServerVM(5, locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("true")).statusIsSuccess()
        .hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3", "server-4", "server-5")
        .hasColumn("Message").containsExactlyInAnyOrder(
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider
        .invokeInEveryMember(
            () -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
                512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS),
            server3, server4, server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.MEMBER, server3.getName());
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3");
    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3);
    VMProvider.invokeInEveryMember(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000,
        "localhost", 100000, 512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS), server4,
        server5);
  }

  @Test
  public void destroyWithoutGatewayReceiverOnMember_isError() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = clusterStartupRule.startServerVM(3, locator1Port);
    server4 = clusterStartupRule.startServerVM(4, locator1Port);
    server5 = clusterStartupRule.startServerVM(5, locator1Port);

    gfsh.executeAndAssertThat(
        createGatewayReceiverCommand("false", CliStrings.MEMBER + ":server-4,server-5"))
        .statusIsSuccess().hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-4", "server-5")
        .hasColumn("Message").containsExactlyInAnyOrder(
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");
    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.MEMBER, server3.getName());
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3");
  }

  @Test
  public void destroyGatewayReceiverClusterNotInConfig_isError_noNotPersistMessage() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = clusterStartupRule.startServerVM(3, locator1Port);
    server4 = clusterStartupRule.startServerVM(4, locator1Port);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER);
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .doesNotContainOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3", "server-4");
  }

  @Test
  public void destroyIfExistsWithoutGatewayReceivers_isIgnored() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = clusterStartupRule.startServerVM(3, locator1Port);
    server4 = clusterStartupRule.startServerVM(4, locator1Port);
    server5 = clusterStartupRule.startServerVM(5, locator1Port);

    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3, server4,
        server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.IFEXISTS, "true").addOption(CliStrings.MEMBER, server3.getName());
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3")
        .hasColumn("Status").containsExactlyInAnyOrder("IGNORED");
  }

  @Test
  public void destroyGatewayReceiverOnGroup_destroysReceiversOnAllGroupMembers() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = startServerWithGroups(3, "Grp1", locator1Port);
    server4 = startServerWithGroups(4, "Grp1", locator1Port);
    server5 = startServerWithGroups(5, "Grp2", locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false", CliStrings.GROUP + ":Grp1"))
        .statusIsSuccess().hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3", "server-4")
        .hasColumn("Message").containsExactlyInAnyOrder(
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"");
    verifyConfigHasGatewayReceiver("Grp1");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000,
        "localhost", 100000, 512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS), server3,
        server4);
    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.GROUP, "Grp1");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .doesNotContainOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3", "server-4");
    verifyConfigDoesNotHaveGatewayReceiver("Grp1");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3, server4);
  }

  @Test
  public void destroyGatewayReceiverOnListOfGroups_destroysReceiversOnMembersOfAllGroups() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = startServerWithGroups(3, "Grp1", locator1Port);
    server4 = startServerWithGroups(4, "Grp2", locator1Port);
    server5 = startServerWithGroups(5, "Grp3", locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false", CliStrings.GROUP + ":Grp1"))
        .statusIsSuccess().hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3")
        .hasColumn("Message")
        .containsExactlyInAnyOrder("GatewayReceiver created on member \"server-3\"");
    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false", CliStrings.GROUP + ":Grp2"))
        .statusIsSuccess().hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-4")
        .hasColumn("Message")
        .containsExactlyInAnyOrder("GatewayReceiver created on member \"server-4\"");
    verifyConfigHasGatewayReceiver("Grp1");
    verifyConfigHasGatewayReceiver("Grp2");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000,
        "localhost", 100000, 512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS), server3,
        server4);
    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.GROUP, "Grp1,Grp2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .doesNotContainOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3", "server-4");
    verifyConfigDoesNotHaveGatewayReceiver("Grp1");
    verifyConfigDoesNotHaveGatewayReceiver("Grp2");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3, server4);
  }

  @Test
  public void destroyGatewayReceiverOnGroup_destroysReceiversOnlyOnListedGroup() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = startServerWithGroups(3, "Grp1", locator1Port);
    server4 = startServerWithGroups(4, "Grp2", locator1Port);
    server5 = startServerWithGroups(5, "Grp3", locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false", CliStrings.GROUP + ":Grp1"))
        .statusIsSuccess().hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3")
        .hasColumn("Message")
        .containsExactlyInAnyOrder("GatewayReceiver created on member \"server-3\"");
    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false", CliStrings.GROUP + ":Grp2"))
        .statusIsSuccess().hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-4")
        .hasColumn("Message")
        .containsExactlyInAnyOrder("GatewayReceiver created on member \"server-4\"");
    verifyConfigHasGatewayReceiver("Grp1");
    verifyConfigHasGatewayReceiver("Grp2");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000,
        "localhost", 100000, 512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS), server3,
        server4);
    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.GROUP, "Grp1");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .doesNotContainOutput("change is not persisted")
        .hasTableSection().hasColumn("Member").containsExactlyInAnyOrder("server-3");
    verifyConfigDoesNotHaveGatewayReceiver("Grp1");
    verifyConfigHasGatewayReceiver("Grp2");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000,
        "localhost", 100000, 512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS), server4);
    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3);
  }

  @Test
  public void destroyGatewayReceiverDoesNotExistInMultipleGroups_errorsGroupsWithoutReceviers() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = startServerWithGroups(3, "Grp1", locator1Port);
    server4 = startServerWithGroups(4, "Grp2", locator1Port);
    server5 = startServerWithGroups(5, "Grp3", locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false", CliStrings.GROUP + ":Grp2"))
        .statusIsSuccess().hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-4")
        .hasColumn("Message")
        .containsExactlyInAnyOrder("GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"");
    verifyConfigHasGatewayReceiver("Grp2");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");
    verifyConfigDoesNotHaveGatewayReceiver("Grp1");
    verifyConfigDoesNotHaveGatewayReceiver("Grp3");

    VMProvider.invokeInEveryMember(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000,
        "localhost", 100000, 512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS), server4);
    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3, server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.GROUP, "Grp1,Grp2,Grp3");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .doesNotContainOutput("change is not persisted")
        .hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3", "server-4", "server-5");
    verifyConfigDoesNotHaveGatewayReceiver("Grp1");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3, server4);
  }

  @Test
  public void destroyGatewayReceiverDoesNotExistInMultipleGroups_ifExistsIgnoresGroupsWithoutReceviers() {
    Integer locator1Port = locatorSite1.getPort();
    server3 = startServerWithGroups(3, "Grp1", locator1Port);
    server4 = startServerWithGroups(4, "Grp2", locator1Port);
    server5 = startServerWithGroups(5, "Grp3", locator1Port);

    gfsh.executeAndAssertThat(createGatewayReceiverCommand("false", CliStrings.GROUP + ":Grp2"))
        .statusIsSuccess().hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-4")
        .hasColumn("Message")
        .containsExactlyInAnyOrder("GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"");
    verifyConfigHasGatewayReceiver("Grp2");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");
    verifyConfigDoesNotHaveGatewayReceiver("Grp1");
    verifyConfigDoesNotHaveGatewayReceiver("Grp3");

    VMProvider.invokeInEveryMember(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000,
        "localhost", 100000, 512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS), server4);
    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3, server5);

    CommandStringBuilder csb =
        new CommandStringBuilder(DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER)
            .addOption(CliStrings.IFEXISTS, "true").addOption(CliStrings.GROUP, "Grp1,Grp2,Grp3");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .doesNotContainOutput("change is not persisted")
        .hasTableSection().hasColumn("Member")
        .containsExactlyInAnyOrder("server-3", "server-4", "server-5")
        .hasAnyRow().contains("Member", "Status", "server-3", "IGNORED")
        .hasAnyRow().contains("Member", "Status", "server-5", "IGNORED");
    verifyConfigDoesNotHaveGatewayReceiver("Grp1");
    verifyConfigDoesNotHaveGatewayReceiver("cluster");

    VMProvider.invokeInEveryMember(WANCommandUtils::verifyReceiverDoesNotExist, server3, server4);
  }
}
