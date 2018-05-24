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

import static org.apache.geode.distributed.ConfigurationPersistenceService.CLUSTER_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, AEQTest.class})
public class DestroyAsyncEventQueueCommandDUnitTest {

  private static MemberVM locator, server1, server2, server3;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule(2);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static final String group1 = "group1";
  private static final String group2 = "group2";

  private static final String clusterQueueId = "cluster-wide-queue";
  private static final String group1QueueId = "group-1-queue";
  private static final String group2QueueId = "group-2-queue";

  @Before
  public void setUp() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, group1, locator.getPort());
    server2 = lsRule.startServerVM(2, locator.getPort());
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void destroyAeq_returnsSuccess() {
    createClusterWideQueue();

    gfsh.executeAndAssertThat("destroy async-event-queue --id=" + clusterQueueId).statusIsSuccess()
        .tableHasRowWithValues("Member", "Status", "Message", server1.getName(), "OK",
            String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED,
                clusterQueueId))
        .tableHasRowWithValues("Member", "Status", "Message", server2.getName(), "OK",
            String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED,
                clusterQueueId));

    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");

    assertNotAeqInGroupConfig(CLUSTER_CONFIG, clusterQueueId);
  }

  @Test
  public void destroyAeq_WhenQueueDoesNotExist_defaultReturnsError() {
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");
    assertNotAeqInGroupConfig(CLUSTER_CONFIG, clusterQueueId);

    gfsh.executeAndAssertThat("destroy async-event-queue --id=" + clusterQueueId).statusIsError()
        .tableHasRowWithValues("Member", "Status", "Message", server1.getName(), "ERROR",
            String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
                clusterQueueId))
        .tableHasRowWithValues("Member", "Status", "Message", server2.getName(), "ERROR",
            String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
                clusterQueueId));
  }

  @Test
  public void destroyAeq_WhenQueueDoesNotExist_withIfExistsReturnsSuccess() {
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");
    assertNotAeqInGroupConfig(CLUSTER_CONFIG, clusterQueueId);

    gfsh.executeAndAssertThat("destroy async-event-queue --id=" + clusterQueueId + " --if-exists")
        .statusIsSuccess()
        .tableHasRowWithValues("Member", "Status", "Message", server1.getName(), "IGNORED",
            String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
                clusterQueueId))
        .tableHasRowWithValues("Member", "Status", "Message", server2.getName(), "IGNORED",
            String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
                clusterQueueId));
  }

  @Test
  public void destroyAeq_OnGroup_returnsSuccess() {
    createQueueOnGroup(group1QueueId, group1);

    gfsh.executeAndAssertThat("destroy async-event-queue --id=" + group1QueueId + " --group=group1")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");

    // verify that aeq entry is deleted from cluster config
    assertNotAeqInGroupConfig(group1, group1QueueId);
  }

  @Test
  public void destroyAeq_OnGroupThatDoesNotExist_returnsError() {
    createQueueOnGroup(group1QueueId, group1);

    gfsh.executeAndAssertThat(
        "destroy async-event-queue --id=" + group1QueueId + " --group=no-such-group")
        .statusIsError().containsOutput(CliStrings.NO_MEMBERS_FOUND_MESSAGE);

    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput(group1QueueId);

    assertAeqIsInGroupConfig(group1, group1QueueId);
  }

  @Test
  public void destroyAeq_destroyWithoutGroupDoesDestroyQueueCreatedOnGroup() {
    createQueueOnGroup(group1QueueId, group1);

    gfsh.executeAndAssertThat("destroy async-event-queue --id=" + group1QueueId).statusIsSuccess();

    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .doesNotContainOutput(group1QueueId);

    assertNotAeqInGroupConfig(group1, group1QueueId);
  }

  @Test
  public void destroyAeq_destroyWithoutGroupDoesDestroyQueueCreatedOnGroupAcrossMultipleConfigs() {
    server3 = lsRule.startServerVM(3, group2, locator.getPort());

    String queueIdOnGroups1and2 = "group-1-and-2-queue";

    createQueueOnGroup(queueIdOnGroups1and2, group1);
    createQueueOnGroup(queueIdOnGroups1and2, group2);

    gfsh.executeAndAssertThat("destroy async-event-queue --id=" + queueIdOnGroups1and2)
        .statusIsSuccess();

    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .doesNotContainOutput(queueIdOnGroups1and2);

    assertNotAeqInGroupConfig(group1, queueIdOnGroups1and2);
    assertNotAeqInGroupConfig(group2, queueIdOnGroups1and2);
  }

  @Test
  public void destroyAeq_selectsQueuesOnGroup_showsErrorForServersNotInGroup() {
    createQueueOnGroup(group1QueueId, group1);

    gfsh.executeAndAssertThat("destroy async-event-queue --id=" + group1QueueId).statusIsSuccess()
        .tableHasRowWithValues("Member", "Status", "Message", "server-1", "OK",
            String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED,
                group1QueueId))
        .tableHasRowWithValues("Member", "Status", "Message", "server-2", "ERROR",
            String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND,
                group1QueueId));

    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");

    assertNotAeqInGroupConfig(group1, group1QueueId);
  }

  @Test
  public void destroyAeq_selectsQueuesByGroup_returnsSuccess() {
    server3 = lsRule.startServerVM(3, group2, locator.getPort());

    createQueueOnGroup(group1QueueId, group1);
    createQueueOnGroup(group2QueueId, group2);

    gfsh.executeAndAssertThat(
        "destroy async-event-queue --id=" + group1QueueId + " --group=" + group1).statusIsSuccess()
        .containsOutput(
            String.format(DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED,
                group1QueueId));
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .tableHasRowWithValues("Member", "ID", "server-3", group2QueueId);

    // verify that cluster config aeq entry for destroyed queue is deleted
    assertNotAeqInGroupConfig(group1, group1QueueId);
    assertAeqIsInGroupConfig(group2, group2QueueId);
  }


  /**
   * Expects there to be exactly two servers in the cluster.
   */
  private void createClusterWideQueue() {
    System.out.println("Creating '" + clusterQueueId + "' on entire cluster.");
    gfsh.executeAndAssertThat("create async-event-queue --id=" + clusterQueueId + " --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();

    locator.waitTillAsyncEventQueuesAreReadyOnServers(clusterQueueId, 2);

    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess();

    assertAeqIsInGroupConfig(CLUSTER_CONFIG, clusterQueueId);
  }

  /**
   * Expects there to be only one server in the specified group.
   */
  private void createQueueOnGroup(String aeqId, String groupName) {
    System.out.println("Creating '" + aeqId + "' on group1.");

    gfsh.executeAndAssertThat("create async-event-queue --id=" + aeqId + " --group=" + groupName
        + " --listener=" + MyAsyncEventListener.class.getName()).statusIsSuccess();

    locator.waitTillAsyncEventQueuesAreReadyOnServers(aeqId, 1);

    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess();

    assertAeqIsInGroupConfig(groupName, aeqId);
  }


  private void assertNotAeqInGroupConfig(String groupName, String aeqId) {
    System.out.println("Confirming that queue '" + aeqId + "' IS in group '" + groupName + "'.");

    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration config = service.getConfiguration(groupName);
      if (config != null && config.getCacheXmlContent() != null) {
        assertThat(config.getCacheXmlContent()).doesNotContain("id=\"" + aeqId + "\"");
      }
      if (service.getCacheConfig(groupName) != null) {
        assertThat(service.getCacheConfig(groupName).getAsyncEventQueues()).isEmpty();
      }
    });
  }

  private void assertAeqIsInGroupConfig(String groupName, String aeqId) {
    System.out
        .println("Confirming that queue '" + aeqId + "' is not in group '" + groupName + "'.");

    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      InternalConfigurationPersistenceService service =
          internalLocator.getConfigurationPersistenceService();
      assertThat(service).isNotNull();
      Configuration config = service.getConfiguration(groupName);

      assertThat(config).isNotNull();
      assertThat(config.getCacheXmlContent()).isNotNull();
      assertThat(service.getCacheConfig(groupName)).isNotNull();

      assertThat(config.getCacheXmlContent()).contains("id=\"" + aeqId + "\"");
      assertThat(service.getCacheConfig(groupName).getAsyncEventQueues())
          .filteredOn(aeq -> aeq.getId().equals(aeqId)).isNotEmpty();
    });
  }
}
