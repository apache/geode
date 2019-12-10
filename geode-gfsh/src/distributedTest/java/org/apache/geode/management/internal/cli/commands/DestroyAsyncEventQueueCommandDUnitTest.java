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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({AEQTest.class})
public class DestroyAsyncEventQueueCommandDUnitTest {

  private static MemberVM locator, server1, server2, server3;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void setUp() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    server2 = lsRule.startServerVM(2, locator.getPort());
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void destroyAeq_returnsSuccess() {
    gfsh.executeAndAssertThat(
        "create async-event-queue --id=queue1 --listener=" + MyAsyncEventListener.class.getName())
        .statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 2);
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration config = service.getConfiguration("cluster");
      assertThat(config.getCacheXmlContent()).contains("id=\"queue1\"");
    });

    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1 ").statusIsSuccess();
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");

    // verify that aeq entry is deleted from cluster config
    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration config = service.getConfiguration("cluster");
      assertThat(config.getCacheXmlContent()).doesNotContain("id=\"queue1\"");
    });
  }

  @Test
  public void destroyAeqWhenQueueDoesNotExist_deafultReturnsError() {
    gfsh.executeAndAssertThat(
        "create async-event-queue --id=queue1 --listener=" + MyAsyncEventListener.class.getName())
        .statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 2);
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess();

    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1 ").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1 ").statusIsError();
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");
  }

  @Test
  public void destroyAeqWhenQueueDoesNotExist_withIfExistsReturnsSuccess() {
    gfsh.executeAndAssertThat(
        "create async-event-queue --id=queue1 --listener=" + MyAsyncEventListener.class.getName())
        .statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 2);
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess();

    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1 ").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1 --if-exists")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");

    // verify that aeq entry is deleted from cluster config
    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration config = service.getConfiguration("cluster");
      assertThat(config.getCacheXmlContent()).doesNotContain("id=\"queue1\"");
    });
  }

  @Test
  public void destroyAeqOnGroup_returnsSuccess() {
    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 1);

    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1 --group=group1")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");

    // verify that aeq entry is deleted from cluster config
    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration config = service.getConfiguration("group1");
      assertThat(config.getCacheXmlContent()).doesNotContain("id=\"queue1\"");
    });
  }

  @Test
  public void destroyAeqOnGroupThatDoesNotExisit_returnsError() {
    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 1);

    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1 --group=group2")
        .statusIsError().containsOutput(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess();

    // verify that aeq entry is not deleted from cluster config
    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration config = service.getConfiguration("group1");
      assertThat(config.getCacheXmlContent()).contains("id=\"queue1\"");
    });
  }

  @Test
  public void destroyAeq_selectsQueuesOnGroup_showsErrorForServersNotInGroup() {
    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 1);
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess();

    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1").statusIsSuccess()
        .hasTableSection()
        .hasRowSize(2)
        .hasAnyRow().contains("server-1", "OK", "Async event queue \"queue1\" destroyed")
        .hasAnyRow().contains("server-2", "ERROR", "Async event queue \"queue1\" not found");

    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");
  }

  @Test
  public void destroyAeq_selectsQueuesByGroup_returnsSuccess() {
    server3 = lsRule.startServerVM(3, "group3", locator.getPort());

    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();
    gfsh.executeAndAssertThat("create async-event-queue --id=queue3 --group=group3 --listener="
        + MyAsyncEventListener.class.getName())/* .statusIsSuccess() */;

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 1);
    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue3", 1);
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess();

    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1 --group=group1")
        .statusIsSuccess().containsOutput(String.format(
            DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, "queue1"));
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .hasTableSection().hasAnyRow().contains("server-3", "queue3");

    // verify that cluster config aeq entry for destroyed queue is deleted
    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      System.out.println("cluster config: " + service.getConfiguration("cluster"));
      Configuration config1 = service.getConfiguration("group1");
      assertThat(config1.getCacheXmlContent()).doesNotContain("id=\"queue1\"");
      Configuration config3 = service.getConfiguration("group3");
      assertThat(config3.getCacheXmlContent()).contains("id=\"queue3\"");
    });
  }
}
