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

import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

/**
 * Tests for the "list async-event-queues" gfsh command.
 *
 * <p>
 * Note on Command Name (Spring Shell 3.x Migration):
 * The correct command name is "list async-event-queues" (plural), as defined in
 * {@link CliStrings#LIST_ASYNC_EVENT_QUEUES}. Previous versions of these tests incorrectly
 * used "list async-event-queue" (singular).
 *
 * <p>
 * This bug surfaced after the Spring Shell 3.x migration (GEODE-10466) because:
 * <ul>
 * <li>Spring Shell 3.x has stricter command name matching and validation</li>
 * <li>Command names must exactly match the registered command key</li>
 * <li>Variations or shortened command names are no longer automatically resolved</li>
 * </ul>
 *
 * <p>
 * In Spring Shell 1.x, the command parser was more lenient and may have accepted
 * command name variations. With Spring Shell 3.x, attempting to execute
 * "list async-event-queue" results in: "Command 'list async-event-queue' not found".
 */
@Category({AEQTest.class})
public class ListAsyncEventQueuesCommandDUnitTest {

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule(locator::getJmxPort, jmxManager);

  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() {
    locator = lsRule.startLocatorVM(0);
    lsRule.startServerVM(1, "group1", locator.getPort());
    lsRule.startServerVM(2, "group2", locator.getPort());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void list() {
    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();

    gfsh.executeAndAssertThat("create async-event-queue --id=queue2 --group=group2 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 1);
    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue2", 1);

    // Note: Command must be "list async-event-queues" (plural), not "list async-event-queue".
    // See class-level Javadoc for explanation of Spring Shell 3.x migration impact.
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .tableHasRowCount(2).tableHasRowWithValues("Member", "ID", "server-1", "queue1")
        .tableHasRowWithValues("Member", "ID", "server-2", "queue2");

    // create another async event queue on the entire cluster, verify that the command will list all
    gfsh.executeAndAssertThat(
        "create async-event-queue --id=queue --listener=" + MyAsyncEventListener.class.getName())
        .statusIsSuccess();

    // Command name must be plural ("list async-event-queues")
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .tableHasRowCount(4).tableHasRowWithValues("Member", "ID", "server-1", "queue1")
        .tableHasRowWithValues("Member", "ID", "server-2", "queue2")
        .tableHasRowWithValues("Member", "ID", "server-1", "queue")
        .tableHasRowWithValues("Member", "ID", "server-2", "queue");

    // Test case where start-paused is set
    gfsh.executeAndAssertThat("create async-event-queue --id=queue3 --listener="
        + MyAsyncEventListener.class.getName() + " --pause-event-processing").statusIsSuccess();

    // locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue3", 1);
    // Command name must be plural ("list async-event-queues")
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .tableHasRowCount(6)
        .tableHasRowWithValues("Member", "ID", "Created with paused event processing",
            "Currently Paused", "server-1", "queue3",
            "true", "true")
        .tableHasRowWithValues("Member", "ID", "Created with paused event processing",
            "Currently Paused", "server-2", "queue2",
            "false", "false");


    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue1").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue2").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy async-event-queue --id=queue3").statusIsSuccess();
  }


  @Test
  public void ensureNoResultIsSuccess() {
    // Command name must be plural ("list async-event-queues"), not singular.
    // The actual command is defined in CliStrings.LIST_ASYNC_EVENT_QUEUES.
    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess()
        .containsOutput(CliStrings.LIST_ASYNC_EVENT_QUEUES__NO_QUEUES_FOUND_MESSAGE);
  }
}
