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
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category(UnitTest.class)
public class ListAsyncEventQueuesCommandDUnitTest {

  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule(locator::getJmxPort, jmxManager);

  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = lsRule.startLocatorVM(0);
    lsRule.startServerVM(1, "group1", locator.getPort());
    lsRule.startServerVM(2, "group2", locator.getPort());
  }

  @Test
  public void list() throws Exception {
    gfsh.executeAndAssertThat("list async-event-queue").statusIsSuccess()
        .containsOutput("No Async Event Queues Found");

    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();

    gfsh.executeAndAssertThat("create async-event-queue --id=queue2 --group=group2 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();

    locator.waitTillAsyncEventQueuesAreReadyOnServers("queue1", 1);
    locator.waitTillAsyncEventQueuesAreReadyOnServers("queue2", 1);

    gfsh.executeAndAssertThat("list async-event-queue").statusIsSuccess()
        .tableHasRowCount("Member", 2).tableHasRowWithValues("Member", "ID", "server-1", "queue1")
        .tableHasRowWithValues("Member", "ID", "server-2", "queue2");

    // create another async event queue on the entire cluster, verify that the command will list all
    gfsh.executeAndAssertThat(
        "create async-event-queue --id=queue --listener=" + MyAsyncEventListener.class.getName())
        .statusIsSuccess();

    gfsh.executeAndAssertThat("list async-event-queue").statusIsSuccess()
        .tableHasRowCount("Member", 4).tableHasRowWithValues("Member", "ID", "server-1", "queue1")
        .tableHasRowWithValues("Member", "ID", "server-2", "queue2")
        .tableHasRowWithValues("Member", "ID", "server-1", "queue")
        .tableHasRowWithValues("Member", "ID", "server-2", "queue");

  }
}
