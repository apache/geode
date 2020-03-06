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

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MemberStarterRule;

@Category({AEQTest.class})
public class CreateAsyncEventQueueCommandDUnitTest {


  public static final String COMMAND = "create async-event-queue ";
  public static final String VALID_COMMAND =
      COMMAND + "--listener=" + MyAsyncEventListener.class.getName();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator, server;

  @Test
  @SuppressWarnings("deprecation")
  public void createQueueWithInvalidClass() throws Exception {
    server = lsRule.startServerVM(0, MemberStarterRule::withJMXManager);
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    IgnoredException
        .addIgnoredException("java.lang.ClassNotFoundException: class.that.does.not.Exist");
    gfsh.executeAndAssertThat(COMMAND + " --id=queue --listener=class.that.does.not.Exist")
        .statusIsError().tableHasRowWithValues("Member", "Status", "Message", "server-0", "ERROR",
            " java.lang.ClassNotFoundException: class.that.does.not.Exist");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void createQueueWithoutCC() throws Exception {
    server = lsRule.startServerVM(0, MemberStarterRule::withJMXManager);
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue").statusIsSuccess()
        .containsOutput(CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED)
        .tableHasColumnWithExactValuesInAnyOrder("Status", "OK").tableHasRowCount(1);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void create_async_event_queue() throws Exception {
    locator = lsRule.startLocatorVM(0);
    lsRule.startServerVM(1, "group1", locator.getPort());
    lsRule.startServerVM(2, "group2", locator.getPort());
    gfsh.connectAndVerify(locator);
    // verify a simple create aeq command
    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue").statusIsSuccess()
        .tableHasRowCount(2)
        .tableHasRowWithValues("Member", "Status", "Message", "server-1", "OK", "Success")
        .tableHasRowWithValues("Member", "Status", "Message", "server-2", "OK", "Success");

    IgnoredException
        .addIgnoredException("java.lang.IllegalStateException: A GatewaySender with id "
            + "AsyncEventQueue_queue is already defined in this cache.");
    // create a queue with the same id would result in failure
    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue").statusIsError()
        .tableHasRowCount(2)
        .tableHasColumnWithExactValuesInAnyOrder("Status", "ERROR", "ERROR")
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            " java.lang.IllegalStateException: A GatewaySender with id AsyncEventQueue_queue is already defined in this cache.",
            " java.lang.IllegalStateException: A GatewaySender with id AsyncEventQueue_queue is already defined in this cache.");

    gfsh.executeAndAssertThat("create disk-store --name=diskStore2 --dir=diskstore")
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    // create another queue with different configuration
    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue2 --group=group2 "
        + "--batch-size=1024 --persistent --disk-store=diskStore2 "
        + "--max-queue-memory=512 --listener-param=param1,param2#value2").statusIsSuccess()
        .tableHasRowCount(1);


    // list the queue to verify the result
    gfsh.executeAndAssertThat("list async-event-queue").statusIsSuccess()
        .tableHasRowCount(3).tableHasRowWithValues("Member", "ID", "Batch Size",
            "Persistent", "Disk Store", "Max Memory", "server-2", "queue2", "1024", "true",
            "diskStore2", "512");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void create_paused_async_event_queue() throws Exception {
    locator = lsRule.startLocatorVM(0);
    lsRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);

    // create queue without start paused set
    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue1 "
        + "--batch-size=1024 --max-queue-memory=512 --listener-param=param1,param2#value2 ")
        .statusIsSuccess().tableHasRowCount(1);


    // list the queue to verify the the queue has start paused set to false
    gfsh.executeAndAssertThat("list async-event-queue").statusIsSuccess()
        .tableHasRowCount(1).tableHasRowWithValues("Member", "ID", "Batch Size",
            "Persistent", "Disk Store", "Max Memory", "Created with paused event processing",
            "Currently Paused", "server-1",
            "queue1", "1024", "false",
            "null", "512", "false", "false");

    // create queue with start paused set
    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue2 "
        + "--batch-size=1024 --max-queue-memory=512 --listener-param=param1,param2#value2 --pause-event-processing")
        .statusIsSuccess().tableHasRowCount(1);


    // list the queue to verify the the queue has start paused set to true
    gfsh.executeAndAssertThat("list async-event-queue").statusIsSuccess()
        .tableHasRowCount(2).tableHasRowWithValues("Member", "ID", "Batch Size",
            "Persistent", "Disk Store", "Max Memory", "Created with paused event processing",
            "Currently Paused", "server-1",
            "queue2", "1024", "false",
            "null", "512", "true", "true");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void create_queue_updates_cc() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server = lsRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      assertThat(service.getConfiguration("cluster").getCacheXmlContent()).isNull();
    });

    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue").statusIsSuccess()
        .tableHasRowCount(1)
        .tableHasRowWithValues("Member", "Status", "Message", "server-1", "OK", "Success");

    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration configuration = service.getConfiguration("cluster");
      assertThat(configuration.getCacheXmlContent()).contains("id=\"queue\"");
    });
  }
}
