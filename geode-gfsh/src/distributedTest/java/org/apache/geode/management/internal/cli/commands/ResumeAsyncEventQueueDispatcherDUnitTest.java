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

import static org.apache.geode.management.internal.i18n.CliStrings.RESUME_ASYNCEVENTQUEUE;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ResumeAsyncEventQueueDispatcherDUnitTest {

  public static final String CREATE_COMMAND =
      "create async-event-queue --listener=" + MyAsyncEventListener.class.getName();

  public static final String RESUME_COMMAND = RESUME_ASYNCEVENTQUEUE;

  public static final String LIST_COMMAND = "list async-event-queue";

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  @SuppressWarnings("deprecation")
  public void create_sync_event_queue() throws Exception {
    MemberVM locator = lsRule.startLocatorVM(0);
    lsRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);

    // create an AEQ with start paused set to false to verify proper behavior
    gfsh.executeAndAssertThat(CREATE_COMMAND + " --id=unpausedqueue --pause-event-processing=false")
        .statusIsSuccess()
        .tableHasRowCount(1)
        .tableHasRowWithValues("Member", "Status", "Message", "server-1", "OK", "Success");

    // verify our AEQ was created as expected
    gfsh.executeAndAssertThat(LIST_COMMAND).statusIsSuccess()
        .tableHasRowCount(1).tableHasRowWithValues("Member", "ID",
            "Created with paused event processing", "Currently Paused", "server-1", "unpausedqueue",
            "false",
            "false");

    // Issue the resume command and confirm it reports that the queue is already dispatching
    gfsh.executeAndAssertThat(RESUME_COMMAND + " --id=unpausedqueue").statusIsSuccess()
        .tableHasRowCount(1)
        .containsOutput("Async Event Queue \"unpausedqueue\" dispatching was not paused.");

    // create an AEQ with start paused set so we have a queue to unpause
    gfsh.executeAndAssertThat(CREATE_COMMAND + " --id=queue --pause-event-processing")
        .statusIsSuccess()
        .tableHasRowCount(1)
        .tableHasRowWithValues("Member", "Status", "Message", "server-1", "OK", "Success");

    // verify our AEQ was created as expected
    gfsh.executeAndAssertThat(LIST_COMMAND).statusIsSuccess()
        .tableHasRowCount(2).tableHasRowWithValues("Member", "ID",
            "Created with paused event processing", "Currently Paused", "server-1", "queue", "true",
            "true");

    // Issue the resume command and confirm it reports success
    gfsh.executeAndAssertThat(RESUME_COMMAND + " --id=queue").statusIsSuccess()
        .tableHasRowCount(1)
        .containsOutput("Async Event Queue \"queue\" dispatching was resumed successfully");

    // list the queue to verify the result
    gfsh.executeAndAssertThat(LIST_COMMAND).statusIsSuccess()
        .tableHasRowCount(2).tableHasRowWithValues("Member", "ID",
            "Created with paused event processing", "Currently Paused", "server-1", "queue", "true",
            "false");
  }
}
