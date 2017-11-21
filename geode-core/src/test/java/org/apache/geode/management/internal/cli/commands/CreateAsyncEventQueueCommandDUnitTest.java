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
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category(DistributedTest.class)
public class CreateAsyncEventQueueCommandDUnitTest {


  public static final String COMMAND = "create async-event-queue ";
  public static final String VALID_COMMAND =
      COMMAND + "--listener=" + MyAsyncEventListener.class.getName();

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
  public void createQueueWithInvalidClass() throws GfJsonException {
    IgnoredException.addIgnoredException("java.lang.ClassNotFoundException: xyz");
    gfsh.executeAndAssertThat(COMMAND + "--id=queue --listener=xyz").statusIsSuccess()
        .tableHasRowCount("Member", 2)
        .tableHasRowWithValues("Member", "Result", "server-1",
            "ERROR: java.lang.ClassNotFoundException: xyz")
        .tableHasRowWithValues("Member", "Result", "server-2",
            "ERROR: java.lang.ClassNotFoundException: xyz");
  }

  @Test
  public void create() throws Exception {
    // verify a simple create aeq command
    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue").statusIsSuccess()
        .tableHasRowCount("Member", 2)
        .tableHasColumnWithExactValuesInAnyOrder("Result", "Success", "Success");

    IgnoredException
        .addIgnoredException("java.lang.IllegalStateException: A GatewaySender with id  "
            + "AsyncEventQueue_queue  is already defined in this cache.");
    // create a queue with the same id would result in failure
    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue").statusIsSuccess()
        .tableHasRowCount("Member", 2).tableHasColumnWithExactValuesInAnyOrder("Result",
            "ERROR: java.lang.IllegalStateException: A GatewaySender with id  AsyncEventQueue_queue  is already defined in this cache.",
            "ERROR: java.lang.IllegalStateException: A GatewaySender with id  AsyncEventQueue_queue  is already defined in this cache.");

    gfsh.executeAndAssertThat("create disk-store --name=diskStore2 --dir=diskstore");
    locator.waitTillDiskstoreIsReady("diskStore2", 2);

    // create another queue with different configuration
    gfsh.executeAndAssertThat(VALID_COMMAND + " --id=queue2 --group=group2 "
        + "--batch-size=1024 --persistent --disk-store=diskStore2 "
        + "--max-queue-memory=512 --listener-param=param1,param2#value2").statusIsSuccess()
        .tableHasRowCount("Member", 1);


    // list the queue to verify the result
    gfsh.executeAndAssertThat("list async-event-queue").statusIsSuccess()
        .tableHasRowCount("Member", 3).tableHasRowWithValues("Member", "ID", "Batch Size",
            "Persistent", "Disk Store", "Max Memory", "server-2", "queue2", "1024", "true",
            "diskStore2", "512");
  }
}
