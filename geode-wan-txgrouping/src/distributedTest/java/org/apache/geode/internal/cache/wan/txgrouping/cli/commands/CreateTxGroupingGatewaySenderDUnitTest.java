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

package org.apache.geode.internal.cache.wan.txgrouping.cli.commands;

import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class CreateTxGroupingGatewaySenderDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  MemberVM locator;

  @Before
  public void before() throws Exception {
    locator = cluster.startLocatorVM(0);
    cluster.startServerVM(1, new Properties(), locator.getPort());
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void createTxGroupingParallelGatewaySender() {
    addIgnoredException("could not get remote locator");

    String createCommandString =
        "create gateway-sender --id=sender1 --remote-distributed-system-id=1 --parallel --group-transaction-events=true";

    // Check command status and output
    CommandResultAssert createCommand =
        gfsh.executeAndAssertThat(createCommandString).statusIsSuccess();
    createCommand
        .hasTableSection()
        .hasColumn("Member")
        .containsExactly("server-1");


    String listCommandString = "list gateways --senders-only";
    CommandResultAssert listCommand =
        gfsh.executeAndAssertThat(listCommandString).statusIsSuccess();

    listCommand
        .hasTableSection()
        .hasColumn("GatewaySender Id")
        .containsExactly("sender1");

    listCommand
        .hasTableSection()
        .hasColumn("Type")
        .containsExactly("Parallel");
  }

  @Test
  public void createTxGroupingSerialGatewaySender() {
    addIgnoredException("could not get remote locator");

    String createCommandString =
        "create gateway-sender --id=sender1 --remote-distributed-system-id=1 --dispatcher-threads=1 --group-transaction-events=true";

    // Check command status and output
    CommandResultAssert createCommand =
        gfsh.executeAndAssertThat(createCommandString).statusIsSuccess();
    createCommand
        .hasTableSection()
        .hasColumn("Member")
        .containsExactly("server-1");


    String listCommandString = "list gateways --senders-only";
    CommandResultAssert listCommand =
        gfsh.executeAndAssertThat(listCommandString).statusIsSuccess();

    listCommand
        .hasTableSection()
        .hasColumn("GatewaySender Id")
        .containsExactly("sender1");

    listCommand
        .hasTableSection()
        .hasColumn("Type")
        .containsExactly("Serial");
  }
}
