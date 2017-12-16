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

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;

import java.util.Properties;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class DescribeRegionDUnitTest {

  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void describeRegionWithGatewayAndAsyncEventQueue() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    MemberVM sending_locator = lsRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + sending_locator.getPort() + "]");
    lsRule.startLocatorVM(2, props);

    lsRule.startServerVM(3, "group1", sending_locator.getPort());
    lsRule.startServerVM(4, "group2", sending_locator.getPort());

    gfsh.connectAndVerify(sending_locator);
    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 "
        + "--listener=org.apache.geode.internal.cache.wan.MyAsyncEventListener").statusIsSuccess();
    gfsh.executeAndAssertThat("create gateway-sender --id=sender1 --remote-distributed-system-id=2")
        .statusIsSuccess();
    sending_locator.waitTillAsyncEventQueuesAreReadyOnServers("queue1", 1);
    sending_locator.waitTilGatewaySendersAreReady(2);

    gfsh.executeAndAssertThat(
        "create region --name=region4 --type=REPLICATE --async-event-queue-id=queue1 --gateway-sender-id=sender1")
        .statusIsSuccess();

    gfsh.executeAndAssertThat("describe region --name=region4").statusIsSuccess()
        .containsOutput("gateway-sender-id", "sender1", "async-event-queue-id", "queue1");
  }
}
