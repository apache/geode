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

package org.apache.geode.management.internal.configuration;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class ClusterConfigLocatorRestartDUnitTest {

  private MemberVM locator1;
  private MemberVM server1;
  private MemberVM server2;

  @Rule
  public ClusterStartupRule rule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void setup() throws Exception {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.MAX_WAIT_TIME_RECONNECT, "5000");
    // TODO: Need an actual port because reconnect doesn't work with an ephemeral port -
    // i.e. if we use 0 initially. Fix this.
    locator1 = rule.startLocatorVM(0, props, AvailablePortHelper.getRandomAvailableTCPPort());

    server1 = rule.startServerVM(1, props, locator1.getPort());
    server2 = rule.startServerVM(2, props, locator1.getPort());
  }

  @Test
  public void serverRestartsAfterLocatorReconnects() throws Exception {
    server2.invokeAsync(() -> MembershipManagerHelper
        .crashDistributedSystem(InternalDistributedSystem.getConnectedInstance()));
    locator1.invokeAsync(() -> MembershipManagerHelper
        .crashDistributedSystem(InternalDistributedSystem.getConnectedInstance()));

    // Wait some time to reconnect
    Thread.sleep(10000);

    rule.startServerVM(3, locator1.getPort());

    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat("list members").statusIsSuccess().tableHasColumnOnlyWithValues("Name",
        "locator-0", "server-1", "server-2", "server-3");
  }

}
