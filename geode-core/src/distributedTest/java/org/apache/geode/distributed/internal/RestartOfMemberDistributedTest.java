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
package org.apache.geode.distributed.internal;


import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;


public class RestartOfMemberDistributedTest {
  public List<MemberVM> locators = new ArrayList<>();
  public List<MemberVM> servers = new ArrayList<>();



  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Before
  public void before() {
    Properties properties = new Properties();

    locators.add(clusterStartupRule.startLocatorVM(0, properties));
    servers.add(clusterStartupRule.startServerVM(1, properties, locators.get(0).getPort()));
    locators.add(clusterStartupRule.startLocatorVM(2, properties, locators.get(0).getPort()));
    servers.add(clusterStartupRule.startServerVM(3, properties, locators.get(0).getPort()));
  }

  @After
  public void after() {
    servers.clear();
    locators.clear();
  }

  @Test
  public void exCoordinatorJoiningQuorumDoesNotThrowNullPointerException() {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(ForcedDisconnectException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException("Possible loss of quorum due to the loss");
    IgnoredException exp3 =
        IgnoredException.addIgnoredException("Received invalid result from");
    try {
      int locator2port = locators.get(1).getPort();
      Properties properties = new Properties();
      int locator0port = locators.get(0).getPort();
      clusterStartupRule.crashVM(1);
      clusterStartupRule.crashVM(0);
      await().until(() -> {
        clusterStartupRule.startLocatorVM(0, locator0port, properties, locator2port);
        return true;
      });
      clusterStartupRule.startServerVM(1, properties, locator2port);
      locators.get(1).waitTilFullyReconnected();
    } finally {
      exp1.remove();
      exp2.remove();
      exp3.remove();
    }
  }

}
