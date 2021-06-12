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

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.alerting.internal.spi.AlertingIOException;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

public class RestartOfMemberDistributedTest {

  private static final int locator1 = 0;
  private static final int server1 = 1;
  private static final int locator2 = 2;
  private static final int server2 = 3;
  private int locatorPort1;
  private int locatorPort2;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Before
  public void before() {
    Properties properties = createProperties();

    clusterStartupRule.startLocatorVM(locator1, properties);
    locatorPort1 = clusterStartupRule.getMember(locator1).getPort();
    clusterStartupRule.startServerVM(server1, properties, locatorPort1);

    clusterStartupRule.startLocatorVM(locator2, properties, locatorPort1);
    locatorPort2 = clusterStartupRule.getMember(locator2).getPort();
    clusterStartupRule.startServerVM(server2, properties, locatorPort1);

    addIgnoredException(ForcedDisconnectException.class.getName());
    addIgnoredException(MemberDisconnectedException.class.getName());
    addIgnoredException(AlertingIOException.class);
    addIgnoredException("Possible loss of quorum due to the loss");
    addIgnoredException("Received invalid result from");
  }

  @Test
  public void exCoordinatorJoiningQuorumDoesNotThrowNullPointerException() {
    clusterStartupRule.crashVM(locator1);
    clusterStartupRule.crashVM(server1);

    clusterStartupRule.startLocatorVM(locator1, locatorPort1, createProperties(), locatorPort2);
    clusterStartupRule.startServerVM(server1, createProperties(), locatorPort2);

    assertThatCode(() -> clusterStartupRule.getMember(locator2).waitTilFullyReconnected())
        .doesNotThrowAnyException();
  }

  private static Properties createProperties() {
    Properties properties = new Properties();
    properties.setProperty(DISABLE_AUTO_RECONNECT, "false");
    properties.setProperty(MAX_WAIT_TIME_RECONNECT, "1000");
    properties.setProperty(MEMBER_TIMEOUT, "2000");
    return properties;
  }
}
