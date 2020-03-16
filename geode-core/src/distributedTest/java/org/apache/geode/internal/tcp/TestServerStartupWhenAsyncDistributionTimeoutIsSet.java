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
package org.apache.geode.internal.tcp;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.SerialAckedMessage;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class TestServerStartupWhenAsyncDistributionTimeoutIsSet implements Serializable {
  int serversToStart = 3;

  protected static InternalDistributedSystem system =
      InternalDistributedSystem.getConnectedInstance();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(serversToStart + 1);

  MemberVM locator;
  MemberVM server1;
  MemberVM server2;
  MemberVM server3;

  @Before
  public void setUp() throws Exception {
    locator = cluster.startLocatorVM(0);
  }

  private MemberVM startServer(final int vmIndex) {
    return cluster.startServerVM(
        vmIndex, s -> s.withConnectionToLocator(locator.getPort())
            .withProperty("async-distribution-timeout", "5"));
  }

  @Test
  public void testServerStartupDoesNotHangWhenAsyncDistributionTimeoutIsSet() {
    server1 = startServer(1);
    server2 = startServer(2);
    server3 = startServer(3);
    locator.invoke(() -> await().untilAsserted(() -> {
      assertThat(ConnectionTable.getNumSenderSharedConnections()).isEqualTo(3);
    }));

    locator.invoke(() -> await("for message to be sent").until(() -> {
      final SerialAckedMessage serialAckedMessage = new SerialAckedMessage();
      serialAckedMessage.send(system.getAllOtherMembers(), false);
      return true;
    }));
  }
}
