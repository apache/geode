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

package org.apache.geode.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.management.internal.FederatingManager;
import org.apache.geode.management.internal.MemberMessenger;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({IntegrationTest.class, JMXTest.class})
public class FederatingManagerIntegrationTest {

  @Rule
  public ServerStarterRule serverRule = new ServerStarterRule();

  @Test
  public void testFederatingManagerConcurrency() throws Exception {
    serverRule.startServer();
    SystemManagementService service =
        (SystemManagementService) ManagementService
            .getExistingManagementService(serverRule.getCache());
    service.createManager();
    FederatingManager manager = service.getFederatingManager();

    MemberMessenger messenger = mock(MemberMessenger.class);
    manager.setMessenger(messenger);

    manager.startManager();

    InternalDistributedMember mockMember = mock(InternalDistributedMember.class);
    when(mockMember.getInetAddress()).thenReturn(InetAddress.getLocalHost());
    when(mockMember.getId()).thenReturn("member-1");

    for (int i = 0; i < 100; i++) {
      manager.addMember(mockMember);
    }

    Awaitility.waitAtMost(1, TimeUnit.SECONDS)
        .until(() -> serverRule.getCache().getAllRegions().size() > 1);
    assertThat(manager.getAndResetLatestException()).isNull();
  }
}
