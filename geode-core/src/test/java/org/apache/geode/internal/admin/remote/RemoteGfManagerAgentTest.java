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
package org.apache.geode.internal.admin.remote;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.GfManagerAgentConfig;
import org.apache.geode.internal.admin.remote.RemoteGfManagerAgent.DSConnectionDaemon;
import org.apache.geode.internal.admin.remote.RemoteGfManagerAgent.JoinProcessor;
import org.apache.geode.internal.admin.remote.RemoteGfManagerAgent.MyMembershipListener;
import org.apache.geode.internal.admin.remote.RemoteGfManagerAgent.SnapshotResultDispatcher;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RemoteGfManagerAgentTest {

  private RemoteGfManagerAgent agent;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    GfManagerAgentConfig config = mock(GfManagerAgentConfig.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);

    when(config.getLogWriter()).thenReturn(mock(InternalLogWriter.class));
    when(config.getTransport()).thenReturn(mock(RemoteTransportConfig.class));

    agent = new RemoteGfManagerAgent(config, props -> system, agent -> mock(JoinProcessor.class),
        agent -> mock(SnapshotResultDispatcher.class), agent -> mock(DSConnectionDaemon.class),
        agent -> mock(MyMembershipListener.class));
  }

  @Test
  public void removeAgentAndDisconnectDoesNotThrowNPE() throws Exception {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    Map<InternalDistributedMember, Future<RemoteApplicationVM>> membersMap = new HashMap<>();
    membersMap.put(member, mock(Future.class));
    agent.setMembersMap(membersMap);

    // removeMember accesses the InternalDistributedSystem
    Future<Void> removeMember = executorServiceRule.submit(() -> {
      agent.removeMember(member);
    });

    // disconnect sets the InternalDistributedSystem to null
    Future<Void> disconnect = executorServiceRule.submit(() -> {
      agent.disconnect();
    });

    removeMember.get();
    disconnect.get();
  }
}
