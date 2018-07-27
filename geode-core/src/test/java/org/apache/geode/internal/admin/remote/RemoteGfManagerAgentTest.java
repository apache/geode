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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.internal.admin.GfManagerAgentConfig;
import org.apache.geode.internal.logging.InternalLogWriter;


public class RemoteGfManagerAgentTest {

  private RemoteGfManagerAgent mockConnectedAgent(GfManagerAgentConfig config) {
    RemoteGfManagerAgent agent = spy(new RemoteGfManagerAgent(config));
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(system.isConnected()).thenReturn(true);
    when(system.getCancelCriterion()).thenReturn(mock(Services.Stopper.class));
    agent.system = system;
    agent.connected = true;
    return agent;
  }

  @Test
  public void removeAgentAndDisconnectWouldNotThrowNPE()
      throws InterruptedException, ExecutionException {
    InternalDistributedMember member;
    member = mock(InternalDistributedMember.class);
    Future future = mock(Future.class);

    GfManagerAgentConfig config = mock(GfManagerAgentConfig.class);
    when(config.getTransport()).thenReturn(mock(RemoteTransportConfig.class));
    when(config.getLogWriter()).thenReturn(mock(InternalLogWriter.class));

    int count = 10;

    for (int i = 0; i < count; i++) {
      RemoteGfManagerAgent agent = mockConnectedAgent(config);
      Map membersMap = new HashMap();
      membersMap.put(member, future);
      agent.membersMap = membersMap;
      ExecutorService es = Executors.newFixedThreadPool(2);

      // removeMember accesses the InternalDistributedSystem
      Future<?> future1 = es.submit(() -> {
        agent.removeMember(member);
      });

      // disconnect sets the InternalDistributedSystem to null
      Future<?> future2 = es.submit(() -> {
        agent.disconnect();
      });

      future1.get();
      future2.get();
    }
  }
}
