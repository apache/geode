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
package org.apache.geode.internal.cache.tier.sockets.command;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;

public class PingTest {

  @Mock
  private Message message;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private Part targetServerPart;
  @Mock
  private InternalCache cache;
  @Mock
  private DistributionManager distributionManager;
  @Mock
  private Message errorResponseMessage;
  @Mock
  private ClientHealthMonitor clientHealthMonitor;

  private InternalDistributedMember myID;
  private InternalDistributedMember targetServer;

  @Mock
  private Message replyMessage;

  @Spy
  @InjectMocks
  private Ping ping;

  @Before
  public void setUp() throws Exception {
    ping = (Ping) Ping.getCommand();
    MockitoAnnotations.initMocks(this);

    when(ping.getClientHealthMonitor()).thenReturn(clientHealthMonitor);
    when(message.getNumberOfParts()).thenReturn(1);
    when(message.getPart(eq(0))).thenReturn(targetServerPart);
    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getReplyMessage()).thenReturn(replyMessage);
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
  }

  @Test
  public void testTargetServerAndCurrentServerIDAreEquals() throws Exception {

    myID = new InternalDistributedMember(new ServerLocation("localhost", 1));
    when(cache.getMyId()).thenReturn(myID);
    targetServer = new InternalDistributedMember(new ServerLocation("localhost", 1));
    when(targetServerPart.getObject()).thenReturn(targetServer);
    ping.cmdExecute(message, serverConnection, null, 0);

    verify(replyMessage).send(serverConnection);
    verify(ping, times(1)).getClientHealthMonitor();
  }

  @Test
  public void testTargetServerAndCurrentServerIDAreDifferentAndTargetServerIsASystemMember()
      throws Exception {

    myID = new InternalDistributedMember(new ServerLocation("localhost", 1));
    when(cache.getMyId()).thenReturn(myID);
    targetServer = new InternalDistributedMember(new ServerLocation("localhost", 2));
    when(targetServerPart.getObject()).thenReturn(targetServer);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    when(distributionManager.isCurrentMember(targetServer)).thenReturn(true);

    ping.cmdExecute(message, serverConnection, null, 0);

    verify(replyMessage).send(serverConnection);
    verify(ping, times(0)).getClientHealthMonitor();
  }

  @Test
  public void testTargetServerAndCurrentServerIDAreDifferentAndTargetServerIsNotASystemMember()
      throws Exception {

    myID = new InternalDistributedMember(new ServerLocation("localhost", 1));
    when(cache.getMyId()).thenReturn(myID);
    targetServer = new InternalDistributedMember(new ServerLocation("localhost", 2));
    when(targetServerPart.getObject()).thenReturn(targetServer);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    when(distributionManager.isCurrentMember(targetServer)).thenReturn(false);
    when(serverConnection.getErrorResponseMessage()).thenReturn(errorResponseMessage);

    ping.cmdExecute(message, serverConnection, null, 0);

    verify(errorResponseMessage, times(1)).send(serverConnection);
    verify(replyMessage, times(0)).send(serverConnection);
    verify(ping, times(0)).getClientHealthMonitor();
  }

  @Test
  public void testTargetServerAndCurrentServerAreEqualsButWithDifferentViewId() throws Exception {
    myID = new InternalDistributedMember(new ServerLocation("localhost", 1));
    myID.setVmViewId(1);
    when(cache.getMyId()).thenReturn(myID);
    targetServer = new InternalDistributedMember(new ServerLocation("localhost", 1));
    targetServer.setVmViewId(2);
    when(targetServerPart.getObject()).thenReturn(targetServer);
    when(serverConnection.getErrorResponseMessage()).thenReturn(errorResponseMessage);

    ping.cmdExecute(message, serverConnection, null, 0);

    verify(errorResponseMessage, times(1)).send(serverConnection);
    verify(replyMessage, times(0)).send(serverConnection);
    verify(ping, times(0)).getClientHealthMonitor();
  }
}
