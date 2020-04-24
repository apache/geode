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

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
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

  private InternalDistributedMember myID;
  private InternalDistributedMember targetServer;

  @Mock
  private Message replyMessage;

  @InjectMocks
  private Ping ping;

  @Before
  public void setUp() throws Exception {
    this.ping = (Ping) Ping.getCommand();
    MockitoAnnotations.initMocks(this);

    when(this.message.getNumberOfParts()).thenReturn(1);
    when(this.message.getPart(eq(0))).thenReturn(this.targetServerPart);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getReplyMessage()).thenReturn(this.replyMessage);
    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
  }

  @Test
  public void testTargetServerAndCurrentServerIDAreEquals() throws Exception {

    myID = new InternalDistributedMember(new ServerLocation("localhost", 1));
    when(this.cache.getMyId()).thenReturn(this.myID);
    targetServer = new InternalDistributedMember(new ServerLocation("localhost", 1));
    when(this.targetServerPart.getObject()).thenReturn(targetServer);
    this.ping.cmdExecute(this.message, this.serverConnection, null, 0);

    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void testTargetServerAndCurrentServerIDAreDifferentAndTargetServerIsASystemMember()
      throws Exception {

    myID = new InternalDistributedMember(new ServerLocation("localhost", 1));
    when(this.cache.getMyId()).thenReturn(this.myID);
    targetServer = new InternalDistributedMember(new ServerLocation("localhost", 2));
    when(this.targetServerPart.getObject()).thenReturn(targetServer);
    when(this.cache.getDistributionManager()).thenReturn(this.distributionManager);
    when(this.distributionManager.isCurrentMember(targetServer)).thenReturn(true);

    this.ping.cmdExecute(this.message, this.serverConnection, null, 0);

    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void testTargetServerAndCurrentServerIDAreDifferentAndTargetServerIsNotASystemMember()
      throws Exception {

    myID = new InternalDistributedMember(new ServerLocation("localhost", 1));
    when(this.cache.getMyId()).thenReturn(this.myID);
    targetServer = new InternalDistributedMember(new ServerLocation("localhost", 2));
    when(this.targetServerPart.getObject()).thenReturn(targetServer);
    when(this.cache.getDistributionManager()).thenReturn(this.distributionManager);
    when(this.distributionManager.isCurrentMember(targetServer)).thenReturn(false);
    when(this.serverConnection.getErrorResponseMessage()).thenReturn(this.errorResponseMessage);

    this.ping.cmdExecute(this.message, this.serverConnection, null, 0);

    verify(this.errorResponseMessage, times(1)).send(this.serverConnection);
    verify(this.replyMessage, times(0)).send(this.serverConnection);
  }

  @Test
  public void testTargetServerAndCurrentServerAreEqualsButWithDifferentViewId() throws Exception {
    myID = new InternalDistributedMember(new ServerLocation("localhost", 1));
    myID.setVmViewId(1);
    when(this.cache.getMyId()).thenReturn(this.myID);
    targetServer = new InternalDistributedMember(new ServerLocation("localhost", 1));
    targetServer.setVmViewId(2);
    when(this.targetServerPart.getObject()).thenReturn(targetServer);
    when(this.serverConnection.getErrorResponseMessage()).thenReturn(this.errorResponseMessage);

    this.ping.cmdExecute(this.message, this.serverConnection, null, 0);

    verify(this.errorResponseMessage, times(1)).send(this.serverConnection);
    verify(this.replyMessage, times(0)).send(this.serverConnection);
  }
}
