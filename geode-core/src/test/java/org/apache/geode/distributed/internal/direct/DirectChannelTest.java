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
package org.apache.geode.distributed.internal.direct;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Random;

import org.jgroups.util.UUID;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MessageListener;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

public class DirectChannelTest {


  private DirectChannel directChannel;
  private InternalDistributedMember[] mockMembers;

  Membership<InternalDistributedMember> mgr;
  MessageListener<InternalDistributedMember> listener;
  ClusterDistributionManager dm;

  DistributionConfig dc;

  /**
   * Some tests require a DirectChannel mock
   */
  @Before
  public void setUp() throws Exception {
    listener = mock(MessageListener.class);
    mgr = mock(Membership.class);
    dm = mock(ClusterDistributionManager.class);
    dc = mock(DistributionConfig.class);

    Random r = new Random();
    mockMembers = new InternalDistributedMember[5];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = new InternalDistributedMember("localhost", 8888 + i);
      UUID uuid = new UUID(r.nextLong(), r.nextLong());
      mockMembers[i].setUUID(uuid);
    }
    when(dm.getConfig()).thenReturn(dc);
    when(dm.getSystem()).thenReturn(mock(InternalDistributedSystem.class));

    int[] range = new int[2];
    range[0] = 41000;
    range[1] = 61000;
    when(dc.getMembershipPortRange()).thenReturn(range);
    SecurableCommunicationChannel[] sslEnabledComponent = new SecurableCommunicationChannel[1];
    sslEnabledComponent[0] = SecurableCommunicationChannel.CLUSTER;

    when(dc.getSecurableCommunicationChannels()).thenReturn(sslEnabledComponent);

    SocketCreatorFactory.setDistributionConfig(dc);
    directChannel = new DirectChannel(mgr, listener, dm);
  }

  @Test
  public void testScheduleCloseEndpoint() throws Exception {
    directChannel.scheduleCloseEndpoint(mockMembers[0], null, false);
    directChannel.scheduleCloseEndpoint(mockMembers[1], null, false);
    directChannel.scheduleCloseEndpoint(mockMembers[2], null, false);

    assertThat(directChannel.getCloseEndpointExecutorQueueSize()).isEqualTo(3);
  }

  @Test
  public void testScheduleCloseEndpointAndClearAllAtDisconnect() throws Exception {
    directChannel.scheduleCloseEndpoint(mockMembers[0], null, false);
    directChannel.scheduleCloseEndpoint(mockMembers[1], null, false);
    directChannel.scheduleCloseEndpoint(mockMembers[2], null, false);
    directChannel.disconnect(null);

    assertThat(directChannel.getCloseEndpointExecutorQueueSize()).isEqualTo(0);
  }
}
