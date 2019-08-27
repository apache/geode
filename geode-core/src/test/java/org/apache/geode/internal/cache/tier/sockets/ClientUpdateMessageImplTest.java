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

package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.Serializable;

import org.junit.Test;

import org.apache.geode.CopyHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.fake.Fakes;

public class ClientUpdateMessageImplTest implements Serializable {
  @Test
  public void addInterestedClientTest() {
    ClientUpdateMessageImpl clientUpdateMessageImpl = new ClientUpdateMessageImpl();
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);

    assertThat(clientUpdateMessageImpl.isClientInterestedInUpdates(clientProxyMembershipID))
        .isFalse();
    clientUpdateMessageImpl.addClientInterestList(clientProxyMembershipID, true);
    assertThat(clientUpdateMessageImpl.isClientInterestedInUpdates(clientProxyMembershipID))
        .isTrue();

    assertThat(clientUpdateMessageImpl.isClientInterestedInInvalidates(clientProxyMembershipID))
        .isFalse();
    clientUpdateMessageImpl.addClientInterestList(clientProxyMembershipID, false);
    assertThat(clientUpdateMessageImpl.isClientInterestedInInvalidates(clientProxyMembershipID))
        .isTrue();
  }

  @Test
  public void serializeClientUpdateMessageNullInterestLists() {
    ClientUpdateMessageImpl clientUpdateMessageImpl = getTestClientUpdateMessage();

    ClientUpdateMessageImpl clientUpdateMessageCopy = CopyHelper.copy(clientUpdateMessageImpl);

    assertNotNull(clientUpdateMessageCopy);
    assertThat(clientUpdateMessageCopy.hasClientsInterestedInUpdates()).isFalse();
    assertThat(clientUpdateMessageCopy.hasClientsInterestedInInvalidates()).isFalse();
  }

  @Test
  public void serializeClientUpdateMessageWithInterestLists() {
    ClientUpdateMessageImpl clientUpdateMessageImpl = getTestClientUpdateMessage();

    DistributedMember distributedMember =
        mock(DistributedMember.class, withSettings().serializable());
    when(distributedMember.getDurableClientAttributes())
        .thenReturn(mock(DurableClientAttributes.class, withSettings().serializable()));

    ClientProxyMembershipID interestedClientID = new ClientProxyMembershipID(distributedMember);

    clientUpdateMessageImpl.addClientInterestList(interestedClientID, false);
    clientUpdateMessageImpl.addClientInterestList(interestedClientID, true);

    // This creates the CacheClientNotifier singleton which is null checked in
    // ClientUpdateMessageImpl.fromData(), so we need to do this for serialization to
    // succeed.
    CacheClientNotifier cacheClientNotifier =
        CacheClientNotifier.getInstance(Fakes.cache(),
            mock(ClientRegistrationEventQueueManager.class),
            mock(CacheServerStats.class), 10, 10,
            mock(ConnectionListener.class), null, true);

    // Mock the deserializing side to include the cache client
    // proxy with the interested client ID, so that the ID is added to the interest
    // collection in the message copy
    CacheClientProxy cacheClientProxy = mock(CacheClientProxy.class);
    when(cacheClientProxy.getProxyID()).thenReturn(interestedClientID);

    cacheClientNotifier.addClientProxy(cacheClientProxy);

    ClientUpdateMessageImpl clientUpdateMessageCopy = CopyHelper.copy(clientUpdateMessageImpl);

    assertNotNull(clientUpdateMessageCopy);
    assertThat(clientUpdateMessageCopy.isClientInterestedInUpdates(interestedClientID)).isTrue();
    assertThat(clientUpdateMessageCopy.isClientInterestedInInvalidates(interestedClientID))
        .isTrue();
  }

  private ClientUpdateMessageImpl getTestClientUpdateMessage() {
    LocalRegion localRegion = mock(LocalRegion.class);
    String regionName = "regionName";
    when(localRegion.getName()).thenReturn(regionName);
    return new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_CREATE, null, null);
  }
}
