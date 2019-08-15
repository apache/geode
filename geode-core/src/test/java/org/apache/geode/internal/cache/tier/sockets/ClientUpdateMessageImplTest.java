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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.versions.VersionTag;

public class ClientUpdateMessageImplTest {
  @Test
  public void copyConstructClientUpdateMessageImplEqualityCheck() {
    EnumListenerEvent operation = EnumListenerEvent.AFTER_UPDATE;
    LocalRegion localRegion = mock(LocalRegion.class);
    String regionName = "regionName";
    when(localRegion.getFullPath()).thenReturn(regionName);
    String key = "key";
    byte[] value = "value".getBytes();
    byte valueIsObject = (byte) 0x01;
    Object callbackArgument = new Object();
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    EventID eventIdentifier = new EventID();
    byte[] deltaBytes = "delta".getBytes();
    VersionTag versionTag = mock(VersionTag.class);

    ClientUpdateMessageImpl message = new ClientUpdateMessageImpl(operation,
        localRegion, key, value, deltaBytes, valueIsObject, callbackArgument,
        clientProxyMembershipID, eventIdentifier, versionTag);

    ClientUpdateMessageImpl.CqNameToOp cqNameToOpHashMap =
        new ClientUpdateMessageImpl.CqNameToOpHashMap(10);
    String hashMapCqNamePrefix = "hashMapCqName";

    int cqNameToOpHashMapSize = 3;
    for (int i = 0; i < cqNameToOpHashMapSize; ++i) {
      cqNameToOpHashMap.add(hashMapCqNamePrefix + i, i);
    }

    message.addClientCqs(clientProxyMembershipID, cqNameToOpHashMap);

    int singleEntryCqOp = 0;
    String singleEntryCqName = "singleEntryCqName";
    ClientUpdateMessageImpl.CqNameToOpSingleEntry cqNameToOpSingleEntry =
        new ClientUpdateMessageImpl.CqNameToOpSingleEntry(singleEntryCqName, singleEntryCqOp);
    message.addClientCqs(clientProxyMembershipID, cqNameToOpSingleEntry);

    Set<ClientProxyMembershipID> interestedProxies = new HashSet<>();

    ClientProxyMembershipID clientProxy0 = mock(ClientProxyMembershipID.class);
    interestedProxies.add(clientProxy0);
    ClientProxyMembershipID clientProxy1 = mock(ClientProxyMembershipID.class);
    interestedProxies.add(clientProxy1);

    message.addClientInterestList(interestedProxies, true);
    message.addClientInterestList(interestedProxies, false);

    ClientUpdateMessageImpl clientUpdateMessageCopy = new ClientUpdateMessageImpl(message);

    assertEquals(operation, clientUpdateMessageCopy.getOperation());
    assertEquals(regionName, clientUpdateMessageCopy.getRegionName());
    assertEquals(key, clientUpdateMessageCopy.getKeyOfInterest());
    assertEquals(value, clientUpdateMessageCopy.getValue());
    assertThat(clientUpdateMessageCopy.valueIsObject()).isTrue();
    assertEquals(callbackArgument, clientUpdateMessageCopy.getCallbackArgument());
    assertEquals(clientProxyMembershipID, clientUpdateMessageCopy.getMembershipId());
    assertEquals(eventIdentifier, clientUpdateMessageCopy.getEventId());
    assertEquals(message.shouldBeConflated(), clientUpdateMessageCopy.shouldBeConflated());
    assertEquals(message.hasCqs(), clientUpdateMessageCopy.hasCqs());
    assertEquals(versionTag, clientUpdateMessageCopy.getVersionTag());

    for (Map.Entry<ClientProxyMembershipID, ClientUpdateMessageImpl.CqNameToOp> entry : clientUpdateMessageCopy
        .getClientCqs().entrySet()) {
      assertEquals(entry.getKey(), clientProxyMembershipID);
      ClientUpdateMessageImpl.CqNameToOp cqNameToOp = entry.getValue();
      if (cqNameToOp instanceof ClientUpdateMessageImpl.CqNameToOpHashMap) {
        ClientUpdateMessageImpl.CqNameToOpHashMap map =
            (ClientUpdateMessageImpl.CqNameToOpHashMap) cqNameToOp;
        assertEquals(cqNameToOpHashMapSize, map.size());

        for (int i = 0; i < cqNameToOpHashMapSize; ++i) {
          assertEquals((int) map.get(hashMapCqNamePrefix + i), i);
        }
      } else {
        Message verificationMessage = mock(Message.class);
        cqNameToOpSingleEntry.addToMessage(verificationMessage);
        verify(verificationMessage, times(1)).addIntPart(singleEntryCqOp);
        verify(verificationMessage, times(1)).addStringPart(singleEntryCqName, true);
      }
    }

    assertThat(clientUpdateMessageCopy.isClientInterestedInUpdates(clientProxy0)).isTrue();
    assertThat(clientUpdateMessageCopy.isClientInterestedInInvalidates(clientProxy0)).isTrue();
    assertThat(clientUpdateMessageCopy.isClientInterestedInUpdates(clientProxy1)).isTrue();
    assertThat(clientUpdateMessageCopy.isClientInterestedInInvalidates(clientProxy1)).isTrue();
  }

  @Test
  public void copyConstructClientUpdateMessageImplNullClientInterestCollections() {
    ClientUpdateMessageImpl clientUpdateMessageImpl = new ClientUpdateMessageImpl();

    ClientUpdateMessageImpl clientUpdateMessageCopy =
        new ClientUpdateMessageImpl(clientUpdateMessageImpl);

    assertNull(clientUpdateMessageCopy.getClientCqs());

    ClientProxyMembershipID clientProxy = mock(ClientProxyMembershipID.class);

    assertThat(clientUpdateMessageCopy.hasClientInterestListForUpdates()).isFalse();
    assertThat(clientUpdateMessageCopy.hasClientInterestListForInvalidates()).isFalse();

    Set<ClientProxyMembershipID> interestedProxies = new HashSet<>();

    interestedProxies.add(clientProxy);

    clientUpdateMessageCopy.addClientInterestList(interestedProxies, true);
    clientUpdateMessageCopy.addClientInterestList(interestedProxies, false);

    assertThat(clientUpdateMessageCopy.isClientInterestedInUpdates(clientProxy)).isTrue();
    assertThat(clientUpdateMessageCopy.isClientInterestedInInvalidates(clientProxy)).isTrue();
  }
}
