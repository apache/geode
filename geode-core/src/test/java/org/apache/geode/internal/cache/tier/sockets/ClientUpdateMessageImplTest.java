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

    ClientUpdateMessageImpl originalClientUpdateMessageImpl = new ClientUpdateMessageImpl(operation,
        localRegion, key, value, deltaBytes, valueIsObject, callbackArgument,
        clientProxyMembershipID, eventIdentifier, versionTag);

    ClientUpdateMessageImpl.CqNameToOp cqNameToOpHashMap =
        new ClientUpdateMessageImpl.CqNameToOpHashMap(10);
    String hashMapCqNamePrefix = "hashMapCqName";

    int cqNameToOpHashMapSize = 3;
    for (int i = 0; i < cqNameToOpHashMapSize; ++i) {
      cqNameToOpHashMap.add(hashMapCqNamePrefix + i, i);
    }

    originalClientUpdateMessageImpl.addClientCqs(clientProxyMembershipID, cqNameToOpHashMap);

    int singleEntryCqOp = 0;
    String singleEntryCqName = "singleEntryCqName";
    ClientUpdateMessageImpl.CqNameToOpSingleEntry cqNameToOpSingleEntry =
        new ClientUpdateMessageImpl.CqNameToOpSingleEntry(singleEntryCqName, singleEntryCqOp);
    originalClientUpdateMessageImpl.addClientCqs(clientProxyMembershipID, cqNameToOpSingleEntry);

    Set<ClientProxyMembershipID> interestedProxies = new HashSet<>();

    ClientProxyMembershipID clientProxy0 = mock(ClientProxyMembershipID.class);
    interestedProxies.add(clientProxy0);
    ClientProxyMembershipID clientProxy1 = mock(ClientProxyMembershipID.class);
    interestedProxies.add(clientProxy1);

    originalClientUpdateMessageImpl.addClientInterestList(interestedProxies, true);
    originalClientUpdateMessageImpl.addClientInterestList(interestedProxies, false);

    ClientUpdateMessageImpl clientUpdateMessageImplCopy =
        new ClientUpdateMessageImpl(originalClientUpdateMessageImpl);

    assertEquals(operation, clientUpdateMessageImplCopy.getOperation());
    assertEquals(regionName, clientUpdateMessageImplCopy.getRegionName());
    assertEquals(key, clientUpdateMessageImplCopy.getKeyOfInterest());
    assertEquals(value, clientUpdateMessageImplCopy.getValue());
    assertThat(clientUpdateMessageImplCopy.valueIsObject()).isTrue();
    assertEquals(callbackArgument, clientUpdateMessageImplCopy.getCallbackArgument());
    assertEquals(clientProxyMembershipID, clientUpdateMessageImplCopy.getMembershipId());
    assertEquals(eventIdentifier, clientUpdateMessageImplCopy.getEventId());
    assertEquals(originalClientUpdateMessageImpl.shouldBeConflated(),
        clientUpdateMessageImplCopy.shouldBeConflated());
    assertEquals(originalClientUpdateMessageImpl.hasCqs(), clientUpdateMessageImplCopy.hasCqs());
    assertEquals(versionTag, clientUpdateMessageImplCopy.getVersionTag());

    for (Map.Entry<ClientProxyMembershipID, ClientUpdateMessageImpl.CqNameToOp> entry : clientUpdateMessageImplCopy
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

    assertThat(clientUpdateMessageImplCopy.isClientInterestedInUpdates(clientProxy0)).isTrue();
    assertThat(clientUpdateMessageImplCopy.isClientInterestedInInvalidates(clientProxy0)).isTrue();
    assertThat(clientUpdateMessageImplCopy.isClientInterestedInUpdates(clientProxy1)).isTrue();
    assertThat(clientUpdateMessageImplCopy.isClientInterestedInInvalidates(clientProxy1)).isTrue();
  }

  @Test
  public void copyConstructClientUpdateMessageImplNullClientInterestCollections() {
    ClientUpdateMessageImpl originalClientUpdateMessageImpl = new ClientUpdateMessageImpl();

    assertNull(originalClientUpdateMessageImpl.getClientCqs());
    assertThat(originalClientUpdateMessageImpl.hasClientInterestListForUpdates()).isFalse();
    assertThat(originalClientUpdateMessageImpl.hasClientInterestListForInvalidates()).isFalse();

    ClientUpdateMessageImpl clientMessageUpdateImplCopy =
        new ClientUpdateMessageImpl(originalClientUpdateMessageImpl);

    assertNull(clientMessageUpdateImplCopy.getClientCqs());
    assertThat(clientMessageUpdateImplCopy.hasClientInterestListForUpdates()).isFalse();
    assertThat(clientMessageUpdateImplCopy.hasClientInterestListForInvalidates()).isFalse();
  }

  @Test
  public void copyConstructClientUpdateMessageImplClientInterestCollectionsAreIsolated() {
    ClientUpdateMessageImpl originalClientUpdateMessageImpl = new ClientUpdateMessageImpl();

    ClientUpdateMessageImpl.CqNameToOp cqNameToOpHashMap =
        new ClientUpdateMessageImpl.CqNameToOpHashMap(10);

    cqNameToOpHashMap.add("existingHashMapCqName", 0);

    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);

    originalClientUpdateMessageImpl.addClientCqs(clientProxyMembershipID, cqNameToOpHashMap);

    ClientUpdateMessageImpl clientUpdateMessageCopy =
        new ClientUpdateMessageImpl(originalClientUpdateMessageImpl);

    ClientProxyMembershipID newClientProxyMembershipID = mock(ClientProxyMembershipID.class);

    clientUpdateMessageCopy.addClientCq(newClientProxyMembershipID, "newSingleEntryCqName", 1);

    assertNull(originalClientUpdateMessageImpl.getClientCq(newClientProxyMembershipID));

    String newHashMapCqName = "newHashMapCqName";
    clientUpdateMessageCopy.addClientCq(clientProxyMembershipID, newHashMapCqName, 2);

    assertNull(((ClientUpdateMessageImpl.CqNameToOpHashMap) originalClientUpdateMessageImpl
        .getClientCq(clientProxyMembershipID)).get(newHashMapCqName));
  }
}
