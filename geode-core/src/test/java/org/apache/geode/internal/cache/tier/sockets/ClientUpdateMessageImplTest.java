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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.CopyHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ClientUpdateMessageImplTest implements Serializable {
  private final ClientProxyMembershipID client1 = mock(ClientProxyMembershipID.class);
  private final ClientProxyMembershipID client2 = mock(ClientProxyMembershipID.class);
  private final ClientUpdateMessageImpl.ClientCqConcurrentMap clientCqs =
      new ClientUpdateMessageImpl.ClientCqConcurrentMap();

  @Rule
  public ExecutorServiceRule executorService = new ExecutorServiceRule();

  @Before
  public void setUp() {
    ClientUpdateMessageImpl.CqNameToOpHashMap cqs1 =
        new ClientUpdateMessageImpl.CqNameToOpHashMap(2);
    cqs1.add("cqName1", 1);
    cqs1.add("cqName2", 2);
    clientCqs.put(client1, cqs1);
    ClientUpdateMessageImpl.CqNameToOpSingleEntry cqs2 =
        new ClientUpdateMessageImpl.CqNameToOpSingleEntry("cqName3", 3);
    clientCqs.put(client2, cqs2);
  }

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
            mock(StatisticsClock.class),
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


  @Test
  public void addClientCqCanBeExecutedConcurrently() throws Exception {
    ClientUpdateMessageImpl clientUpdateMessageImpl = new ClientUpdateMessageImpl();

    int numOfEvents = 4;
    int[] cqEvents = new int[numOfEvents];
    String[] cqNames = new String[numOfEvents];
    ClientProxyMembershipID[] clients = new ClientProxyMembershipID[numOfEvents];
    prepareCqInfo(numOfEvents, cqEvents, cqNames, clients);

    addClientCqConcurrently(clientUpdateMessageImpl, numOfEvents, cqEvents, cqNames, clients);

    assertThat(clientUpdateMessageImpl.getClientCqs()).hasSize(2);
    assertThat(clientUpdateMessageImpl.getClientCqs().get(client1)).isInstanceOf(
        ClientUpdateMessageImpl.CqNameToOpHashMap.class);
    ClientUpdateMessageImpl.CqNameToOpHashMap client1Cqs =
        (ClientUpdateMessageImpl.CqNameToOpHashMap) clientUpdateMessageImpl.getClientCqs()
            .get(client1);
    for (int i = 0; i < 3; i++) {
      assertThat(client1Cqs.get("cqName" + i)).isEqualTo(i);
    }

    assertThat(clientUpdateMessageImpl.getClientCqs().get(client2)).isInstanceOf(
        ClientUpdateMessageImpl.CqNameToOpSingleEntry.class);
    ClientUpdateMessageImpl.CqNameToOpSingleEntry client2Cqs =
        (ClientUpdateMessageImpl.CqNameToOpSingleEntry) clientUpdateMessageImpl.getClientCqs()
            .get(client2);
    assertThat(client2Cqs.isEmpty()).isFalse();
  }

  private void prepareCqInfo(int numOfEvents, int[] cqEvents, String[] cqNames,
      ClientProxyMembershipID[] clients) {
    for (int i = 0; i < numOfEvents; i++) {
      cqEvents[i] = i;
      cqNames[i] = "cqName" + i;
      if (i < 3) {
        clients[i] = client1;
      } else {
        clients[i] = client2;
      }
    }
  }

  private void addClientCqConcurrently(ClientUpdateMessageImpl clientUpdateMessageImpl,
      int numOfEvents, int[] cqEvents, String[] cqNames, ClientProxyMembershipID[] clients)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < numOfEvents; i++) {
      ClientProxyMembershipID client = clients[i];
      String cqName = cqNames[i];
      int cqEvent = cqEvents[i];
      futures.add(executorService
          .submit(() -> clientUpdateMessageImpl.addClientCq(client, cqName, cqEvent)));
    }
    for (Future<Void> future : futures) {
      future.get();
    }
  }

  @Test
  public void addOrSetClientCqsCanSetIfCqsMapIsNull() {
    ClientUpdateMessageImpl clientUpdateMessageImpl = new ClientUpdateMessageImpl();

    clientUpdateMessageImpl.addOrSetClientCqs(client1, clientCqs);

    assertThat(clientUpdateMessageImpl.getClientCqs()).isEqualTo(clientCqs);
  }

  @Test
  public void addOrSetClientCqsCanAddCqsIfCqsMapNotNull() {
    ClientUpdateMessageImpl clientUpdateMessageImpl = new ClientUpdateMessageImpl();
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    clientUpdateMessageImpl.addClientCq(clientProxyMembershipID, "cqName", 10);

    clientUpdateMessageImpl.addOrSetClientCqs(client1, clientCqs);

    assertThat(clientUpdateMessageImpl.getClientCqs()).hasSize(2);
    assertThat(clientUpdateMessageImpl.getClientCqs().get(client1)).isInstanceOf(
        ClientUpdateMessageImpl.CqNameToOpHashMap.class);
    ClientUpdateMessageImpl.CqNameToOpHashMap client1Cqs =
        (ClientUpdateMessageImpl.CqNameToOpHashMap) clientUpdateMessageImpl.getClientCqs()
            .get(client1);
    assertThat(client1Cqs.get("cqName1")).isEqualTo(1);
    assertThat(client1Cqs.get("cqName2")).isEqualTo(2);

    assertThat(clientUpdateMessageImpl.getClientCqs().get(clientProxyMembershipID)).isInstanceOf(
        ClientUpdateMessageImpl.CqNameToOpSingleEntry.class);

    assertThat(clientUpdateMessageImpl.getClientCqs().get(client2)).isNull();
  }

}
