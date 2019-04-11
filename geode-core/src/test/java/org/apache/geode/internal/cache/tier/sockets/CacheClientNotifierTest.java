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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;

import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.fake.Fakes;

public class CacheClientNotifierTest {
  @Test
  public void eventsInClientRegistrationQueueAreSentToClientAfterRegistrationIsComplete()
      throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
      InvocationTargetException {
    InternalCache internalCache = Fakes.cache();
    CacheServerStats cacheServerStats = mock(CacheServerStats.class);
    ConnectionListener connectionListener = mock(ConnectionListener.class);

    CacheClientNotifier cacheClientNotifier = CacheClientNotifier.getInstance(internalCache,
        cacheServerStats, 0, 0, connectionListener, null, false);

    ClientRegistrationMetadata clientRegistrationMetadata = mock(ClientRegistrationMetadata.class);
    when(clientRegistrationMetadata.getDataOutputStream()).thenReturn(mock(DataOutputStream.class));
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    when(clientProxyMembershipID.getDistributedMember()).thenReturn(mock(DistributedMember.class));
    when(clientRegistrationMetadata.getClientProxyMembershipID()).thenReturn(
        clientProxyMembershipID);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(mock(InetAddress.class));

    cacheClientNotifier = spy(cacheClientNotifier);
    doNothing().when(cacheClientNotifier).registerClientInternal(clientRegistrationMetadata, socket,
        false, 0, true);
    cacheClientNotifier.registerClient(clientRegistrationMetadata, socket, false, 0, true);
  }
}
