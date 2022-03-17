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

package org.apache.geode.cache.client.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.Test;

import org.apache.geode.cache.client.internal.locator.ServerLocationRequest;
import org.apache.geode.cache.client.internal.locator.ServerLocationResponse;
import org.apache.geode.distributed.internal.tcpserver.ClusterSocketCreator;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.distributed.internal.tcpserver.VersionResponse;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;

class AutoConnectionSourceImplTest {

  @Test
  void queryLocatorsTriesNextLocatorOnSSLExceptions() throws IOException, ClassNotFoundException {
    final HostAndPort locator1 = new HostAndPort("locator1", 1234);
    final HostAndPort locator2 = new HostAndPort("locator2", 1234);
    final TcpSocketCreator socketCreator = mock(TcpSocketCreator.class);
    final ObjectSerializer objectSerializer = mock(ObjectSerializer.class);
    final ObjectDeserializer objectDeserializer = mock(ObjectDeserializer.class);
    final TcpSocketFactory socketFactory = mock(TcpSocketFactory.class);
    final ClusterSocketCreator clusterSocketCreator = mock(ClusterSocketCreator.class);
    final Socket socket = mock(Socket.class);
    final InternalPool internalPool = mock(InternalPool.class);
    final ServerLocationRequest request = mock(ServerLocationRequest.class);
    final ServerLocationResponse response = mock(ServerLocationResponse.class);

    final List<HostAndPort> locators = Arrays.asList(locator1, locator2);
    final TcpClient tcpClient =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    when(internalPool.getStats()).thenReturn(mock(PoolStats.class));
    when(response.hasResult()).thenReturn(true);

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(eq(locator1), anyInt(), any(), same(socketFactory)))
        .thenThrow(new SSLHandshakeException("ouch"));
    when(clusterSocketCreator.connect(eq(locator2), anyInt(), any(), same(socketFactory)))
        .thenReturn(socket);
    when(socket.getInputStream()).thenReturn(mock(InputStream.class));
    when(socket.getOutputStream()).thenReturn(mock(OutputStream.class));
    when(objectDeserializer.readObject(any())).thenReturn(new VersionResponse(), response);

    final AutoConnectionSourceImpl autoConnectionSource =
        new AutoConnectionSourceImpl(locators, "", Integer.MAX_VALUE, tcpClient);
    autoConnectionSource.start(internalPool);

    assertThat(autoConnectionSource.queryLocators(request)).isSameAs(response);

    verify(socketCreator, times(4)).forCluster();
    verify(objectDeserializer, times(2)).readObject(any());
    verify(response).hasResult();

    verifyNoMoreInteractions(response, socketCreator, objectDeserializer);
  }
}
