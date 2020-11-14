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
package org.apache.geode.internal.cache.wan;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.cache.client.internal.Connection;

public class GatewaySenderEventRemoteDispatcherJUnitTest {
  @Test
  public void getConnectionShouldShutdownTheAckThreadReaderWhenEventProcessorIsShutDown() {
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    AbstractGatewaySenderEventProcessor eventProcessor =
        mock(AbstractGatewaySenderEventProcessor.class);
    GatewaySenderEventRemoteDispatcher dispatcher =
        new GatewaySenderEventRemoteDispatcher(eventProcessor, null);
    GatewaySenderEventRemoteDispatcher.AckReaderThread ackReaderThread =
        dispatcher.new AckReaderThread(sender, "AckReaderThread");
    dispatcher.setAckReaderThread(ackReaderThread);
    assertFalse(ackReaderThread.isShutdown());
    when(eventProcessor.isStopped()).thenReturn(true);
    assertNull(dispatcher.getConnection(false));
    assertTrue(ackReaderThread.isShutdown());
  }

  @Test
  public void shuttingDownAckThreadReaderConnectionShouldshutdownTheAckThreadReader() {
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    AbstractGatewaySenderEventProcessor eventProcessor =
        mock(AbstractGatewaySenderEventProcessor.class);
    GatewaySenderEventRemoteDispatcher dispatcher =
        new GatewaySenderEventRemoteDispatcher(eventProcessor, null);
    GatewaySenderEventRemoteDispatcher.AckReaderThread ackReaderThread =
        dispatcher.new AckReaderThread(sender, "AckReaderThread");
    dispatcher.setAckReaderThread(ackReaderThread);
    dispatcher.shutDownAckReaderConnection();
    assertTrue(ackReaderThread.isShutdown());
  }

  @Test
  public void getConnectionShouldCreateNewConnectionWhenServerIsNull() {
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    when(sender.isParallel()).thenReturn(false);
    AbstractGatewaySenderEventProcessor eventProcessor =
        mock(AbstractGatewaySenderEventProcessor.class);
    when(eventProcessor.getSender()).thenReturn(sender);
    Connection connection = mock(Connection.class);
    when(connection.isDestroyed()).thenReturn(false);
    when(connection.getServer()).thenReturn(null);
    GatewaySenderEventRemoteDispatcher dispatcher =
        new GatewaySenderEventRemoteDispatcher(eventProcessor, connection);
    dispatcher = spy(dispatcher);
    doNothing().when(dispatcher).initializeConnection();
    Connection newConnection = dispatcher.getConnection(true);
    verify(dispatcher, times(1)).initializeConnection();
    verify(dispatcher, times(2)).getConnectionLifeCycleLock();
  }
}
