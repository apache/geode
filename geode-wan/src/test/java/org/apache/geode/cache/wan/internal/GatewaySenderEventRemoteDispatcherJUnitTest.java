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
package org.apache.geode.cache.wan.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.Op;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocationAndMemberId;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.pdx.internal.PeerTypeRegistration;

public class GatewaySenderEventRemoteDispatcherJUnitTest {

  @Mock
  private AbstractGatewaySender senderMock;

  @Mock
  private AbstractGatewaySenderEventProcessor eventProcessorMock;

  @InjectMocks
  private GatewaySenderEventRemoteDispatcher eventDispatcher;

  @Mock
  private PoolImpl poolMock;

  @Mock
  private Connection connectionMock;

  @Mock
  private ServerQueueStatus serverQueueStatusMock;

  @Mock
  private Endpoint endpointMock;

  @Mock
  private DistributedMember memberIdMock;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(eventProcessorMock.getSender()).thenReturn(senderMock);

    when(senderMock.isParallel()).thenReturn(false);
    when(senderMock.getLockForConcurrentDispatcher()).thenReturn(new Object());
    when(senderMock.getProxy()).thenReturn(poolMock);

    when(poolMock.isDestroyed()).thenReturn(false);
    when(poolMock.acquireConnection()).thenReturn(connectionMock);

    when(connectionMock.getQueueStatus()).thenReturn(serverQueueStatusMock);
  }

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
  public void shuttingDownAckThreadReaderConnectionShouldShutdownTheAckThreadReader() {
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
  public void shuttingDownAckThreadReaderShouldStopWhenCanceledAfterReadAckReturnNull() {
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    InternalCache cache = mock(InternalCache.class);
    when(sender.getCache()).thenReturn(cache);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    when(cancelCriterion.isCancelInProgress()).thenReturn(false).thenReturn(true);
    AbstractGatewaySenderEventProcessor eventProcessor =
        mock(AbstractGatewaySenderEventProcessor.class);
    when(eventProcessor.getSender()).thenReturn(sender);
    GatewaySenderEventRemoteDispatcher dispatcher =
        new GatewaySenderEventRemoteDispatcher(eventProcessor, null);
    GatewaySenderEventRemoteDispatcher.AckReaderThread ackReaderThread =
        dispatcher.new AckReaderThread(sender, "AckReaderThread");

    ackReaderThread.run();

    // Check that thread is stopped when canceled
    verify(cache, times(2)).getCancelCriterion();
  }

  @Test
  public void shuttingDownAckThreadReaderShouldStopWhenCanceledAndHandleSuccessBatchAckThrowException() {
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    InternalCache cache = mock(InternalCache.class);
    when(sender.getCache()).thenReturn(cache);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    when(cancelCriterion.isCancelInProgress()).thenReturn(false).thenReturn(true);
    AbstractGatewaySenderEventProcessor eventProcessor =
        mock(AbstractGatewaySenderEventProcessor.class);
    when(eventProcessor.getSender()).thenReturn(sender);
    GatewaySenderEventRemoteDispatcher dispatcher =
        new GatewaySenderEventRemoteDispatcher(eventProcessor, null);
    GatewaySenderEventRemoteDispatcher.AckReaderThread ackReaderThread =
        dispatcher.new AckReaderThread(sender, "AckReaderThread");
    PoolImpl pool = mock(PoolImpl.class);
    when(sender.getProxy()).thenReturn(pool);
    Connection connection = mock(Connection.class);
    when(pool.acquireConnection()).thenReturn(connection);
    when(sender.isParallel()).thenReturn(true);
    when(cache.getRegion(PeerTypeRegistration.REGION_NAME)).thenReturn(null);
    ServerQueueStatus serverQueueStatus = mock(ServerQueueStatus.class);
    when(connection.getQueueStatus()).thenReturn(serverQueueStatus);
    when(serverQueueStatus.getPdxSize()).thenReturn(0);

    when(eventProcessor.isStopped()).thenReturn(false);
    GatewaySenderEventRemoteDispatcher.GatewayAck gatewayAck =
        mock(GatewaySenderEventRemoteDispatcher.GatewayAck.class);
    when(pool.executeOn(any(Connection.class), any(Op.class), anyBoolean())).thenReturn(gatewayAck);

    doThrow(CacheClosedException.class).when(eventProcessor).handleSuccessBatchAck(anyInt());

    ackReaderThread.run();

    // Check that thread is stopped when canceled
    verify(cache, times(2)).getCancelCriterion();
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

  @Test
  public void initializeConnectionWithParallelSenderDoesNotRetryInitializeConnection() {
    when(senderMock.isParallel()).thenReturn(true);

    eventDispatcher = new GatewaySenderEventRemoteDispatcher(eventProcessorMock, null);
    GatewaySenderEventRemoteDispatcher dispatcherSpy = spy(eventDispatcher);
    dispatcherSpy.initializeConnection();

    verify(senderMock, times(0)).getLockForConcurrentDispatcher();
    verify(senderMock, times(1)).setServerLocation(any());
    verify(poolMock, times(1)).acquireConnection();
    verify(dispatcherSpy, times(0)).retryInitializeConnection(connectionMock);
  }

  @Test
  public void initializeConnectionWithSerialSenderAndEnforceThreadsConnectSameRecieverFalseDoesNotRetryInitializeConnection() {
    when(senderMock.getEnforceThreadsConnectSameReceiver()).thenReturn(false);

    when(connectionMock.getEndpoint()).thenReturn(endpointMock);
    when(endpointMock.getMemberId()).thenReturn(memberIdMock);
    when(memberIdMock.getUniqueId()).thenReturn("receiverId");

    eventDispatcher = new GatewaySenderEventRemoteDispatcher(eventProcessorMock, null);
    GatewaySenderEventRemoteDispatcher dispatcherSpy = spy(eventDispatcher);
    dispatcherSpy.initializeConnection();

    verify(senderMock, times(1)).getLockForConcurrentDispatcher();
    verify(senderMock, times(1)).getEnforceThreadsConnectSameReceiver();
    verify(poolMock, times(1)).acquireConnection();
    verify(dispatcherSpy, times(0)).retryInitializeConnection(connectionMock);
  }

  @Test
  public void initializeConnectionWithSerialSenderAndEnforceThreadsConnectSameReceiverTrueAndNoExpectedReceiverIdSetsReceiverIdAndDoesNotReacquireConnection() {

    when(senderMock.getEnforceThreadsConnectSameReceiver()).thenReturn(true);

    when(connectionMock.getEndpoint()).thenReturn(endpointMock);
    when(endpointMock.getMemberId()).thenReturn(memberIdMock);
    when(memberIdMock.getUniqueId()).thenReturn("receiverId");
    when(eventProcessorMock.getExpectedReceiverUniqueId()).thenReturn("");

    eventDispatcher = new GatewaySenderEventRemoteDispatcher(eventProcessorMock, null);
    GatewaySenderEventRemoteDispatcher dispatcherSpy = spy(eventDispatcher);
    dispatcherSpy.initializeConnection();

    verify(senderMock, times(1)).getLockForConcurrentDispatcher();
    verify(senderMock, times(1)).getEnforceThreadsConnectSameReceiver();
    verify(dispatcherSpy, times(1)).retryInitializeConnection(connectionMock);
    verify(poolMock, times(1)).acquireConnection();
    verify(eventProcessorMock, times(1)).setExpectedReceiverUniqueId("receiverId");
  }

  @Test
  public void initializeConnectionWithSerialSenderAndEnforceThreadsConnectSameReceiverTrueAndConnectedToExpectedReceiverDoesNotReacquireConnection() {

    when(senderMock.getEnforceThreadsConnectSameReceiver()).thenReturn(true);

    when(connectionMock.getEndpoint()).thenReturn(endpointMock);
    when(endpointMock.getMemberId()).thenReturn(memberIdMock);
    when(memberIdMock.getUniqueId()).thenReturn("expectedId");
    when(eventProcessorMock.getExpectedReceiverUniqueId()).thenReturn("expectedId");

    eventDispatcher = new GatewaySenderEventRemoteDispatcher(eventProcessorMock, null);
    GatewaySenderEventRemoteDispatcher dispatcherSpy = spy(eventDispatcher);
    dispatcherSpy.initializeConnection();

    verify(senderMock, times(1)).getLockForConcurrentDispatcher();
    verify(senderMock, times(1)).getEnforceThreadsConnectSameReceiver();
    verify(dispatcherSpy, times(1)).retryInitializeConnection(connectionMock);
    verify(poolMock, times(1)).acquireConnection();
    verify(eventProcessorMock, times(0)).setExpectedReceiverUniqueId(any());
  }

  @Test
  public void initializeConnectionWithSerialSenderAndEnforceThreadsConnectSameReceiverTrueAndConnectedToExpectedReceiverOnSecondTryReacquiresConnectionOnce() {

    when(senderMock.getEnforceThreadsConnectSameReceiver()).thenReturn(true);

    when(connectionMock.getEndpoint()).thenReturn(endpointMock);
    when(endpointMock.getMemberId()).thenReturn(memberIdMock);
    when(memberIdMock.getUniqueId()).thenReturn("notExpectedId").thenReturn("expectedId");
    when(eventProcessorMock.getExpectedReceiverUniqueId()).thenReturn("expectedId");

    eventDispatcher = new GatewaySenderEventRemoteDispatcher(eventProcessorMock, null);
    GatewaySenderEventRemoteDispatcher dispatcherSpy = spy(eventDispatcher);
    dispatcherSpy.initializeConnection();

    verify(senderMock, times(1)).getLockForConcurrentDispatcher();
    verify(senderMock, times(1)).getEnforceThreadsConnectSameReceiver();
    verify(dispatcherSpy, times(1)).retryInitializeConnection(connectionMock);
    verify(poolMock, times(2)).acquireConnection();
    verify(eventProcessorMock, times(0)).setExpectedReceiverUniqueId(any());

  }

  @Test
  public void initializeConnectionWithSerialSenderAndEnforceThreadsConnectSameReceiverTrueAndMaxRetriesExceededAndNoServersAvailableThrowsException() {

    when(senderMock.getEnforceThreadsConnectSameReceiver()).thenReturn(true);

    when(connectionMock.getEndpoint()).thenReturn(endpointMock);
    when(endpointMock.getMemberId()).thenReturn(memberIdMock);
    when(memberIdMock.getUniqueId()).thenReturn("notExpectedId");
    when(eventProcessorMock.getExpectedReceiverUniqueId()).thenReturn("expectedId");

    eventDispatcher = new GatewaySenderEventRemoteDispatcher(eventProcessorMock, null);
    GatewaySenderEventRemoteDispatcher dispatcherSpy = spy(eventDispatcher);

    String expectedExceptionMessage =
        "There are no active servers. "
            + GatewaySenderEventRemoteDispatcher.maxAttemptsReachedConnectingServerIdExceptionMessage
            + " [expectedId] (10 attempts)";
    assertThatThrownBy(dispatcherSpy::initializeConnection)
        .isInstanceOf(GatewaySenderException.class).hasMessageContaining(expectedExceptionMessage);

    verify(senderMock, times(1)).getLockForConcurrentDispatcher();
    verify(senderMock, times(2)).getEnforceThreadsConnectSameReceiver();
    verify(dispatcherSpy, times(1)).retryInitializeConnection(connectionMock);
    verify(poolMock, times(10)).acquireConnection();
    verify(eventProcessorMock, times(0)).setExpectedReceiverUniqueId(any());
  }

  @Test
  public void initializeConnectionWithSerialSenderAndEnforceThreadsConnectSameReceiverTrueAndMaxRetriesExceededAndServersAvailableThrowsException() {

    when(senderMock.getEnforceThreadsConnectSameReceiver()).thenReturn(true);

    when(connectionMock.getEndpoint()).thenReturn(endpointMock);
    when(endpointMock.getMemberId()).thenReturn(memberIdMock);
    when(memberIdMock.getUniqueId()).thenReturn("notExpectedId");
    when(eventProcessorMock.getExpectedReceiverUniqueId()).thenReturn("expectedId");
    List<ServerLocationAndMemberId> currentServers = new ArrayList<>();
    currentServers.add(new ServerLocationAndMemberId(new ServerLocation("host1", 1), "id1"));
    currentServers.add(new ServerLocationAndMemberId(new ServerLocation("host2", 2), "id2"));
    when(poolMock.getCurrentServers()).thenReturn(currentServers);

    eventDispatcher = new GatewaySenderEventRemoteDispatcher(eventProcessorMock, null);
    GatewaySenderEventRemoteDispatcher dispatcherSpy = spy(eventDispatcher);

    String expectedExceptionMessage =
        "No available connection was found, but the following active servers exist: host1:1@id1, host2:2@id2 "
            + GatewaySenderEventRemoteDispatcher.maxAttemptsReachedConnectingServerIdExceptionMessage
            + " [expectedId] (10 attempts)";
    assertThatThrownBy(dispatcherSpy::initializeConnection)
        .isInstanceOf(GatewaySenderException.class).hasMessageContaining(expectedExceptionMessage);

    verify(senderMock, times(1)).getLockForConcurrentDispatcher();
    verify(senderMock, times(2)).getEnforceThreadsConnectSameReceiver();
    verify(dispatcherSpy, times(1)).retryInitializeConnection(connectionMock);
    verify(poolMock, times(10)).acquireConnection();
    verify(eventProcessorMock, times(0)).setExpectedReceiverUniqueId(any());
  }

}
