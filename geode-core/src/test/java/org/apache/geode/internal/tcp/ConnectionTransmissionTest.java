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
package org.apache.geode.internal.tcp;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_SOCKET_BUFFER_SIZE;
import static org.apache.geode.distributed.internal.DistributionConfigImpl.SECURITY_SYSTEM_PREFIX;
import static org.apache.geode.internal.inet.LocalHostUtil.getLocalHost;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.Distribution;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;
import org.apache.geode.internal.net.BufferPool;
import org.apache.geode.internal.net.SocketCloser;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category(MembershipTest.class)
public class ConnectionTransmissionTest {

  /**
   * Create a sender connection and a receiver connection and pass data from
   * one to the other.
   *
   * This test uses a real socket, but attempts to mock all other collaborators
   * of connection, such as the InternalDistributedSystem.
   */
  @Test
  public void testDataTransmittedBetweenSenderAndReceiverIfMembershipCheckPassed()
      throws Exception {
    final DMStats stats = mock(DMStats.class);
    final BufferPool bufferPool = new BufferPool(stats);

    final ServerSocketChannel acceptorSocket = createReceiverSocket();
    final int serverSocketPort = acceptorSocket.socket().getLocalPort();

    final CompletableFuture<Connection> readerFuture = createReaderFuture(acceptorSocket, true);

    // Create a sender that connects to the server socket, which should trigger the
    // reader to be created
    final Connection sender = createWriter(serverSocketPort, false);
    // Get the reader from the future
    final Connection reader = readerFuture.get();

    final ReplyMessage msg = createRelyMessage(sender);

    final List<Connection> connections = new ArrayList<>();
    connections.add(sender);

    final BaseMsgStreamer streamer = MsgStreamer.create(connections, msg, false, stats, bufferPool);
    streamer.writeMessage();

    await().untilAsserted(() -> verify(reader, times(1)).readMessage(any(), any()));

    assertThat(reader.isClosing()).isFalse();
    verify(reader, times(1)).readHandshakeForReceiver(any());
    verify(reader, times(0)).requestClose(any());
  }

  @Test
  public void testReceiverClosesConnectionIfMembershipCheckFailed() throws Exception {
    final DMStats stats = mock(DMStats.class);
    final BufferPool bufferPool = new BufferPool(stats);
    final ServerSocketChannel acceptorSocket = createReceiverSocket();

    final int serverSocketPort = acceptorSocket.socket().getLocalPort();
    final CompletableFuture<Connection> readerFuture = createReaderFuture(acceptorSocket, false);

    final Connection sender = createWriter(serverSocketPort, true);

    final Connection reader = readerFuture.get();
    final ReplyMessage msg = createRelyMessage(sender);

    final List<Connection> connections = new ArrayList<>();
    connections.add(sender);

    final BaseMsgStreamer streamer = MsgStreamer.create(connections, msg, false, stats, bufferPool);
    streamer.writeMessage();

    await().untilAsserted(() -> assertThat(assertThat(reader.isClosing()).isTrue()));

    verify(reader, times(1)).readHandshakeForReceiver(any());
    verify(reader, times(1)).requestClose("timed out during a membership check");
  }

  /**
   * Start an asynchronous runnable that is waiting for a sender to connect to the socket
   * When the sender connects, this runnable will create a receiver connection and
   * return it to the future.
   */
  private CompletableFuture<Connection> createReaderFuture(ServerSocketChannel acceptorSocket,
      boolean isSenderInView) {
    return CompletableFuture.supplyAsync(
        () -> createReceiverConnectionOnFirstAccept(acceptorSocket, isSenderInView));
  }

  /**
   * Creates a socket for the receiver side. This is the server socket listening for connections.
   */
  private ServerSocketChannel createReceiverSocket() throws IOException {
    final ServerSocketChannel acceptorSocket = ServerSocketChannel.open();
    acceptorSocket.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));
    return acceptorSocket;
  }

  /**
   * Creates a dummy reply message.
   */
  private ReplyMessage createRelyMessage(Connection sender) {
    final ReplyMessage msg = new ReplyMessage();
    msg.setProcessorId(1);
    msg.setRecipient(sender.getRemoteAddress());
    return msg;
  }

  /**
   * Create a sender that connects to the server socket.
   */
  private Connection createWriter(final int serverSocketPort, boolean isCancelInProgress)
      throws IOException {
    final ConnectionTable writerTable = mockConnectionTable();

    final Membership<InternalDistributedMember> membership = mock(Membership.class);
    final TCPConduit conduit = writerTable.getConduit();

    when(conduit.getCancelCriterion().isCancelInProgress()).thenReturn(isCancelInProgress);
    when(conduit.getMembership()).thenReturn(membership);
    when(membership.memberExists(any())).thenReturn(true);
    final InternalDistributedMember remoteAddr =
        new InternalDistributedMember(InetAddress.getLocalHost(), 0, true, true);
    remoteAddr.setDirectChannelPort(serverSocketPort);
    final InternalDistributedMember senderAddr =
        new InternalDistributedMember(InetAddress.getLocalHost(), 1, true, true);
    when(conduit.getDM().getCanonicalId(remoteAddr)).thenReturn(remoteAddr);
    when(conduit.getDM().getCanonicalId(senderAddr)).thenReturn(senderAddr);
    senderAddr.setDirectChannelPort(conduit.getPort());
    when(conduit.getMemberId()).thenReturn(senderAddr);

    return spy(Connection.createSender(membership, writerTable, true, remoteAddr, true,
        System.currentTimeMillis(), 1000, 1000));
  }

  private Connection createReceiverConnectionOnFirstAccept(final ServerSocketChannel acceptorSocket,
      boolean isSenderInView) {
    try {
      final SocketChannel readerSocket = acceptorSocket.accept();
      final ConnectionTable readerTable = mockConnectionTable();
      if (isSenderInView) {
        when(readerTable.getConduit().waitForMembershipCheck(any())).thenReturn(true);
      }

      final Connection reader = spy(new Connection(readerTable, readerSocket.socket()));
      CompletableFuture.runAsync(() -> {
        try {
          reader.initReceiver();
        } catch (final RuntimeException e) {
          e.printStackTrace();
          throw e;
        }
      });
      return reader;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private ConnectionTable mockConnectionTable() throws UnknownHostException {
    final ConnectionTable connectionTable = mock(ConnectionTable.class);
    final Distribution distribution = mock(Distribution.class);
    final DistributionManager distributionManager = mock(DistributionManager.class);
    final DMStats dmStats = mock(DMStats.class);
    final CancelCriterion stopper = mock(CancelCriterion.class);
    final SocketCloser socketCloser = mock(SocketCloser.class);
    final TCPConduit tcpConduit = mock(TCPConduit.class);
    final ThreadsMonitoring threadMonitoring = mock(ThreadsMonitoring.class);
    final AbstractExecutor threadMonitoringExecutor = mock(AbstractExecutor.class);
    final DistributionConfig config = mock(DistributionConfig.class);

    System.setProperty(SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTH_INIT, "true");
    when(connectionTable.getBufferPool()).thenReturn(new BufferPool(dmStats));
    when(connectionTable.getConduit()).thenReturn(tcpConduit);
    when(connectionTable.getDM()).thenReturn(distributionManager);
    when(distributionManager.getConfig()).thenReturn(config);
    when(connectionTable.getSocketCloser()).thenReturn(socketCloser);
    when(distributionManager.getDistribution()).thenReturn(distribution);
    when(stopper.cancelInProgress()).thenReturn(null);
    when(tcpConduit.getCancelCriterion()).thenReturn(stopper);
    when(tcpConduit.getDM()).thenReturn(distributionManager);
    when(tcpConduit.getSocketId()).thenReturn(new InetSocketAddress(getLocalHost(), 10337));
    when(tcpConduit.getStats()).thenReturn(dmStats);
    when(distributionManager.getThreadMonitoring()).thenReturn(threadMonitoring);
    when(threadMonitoring.createAbstractExecutor(any())).thenReturn(threadMonitoringExecutor);
    when(tcpConduit.getConfig()).thenReturn(config);
    tcpConduit.tcpBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;

    doAnswer(invocationOnMock -> {
      final Runnable runnable = (invocationOnMock.getArgument(0));
      CompletableFuture.runAsync(() -> {
        try {
          runnable.run();

        } catch (final Exception e) {
          e.printStackTrace();
          throw e;
        }
      });
      try {
        Thread.sleep(1000);
      } catch (final InterruptedException ex) {
        ex.printStackTrace();
        throw ex;
      }
      return null;
    }).when(connectionTable).executeCommand(any());
    return connectionTable;
  }
}
