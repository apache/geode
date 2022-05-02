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
import java.util.concurrent.ExecutionException;

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
   * This test uses a real socket, but mocks all other collaborators
   * of connection, such as the InternalDistributedSystem.
   */
  @Test
  public void testDataTransmittedBetweenSenderAndReceiverIfMembershipCheckPassed()
      throws Exception {
    final Connection reader = createConnectionsAndWriteMessage(true, false, false);

    await().untilAsserted(() -> verify(reader, times(1)).readMessage(any(), any()));
    assertThat(reader.isClosing()).isFalse();
    verify(reader).readHandshakeForReceiver(any());
    verify(reader).readMessage(any(), any());
    verify(reader, times(0)).requestClose(any());
  }

  @Test
  public void testReceiverClosesConnectionIfMembershipCheckFailed() throws Exception {
    final Connection reader = createConnectionsAndWriteMessage(false, true, true);

    await().untilAsserted(() -> assertThat(assertThat(reader.isClosing()).isTrue()));
    verify(reader).readHandshakeForReceiver(any());
    verify(reader).requestClose("timed out during a membership check");
  }

  private Connection createConnectionsAndWriteMessage(final boolean isSenderInView,
      final boolean isCancelInProgress, final boolean waitUntilReaderExits)
      throws IOException, InterruptedException, ExecutionException {
    final DMStats stats = mock(DMStats.class);
    final BufferPool bufferPool = new BufferPool(stats);
    final ServerSocketChannel acceptorSocket = createReceiverSocket();

    final int serverSocketPort = acceptorSocket.socket().getLocalPort();
    final CompletableFuture<Connection> readerFuture =
        createReaderFuture(acceptorSocket, isSenderInView);

    final Connection sender =
        createWriter(serverSocketPort, isCancelInProgress, waitUntilReaderExits);

    final Connection reader = readerFuture.get();
    final ReplyMessage msg = createReplyMessage(sender);

    final List<Connection> connections = new ArrayList<>();
    connections.add(sender);

    final BaseMsgStreamer streamer = MsgStreamer.create(connections, msg, false, stats, bufferPool);
    streamer.writeMessage();
    return reader;
  }

  /**
   * Start an asynchronous runnable that is waiting for a sender to connect to the socket
   * When the sender connects, this runnable will create a receiver connection and
   * return it to the future.
   */
  private CompletableFuture<Connection> createReaderFuture(final ServerSocketChannel acceptorSocket,
      final boolean isSenderInView) {
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
  private ReplyMessage createReplyMessage(final Connection sender) {
    final ReplyMessage msg = new ReplyMessage();
    msg.setProcessorId(1);
    msg.setRecipient(sender.getRemoteAddress());
    return msg;
  }

  /**
   * Create a sender that connects to the server socket.
   */
  private Connection createWriter(final int serverSocketPort, final boolean isCancelInProgress,
      boolean waitUntilReaderExits)
      throws IOException {
    final ConnectionTable writerTable = mockConnectionTable(waitUntilReaderExits);

    final Membership<InternalDistributedMember> membership = mock(Membership.class);
    final TCPConduit conduit = writerTable.getConduit();
    final InternalDistributedMember remoteAddr =
        new InternalDistributedMember(InetAddress.getLocalHost(), 0, true, true);
    final InternalDistributedMember senderAddr =
        new InternalDistributedMember(InetAddress.getLocalHost(), 1, true, true);

    when(conduit.getCancelCriterion().isCancelInProgress()).thenReturn(isCancelInProgress);
    when(conduit.getMembership()).thenReturn(membership);
    when(conduit.getDM().getCanonicalId(remoteAddr)).thenReturn(remoteAddr);
    when(conduit.getDM().getCanonicalId(senderAddr)).thenReturn(senderAddr);
    when(conduit.getMemberId()).thenReturn(senderAddr);
    when(membership.memberExists(any())).thenReturn(true);

    remoteAddr.setDirectChannelPort(serverSocketPort);
    senderAddr.setDirectChannelPort(conduit.getPort());

    return spy(Connection.createSender(membership, writerTable, true, remoteAddr, true,
        System.currentTimeMillis(), 1000, 1000, false));
  }

  private Connection createReceiverConnectionOnFirstAccept(final ServerSocketChannel acceptorSocket,
      final boolean isSenderInView) {
    try {
      final SocketChannel readerSocket = acceptorSocket.accept();
      final ConnectionTable readerTable = mockConnectionTable(false);
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

  /**
   * @param waitUntilReaderExits if true, start reader thread and wait until it exits,
   *        otherwise run it asynchronously.
   */
  private ConnectionTable mockConnectionTable(final boolean waitUntilReaderExits)
      throws UnknownHostException {
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
    tcpConduit.tcpBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;

    when(connectionTable.getBufferPool()).thenReturn(new BufferPool(dmStats));
    when(connectionTable.getConduit()).thenReturn(tcpConduit);
    when(connectionTable.getDM()).thenReturn(distributionManager);
    when(connectionTable.getSocketCloser()).thenReturn(socketCloser);

    when(distributionManager.getConfig()).thenReturn(config);
    when(distributionManager.getDistribution()).thenReturn(distribution);
    when(distributionManager.getThreadMonitoring()).thenReturn(threadMonitoring);

    when(tcpConduit.getDM()).thenReturn(distributionManager);
    when(tcpConduit.getCancelCriterion()).thenReturn(stopper);
    when(tcpConduit.getSocketId()).thenReturn(new InetSocketAddress(getLocalHost(), 10337));
    when(tcpConduit.getStats()).thenReturn(dmStats);
    when(tcpConduit.getConfig()).thenReturn(config);

    when(stopper.cancelInProgress()).thenReturn(null);
    when(threadMonitoring.createAbstractExecutor(any())).thenReturn(threadMonitoringExecutor);

    doAnswer(invocationOnMock -> {
      final Runnable runnable = (invocationOnMock.getArgument(0));
      if (waitUntilReaderExits) {
        startReader(runnable);
      } else {
        CompletableFuture.runAsync(() -> startReader(runnable));
      }
      return null;
    }).when(connectionTable).executeCommand(any());
    return connectionTable;
  }

  private void startReader(Runnable runnable) {
    try {
      runnable.run();
    } catch (final Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
