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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class ServerConnectionIntegrationTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(LENIENT);

  private AcceptorImpl acceptor;
  private Socket socket;
  private InternalCache cache;
  private CachedRegionHelper cachedRegionHelper;
  private SecurityService securityService;
  private CacheServerStats stats;

  @Before
  public void setUp() throws IOException {
    InetAddress inetAddress = mock(InetAddress.class);

    acceptor = mock(AcceptorImpl.class);
    socket = mock(Socket.class);
    cache = mock(InternalCache.class);
    cachedRegionHelper = mock(CachedRegionHelper.class);
    securityService = mock(SecurityService.class);
    stats = mock(CacheServerStats.class);

    when(inetAddress.getHostAddress()).thenReturn("localhost");
    when(socket.getInetAddress()).thenReturn(inetAddress);

    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    DistributionManager distributionManager = mock(DistributionManager.class);
    ThreadsMonitoring threadsMonitoring = mock(ThreadsMonitoring.class);

    when(cachedRegionHelper.getCache()).thenReturn(cache);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getDM()).thenReturn(distributionManager);
    when(distributionManager.getThreadMonitoring()).thenReturn(threadsMonitoring);
  }

  /**
   * This test sets up a TestConnection which will register with the ClientHealthMonitor and then
   * block waiting to receive a fake message. This message will arrive just after the health monitor
   * times out this connection and kills it. The test then makes sure that the connection correctly
   * handles the terminated state and exits.
   */
  @Test
  public void terminatingConnectionHandlesNewRequestsGracefully() {
    ClientHealthMonitor.createInstance(cache, 100, mock(CacheClientNotifierStats.class));
    ClientHealthMonitor clientHealthMonitor = ClientHealthMonitor.getInstance();

    when(cache.getCacheTransactionManager()).thenReturn(mock(TXManagerImpl.class));
    when(acceptor.getClientHealthMonitor()).thenReturn(clientHealthMonitor);
    when(acceptor.getConnectionListener()).thenReturn(mock(ConnectionListener.class));

    TestServerConnection testServerConnection =
        new TestServerConnection(socket, cache, cachedRegionHelper, stats, 0, 0, null,
            CommunicationMode.PrimaryServerToClient.getModeNumber(), acceptor, securityService);

    assertThatCode(() -> testServerConnection.run()).doesNotThrowAnyException();
  }

  private static class TestMessage extends Message {

    private final Lock lock = new ReentrantLock();
    private final Condition testGate = lock.newCondition();
    private volatile boolean signalled;

    TestMessage() {
      super(3, KnownVersion.CURRENT);
      messageType = MessageType.REQUEST;
      securePart = new Part();
    }

    @Override
    public void receive() {
      try {
        lock.lock();
        testGate.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        lock.unlock();
        if (!signalled) {
          fail("Message never received continueProcessing call");
        }
      }
    }

    void continueProcessing() {
      lock.lock();
      testGate.signalAll();
      signalled = true;
      lock.unlock();
    }
  }

  private static class TestServerConnection extends OriginalServerConnection {

    private volatile TestMessage testMessage;

    /**
     * Creates a new {@code ServerConnection} that processes messages received from an edge
     * client over a given {@code Socket}.
     */
    TestServerConnection(Socket socket, InternalCache internalCache,
        CachedRegionHelper cachedRegionHelper, CacheServerStats stats, int hsTimeout,
        int socketBufferSize, String communicationModeStr, byte communicationMode,
        Acceptor acceptor, SecurityService securityService) {
      super(socket, internalCache, cachedRegionHelper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor, securityService);

      // Not clear where this is supposed to be set in the timeout path
      setClientDisconnectCleanly();
    }

    @Override
    protected void doHandshake() {
      ClientProxyMembershipID proxyID = mock(ClientProxyMembershipID.class);
      ServerSideHandshake handshake = mock(ServerSideHandshake.class);
      MessageIdExtractor extractor = mock(MessageIdExtractor.class);

      when(handshake.getVersion()).thenReturn(KnownVersion.CURRENT);
      when(proxyID.getDistributedMember()).thenReturn(mock(InternalDistributedMember.class));

      setHandshake(handshake);
      setProxyId(proxyID);

      processHandShake();
      initializeCommands();

      setFakeRequest();

      setMessageIdExtractor(extractor);
    }

    @Override
    void handleTermination(boolean timedOut) {
      super.handleTermination(timedOut);
      testMessage.continueProcessing();
    }

    private void setFakeRequest() {
      testMessage = new TestMessage();
      setRequestMessage(testMessage);
    }
  }
}
