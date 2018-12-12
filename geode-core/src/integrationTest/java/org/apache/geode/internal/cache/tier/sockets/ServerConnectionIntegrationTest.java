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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockitoAnnotations;

import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ServerConnectionIntegrationTest {

  private AcceptorImpl acceptor;
  private Socket socket;
  private InternalCache cache;
  private SecurityService securityService;
  private CacheServerStats stats;

  @Before
  public void setUp() throws IOException {
    acceptor = mock(AcceptorImpl.class);

    InetAddress inetAddress = mock(InetAddress.class);
    when(inetAddress.getHostAddress()).thenReturn("localhost");

    socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(inetAddress);

    cache = mock(InternalCache.class);
    securityService = mock(SecurityService.class);

    stats = mock(CacheServerStats.class);
  }

  class TestMessage extends Message {
    private final Lock lock = new ReentrantLock();
    private final Condition testGate = lock.newCondition();
    private boolean signalled = false;

    public TestMessage() {
      super(3, Version.CURRENT);
      messageType = MessageType.REQUEST;
      securePart = new Part();
    }

    @Override
    public void receive() throws IOException {
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

    public void continueProcessing() {
      lock.lock();
      testGate.signal();
      signalled = true;
      lock.unlock();
    }
  }

  class TestServerConnection extends OriginalServerConnection {

    private TestMessage testMessage;

    /**
     * Creates a new <code>ServerConnection</code> that processes messages received from an edge
     * client over a given <code>Socket</code>.
     */
    public TestServerConnection(Socket socket, InternalCache internalCache,
        CachedRegionHelper helper, CacheServerStats stats, int hsTimeout, int socketBufferSize,
        String communicationModeStr, byte communicationMode, Acceptor acceptor,
        SecurityService securityService) {
      super(socket, internalCache, helper, stats, hsTimeout, socketBufferSize, communicationModeStr,
          communicationMode, acceptor, securityService);

      setClientDisconnectCleanly(); // Not clear where this is supposed to be set in the timeout
      // path
    }

    @Override
    protected void doHandshake() {
      ClientProxyMembershipID proxyID = mock(ClientProxyMembershipID.class);
      when(proxyID.getDistributedMember()).thenReturn(mock(InternalDistributedMember.class));
      ServerSideHandshake handshake = mock(ServerSideHandshake.class);
      when(handshake.getMembershipId()).thenReturn(proxyID);
      when(handshake.getVersion()).thenReturn(Version.CURRENT);

      setHandshake(handshake);
      setProxyId(proxyID);

      processHandShake();
      initializeCommands();

      setFakeRequest();

      long fakeId = -1;
      MessageIdExtractor extractor = mock(MessageIdExtractor.class);
      when(extractor.getUniqueIdFromMessage(getRequestMessage(), handshake.getEncryptor(),
          Connection.DEFAULT_CONNECTION_ID)).thenReturn(fakeId);
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

  /**
   * This test sets up a TestConnection which will register with the ClientHealthMonitor and then
   * block waiting to receive a fake message. This message will arrive just after the health monitor
   * times out this connection and kills it. The test then makes sure that the connection correctly
   * handles the terminated state and exits.
   */
  @Test
  public void terminatingConnectionHandlesNewRequestsGracefully() throws Exception {
    when(cache.getCacheTransactionManager()).thenReturn(mock(TXManagerImpl.class));
    ClientHealthMonitor.createInstance(cache, 100, mock(CacheClientNotifierStats.class));
    ClientHealthMonitor clientHealthMonitor = ClientHealthMonitor.getInstance();
    when(acceptor.getClientHealthMonitor()).thenReturn(clientHealthMonitor);
    when(acceptor.getConnectionListener()).thenReturn(mock(ConnectionListener.class));
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    TestServerConnection testServerConnection =
        new TestServerConnection(socket, cache, mock(CachedRegionHelper.class), stats, 0, 0, null,
            CommunicationMode.PrimaryServerToClient.getModeNumber(), acceptor, securityService);
    MockitoAnnotations.initMocks(this);

    testServerConnection.run();
  }
}
