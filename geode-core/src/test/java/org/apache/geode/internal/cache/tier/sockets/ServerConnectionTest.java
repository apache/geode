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
 *
 */

package org.apache.geode.internal.cache.tier.sockets;


import static org.apache.geode.internal.i18n.LocalizedStrings.HandShake_NO_SECURITY_CREDENTIALS_ARE_PROVIDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.StringId;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.RestoreLocaleRule;

@Category(UnitTest.class)
public class ServerConnectionTest {

  /**
   * This test assumes Locale is in English. Before the test, change the locale of Locale and
   * StringId to English and restore the original locale after the test.
   */
  @Rule
  public final RestoreLocaleRule restoreLocale =
      new RestoreLocaleRule(Locale.ENGLISH, l -> StringId.setLocale(l));

  @Mock
  private Message requestMsg;

  @Mock
  private MessageIdExtractor messageIdExtractor;

  @InjectMocks
  private ServerConnection serverConnection;

  private AcceptorImpl acceptor;
  private Socket socket;
  private ServerSideHandshake handshake;
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

    handshake = mock(ServerSideHandshake.class);
    when(handshake.getEncryptor()).thenReturn(mock(Encryptor.class));

    serverConnection =
        new ServerConnectionFactory().makeServerConnection(socket, cache, null, stats, 0, 0, null,
            CommunicationMode.PrimaryServerToClient.getModeNumber(), acceptor, securityService);
    MockitoAnnotations.initMocks(this);
  }


  @Test
  public void pre65SecureShouldReturnUserAuthId() {
    long userAuthId = 12345L;
    serverConnection.setUserAuthId(userAuthId);

    when(handshake.getVersion()).thenReturn(Version.GFE_61);
    when(requestMsg.isSecureMode()).thenReturn(true);

    assertThat(serverConnection.getUniqueId()).isEqualTo(userAuthId);
  }

  @Test
  public void pre65NonSecureShouldReturnUserAuthId() {
    long userAuthId = 12345L;
    serverConnection.setUserAuthId(userAuthId);

    when(handshake.getVersion()).thenReturn(Version.GFE_61);
    when(requestMsg.isSecureMode()).thenReturn(false);

    assertThat(serverConnection.getUniqueId()).isEqualTo(userAuthId);
  }


  @Test
  public void post65SecureShouldUseUniqueIdFromMessage() {
    long uniqueIdFromMessage = 23456L;
    when(handshake.getVersion()).thenReturn(Version.GFE_82);
    serverConnection.setRequestMsg(requestMsg);

    assertThat(serverConnection.getRequestMessage()).isSameAs(requestMsg);
    when(requestMsg.isSecureMode()).thenReturn(true);

    when(messageIdExtractor.getUniqueIdFromMessage(any(Message.class), any(Encryptor.class),
        anyLong())).thenReturn(uniqueIdFromMessage);
    serverConnection.setMessageIdExtractor(messageIdExtractor);

    assertThat(serverConnection.getUniqueId()).isEqualTo(uniqueIdFromMessage);
  }

  @Test
  public void post65NonSecureShouldThrow() {
    when(handshake.getVersion()).thenReturn(Version.GFE_82);
    when(requestMsg.isSecureMode()).thenReturn(false);

    assertThatThrownBy(serverConnection::getUniqueId)
        .isExactlyInstanceOf(AuthenticationRequiredException.class)
        .hasMessage(HandShake_NO_SECURITY_CREDENTIALS_ARE_PROVIDED.getRawText());
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
    public void recv() throws IOException {
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
      setRequestMsg(testMessage);
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
