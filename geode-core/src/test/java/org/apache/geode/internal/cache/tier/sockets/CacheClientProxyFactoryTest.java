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

import static org.apache.geode.internal.cache.tier.sockets.Handshake.CONFLATION_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.Socket;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxyFactory.InternalCacheClientProxyFactory;
import org.apache.geode.internal.net.NioFilter;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsManager;

public class CacheClientProxyFactoryTest {

  private CacheClientNotifier notifier;
  private Socket socket;
  private ClientProxyMembershipID proxyId;
  private KnownVersion clientVersion;
  private SecurityService securityService;
  private Subject subject;
  private StatisticsClock statisticsClock;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    notifier = mock(CacheClientNotifier.class);
    socket = mock(Socket.class);
    proxyId = mock(ClientProxyMembershipID.class);
    clientVersion = mock(KnownVersion.class);
    securityService = mock(SecurityService.class);
    subject = mock(Subject.class);
    statisticsClock = mock(StatisticsClock.class);

    InetAddress inetAddress = mock(InetAddress.class);
    InternalCache cache = mock(InternalCache.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);

    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(inetAddress.getHostAddress()).thenReturn("localhost");
    when(member.getId()).thenReturn("memberId");
    when(notifier.getCache()).thenReturn(cache);
    when(notifier.getAcceptorStats()).thenReturn(mock(CacheServerStats.class));
    when(proxyId.getDistributedMember()).thenReturn(member);
    when(socket.getInetAddress()).thenReturn(inetAddress);
    when(socket.getPort()).thenReturn(33333);
    when(system.getStatisticsManager()).thenReturn(mock(StatisticsManager.class));
  }

  @Test
  public void createsCacheClientProxyByDefault() {
    CacheClientProxyFactory factory = new CacheClientProxyFactory();

    CacheClientProxy proxy = factory.create(notifier, socket, proxyId, false, CONFLATION_DEFAULT,
        clientVersion, 0, false, securityService, subject, statisticsClock, null);

    assertThat(proxy).isExactlyInstanceOf(CacheClientProxy.class);
  }

  @Test
  public void usesCustomInternalFactorySpecifiedByProperty() {
    System.setProperty(CacheClientProxyFactory.INTERNAL_FACTORY_PROPERTY,
        SubCacheClientProxyFactory.class.getName());
    CacheClientProxyFactory factory = new CacheClientProxyFactory();

    CacheClientProxy proxy = factory.create(notifier, socket, proxyId, false, CONFLATION_DEFAULT,
        clientVersion, 0, false, securityService, subject, statisticsClock, null);

    assertThat(proxy).isExactlyInstanceOf(SubCacheClientProxy.class);

  }

  public static class SubCacheClientProxyFactory implements InternalCacheClientProxyFactory {

    @Override
    public CacheClientProxy create(CacheClientNotifier notifier, Socket socket,
        ClientProxyMembershipID proxyId, boolean isPrimary, byte clientConflation,
        KnownVersion clientVersion, long acceptorId, boolean notifyBySubscription,
        SecurityService securityService, Subject subject, StatisticsClock statisticsClock,
        NioFilter ioFilter)
        throws CacheException {
      return new SubCacheClientProxy(notifier, socket, proxyId, isPrimary, clientConflation,
          clientVersion, acceptorId, notifyBySubscription, securityService, subject,
          statisticsClock, ioFilter);
    }
  }

  private static class SubCacheClientProxy extends CacheClientProxy {

    SubCacheClientProxy(CacheClientNotifier notifier, Socket socket,
        ClientProxyMembershipID proxyId, boolean isPrimary, byte clientConflation,
        KnownVersion clientVersion, long acceptorId, boolean notifyBySubscription,
        SecurityService securityService, Subject subject, StatisticsClock statisticsClock,
        NioFilter ioFilter)
        throws CacheException {
      super(notifier, socket, proxyId, isPrimary, clientConflation, clientVersion, acceptorId,
          notifyBySubscription, securityService, subject, statisticsClock, ioFilter);
    }
  }
}
