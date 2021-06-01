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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.server.CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;
import static org.apache.geode.internal.cache.AbstractCacheServer.MAXIMUM_TIME_BETWEEN_PINGS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.cache.ClientSession;
import org.apache.geode.cache.InterestRegistrationListener;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClock;

public class AbstractCacheServerTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void getMaximumTimeBetweenPings_returnsDefault() {
    InternalCache cache = mock(InternalCache.class);
    CacheServer server = new TestableCacheServer(cache);

    int value = server.getMaximumTimeBetweenPings();

    assertThat(value).isEqualTo(DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS);
  }

  @Test
  public void getMaximumTimeBetweenPings_returnsValueOfSystemProperty() {
    int maximumTimeBetweenPings = DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS * 2;
    String maximumTimeBetweenPingsString = String.valueOf(maximumTimeBetweenPings);
    System.setProperty(MAXIMUM_TIME_BETWEEN_PINGS_PROPERTY, maximumTimeBetweenPingsString);
    InternalCache cache = mock(InternalCache.class);
    CacheServer server = new TestableCacheServer(cache);

    int value = server.getMaximumTimeBetweenPings();

    assertThat(value).isEqualTo(maximumTimeBetweenPings);
  }

  private static class TestableCacheServer extends AbstractCacheServer {

    private TestableCacheServer(InternalCache cache) {
      super(cache, false);
    }

    @Override
    public boolean isRunning() {
      return false;
    }

    @Override
    public void stop() {
      // nothing
    }

    @Override
    public ClientSubscriptionConfig getClientSubscriptionConfig() {
      return null;
    }

    @Override
    public ClientSession getClientSession(DistributedMember member) {
      return null;
    }

    @Override
    public ClientSession getClientSession(String durableClientId) {
      return null;
    }

    @Override
    public Set<ClientSession> getAllClientSessions() {
      return null;
    }

    @Override
    public void registerInterestRegistrationListener(InterestRegistrationListener listener) {
      // nothing
    }

    @Override
    public void unregisterInterestRegistrationListener(InterestRegistrationListener listener) {
      // nothing
    }

    @Override
    public Set<InterestRegistrationListener> getInterestRegistrationListeners() {
      return null;
    }

    @Override
    public Acceptor getAcceptor() {
      return null;
    }

    @Override
    public Acceptor createAcceptor(OverflowAttributes overflowAttributes) throws IOException {
      return null;
    }

    @Override
    public String getExternalAddress() {
      return null;
    }

    @Override
    public ConnectionListener getConnectionListener() {
      return null;
    }

    @Override
    public long getTimeLimitMillis() {
      return 0;
    }

    @Override
    public SecurityService getSecurityService() {
      return null;
    }

    @Override
    public Supplier<SocketCreator> getSocketCreatorSupplier() {
      return null;
    }

    @Override
    public CacheClientNotifier.CacheClientNotifierProvider getCacheClientNotifierProvider() {
      return null;
    }

    @Override
    public ClientHealthMonitor.ClientHealthMonitorProvider getClientHealthMonitorProvider() {
      return null;
    }

    @Override
    public String[] getCombinedGroups() {
      return new String[0];
    }

    @Override
    public StatisticsClock getStatisticsClock() {
      return null;
    }
  }
}
