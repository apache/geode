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
package org.apache.geode.internal.cache.xmlcache;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.geode.cache.ClientSession;
import org.apache.geode.cache.InterestRegistrationListener;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.AbstractCacheServer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClock;

/**
 * Represents a {@link CacheServer} that is created declaratively.
 *
 * @since GemFire 4.0
 */
public class CacheServerCreation extends AbstractCacheServer {

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>BridgeServerCreation</code> with the default configuration.
   *
   * @param cache The cache being served
   */
  CacheServerCreation(InternalCache cache) {
    super(cache);
  }

  CacheServerCreation(InternalCache cache, boolean attachListener) {
    super(cache, attachListener);
  }

  /**
   * Constructor for retaining cache server information during auto-reconnect
   *
   */
  public CacheServerCreation(InternalCache cache, CacheServer other) {
    super(cache);
    setPort(other.getPort());
    setBindAddress(other.getBindAddress());
    setHostnameForClients(other.getHostnameForClients());
    setMaxConnections(other.getMaxConnections());
    setMaxThreads(other.getMaxThreads());
    setNotifyBySubscription(other.getNotifyBySubscription());
    setSocketBufferSize(other.getSocketBufferSize());
    setTcpNoDelay(other.getTcpNoDelay());
    setMaximumTimeBetweenPings(other.getMaximumTimeBetweenPings());
    setMaximumMessageCount(other.getMaximumMessageCount());
    setMessageTimeToLive(other.getMessageTimeToLive());
    setGroups(other.getGroups());
    setLoadProbe(other.getLoadProbe());
    setLoadPollInterval(other.getLoadPollInterval());
    ClientSubscriptionConfig cscOther = other.getClientSubscriptionConfig();
    ClientSubscriptionConfig cscThis = getClientSubscriptionConfig();
    // added for configuration of ha overflow
    cscThis.setEvictionPolicy(cscOther.getEvictionPolicy());
    cscThis.setCapacity(cscOther.getCapacity());
    String diskStoreName = cscOther.getDiskStoreName();
    if (diskStoreName != null) {
      cscThis.setDiskStoreName(diskStoreName);
    } else {
      cscThis.setOverflowDirectory(cscOther.getOverflowDirectory());
    }
    // this.cache = null; we should null out the cache since we no longer need it
  }

  ///////////////////// Instance Methods /////////////////////

  @Override
  public void start() throws IOException {
    // This method is invoked during testing, but it is not necessary
    // to do anything.
  }

  @Override
  public void setNotifyBySubscription(boolean b) {
    notifyBySubscription = b;
  }

  @Override
  public boolean getNotifyBySubscription() {
    return notifyBySubscription;
  }

  @Override
  public void setSocketBufferSize(int socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
  }

  @Override
  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  @Override
  public void setTcpNoDelay(boolean setting) {
    tcpNoDelay = setting;
  }

  @Override
  public boolean getTcpNoDelay() {
    return tcpNoDelay;
  }

  @Override
  public void setMaximumTimeBetweenPings(int maximumTimeBetweenPings) {
    this.maximumTimeBetweenPings = maximumTimeBetweenPings;
  }

  @Override
  public int getMaximumTimeBetweenPings() {
    return maximumTimeBetweenPings;
  }

  @Override
  public int getMaximumMessageCount() {
    return maximumMessageCount;
  }

  @Override
  public void setMaximumMessageCount(int maximumMessageCount) {
    this.maximumMessageCount = maximumMessageCount;
  }

  @Override
  public int getMessageTimeToLive() {
    return messageTimeToLive;
  }

  @Override
  public void setMessageTimeToLive(int messageTimeToLive) {
    this.messageTimeToLive = messageTimeToLive;
  }

  @Override
  public boolean isRunning() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  /**
   * Returns whether or not this cache server has the same configuration as another cache server.
   */
  @Override
  public boolean sameAs(CacheServer other) {
    ClientSubscriptionConfig cscThis = getClientSubscriptionConfig();
    ClientSubscriptionConfig cscOther = other.getClientSubscriptionConfig();
    boolean result =
        isCacheServerPortEquals(other) && getSocketBufferSize() == other.getSocketBufferSize()
            && getMaximumTimeBetweenPings() == other.getMaximumTimeBetweenPings()
            && getNotifyBySubscription() == other.getNotifyBySubscription()
            && getMaxConnections() == other.getMaxConnections()
            && getMaxThreads() == other.getMaxThreads()
            && getMaximumMessageCount() == other.getMaximumMessageCount()
            && getMessageTimeToLive() == other.getMessageTimeToLive()
            && getTcpNoDelay() == other.getTcpNoDelay()
            && cscThis.getCapacity() == cscOther.getCapacity()
            && cscThis.getEvictionPolicy().equals(cscOther.getEvictionPolicy());
    String diskStoreName = cscThis.getDiskStoreName();
    if (diskStoreName != null) {
      result = result && diskStoreName.equals(cscOther.getDiskStoreName());
    } else {
      result = result && cscThis.getOverflowDirectory().equals(cscOther.getOverflowDirectory());
    }
    return result;
  }

  /**
   * Compare configured cacheServer port against the running cacheServer port. If the current
   * cacheServer port is set to 0 a random ephemeral port will be used so there is no need to
   * compare returning <code>true</code>. If a port is specified, return the proper comparison.
   *
   * @param other CacheServer
   */
  private boolean isCacheServerPortEquals(CacheServer other) {
    return getPort() == 0 || getPort() == other.getPort();
  }

  @Override
  public String toString() {
    return "BridgeServerCreation on port " + getPort() + " notify by subscription "
        + getNotifyBySubscription() + " maximum time between pings "
        + getMaximumTimeBetweenPings() + " socket buffer size " + getSocketBufferSize()
        + " maximum connections " + getMaxConnections() + " maximum threads "
        + getMaxThreads() + " maximum message count " + getMaximumMessageCount()
        + " message time to live " + getMessageTimeToLive() + " groups "
        + Arrays.asList(getGroups()) + " loadProbe " + loadProbe + " loadPollInterval "
        + loadPollInterval + getClientSubscriptionConfig().toString();
  }

  @Override
  public ClientSubscriptionConfig getClientSubscriptionConfig() {
    return clientSubscriptionConfig;
  }

  @Override
  public Set getInterestRegistrationListeners() {
    // TODO Yogesh : implement me
    return null;
  }

  @Override
  public void registerInterestRegistrationListener(InterestRegistrationListener listener) {
    // TODO Yogesh : implement me
  }

  @Override
  public void unregisterInterestRegistrationListener(InterestRegistrationListener listener) {
    // TODO Yogesh : implement me
  }

  @Override
  public Set getAllClientSessions() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public ClientSession getClientSession(DistributedMember member) {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public ClientSession getClientSession(String durableClientId) {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public Acceptor getAcceptor() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public Acceptor createAcceptor(OverflowAttributes overflowAttributes) throws IOException {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public String getExternalAddress() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public ConnectionListener getConnectionListener() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public long getTimeLimitMillis() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public SecurityService getSecurityService() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public Supplier<SocketCreator> getSocketCreatorSupplier() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public CacheClientNotifierProvider getCacheClientNotifierProvider() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public ClientHealthMonitorProvider getClientHealthMonitorProvider() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public String[] getCombinedGroups() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }

  @Override
  public StatisticsClock getStatisticsClock() {
    throw new UnsupportedOperationException("Shouldn't be invoked");
  }
}
