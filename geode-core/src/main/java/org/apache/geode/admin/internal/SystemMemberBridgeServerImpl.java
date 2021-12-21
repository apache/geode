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
package org.apache.geode.admin.internal;

import java.io.Serializable;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.SystemMemberBridgeServer;
import org.apache.geode.admin.SystemMemberCacheServer;
import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.internal.admin.AdminBridgeServer;
import org.apache.geode.internal.admin.CacheInfo;
import org.apache.geode.internal.admin.GemFireVM;

/**
 * Implementation of an object used for managing cache servers.
 *
 * @since GemFire 4.0
 */
public class SystemMemberBridgeServerImpl
    implements SystemMemberCacheServer, SystemMemberBridgeServer {

  /** The VM in which the cache server resides */
  private final GemFireVM vm;

  /** The cache server by this cache server */
  private final CacheInfo cache;

  /** Information about the cache server */
  private AdminBridgeServer bridgeInfo;

  ///////////////////// Constructors /////////////////////

  /**
   * Creates a new <code>SystemMemberBridgeServerImpl</code> that administers the given bridge
   * server in the given VM.
   */
  protected SystemMemberBridgeServerImpl(SystemMemberCacheImpl cache, AdminBridgeServer bridgeInfo)

      throws AdminException {

    vm = cache.getVM();
    this.cache = cache.getCacheInfo();
    this.bridgeInfo = bridgeInfo;
  }

  //////////////////// Instance Methods ////////////////////

  /**
   * Throws an <code>AdminException</code> if this cache server is running.
   */
  private void checkRunning() throws AdminException {
    if (isRunning()) {
      throw new AdminException(
          "Cannot change the configuration of a running cache server.");
    }
  }

  @Override
  public int getPort() {
    return bridgeInfo.getPort();
  }

  @Override
  public void setPort(int port) throws AdminException {
    checkRunning();
    bridgeInfo.setPort(port);
  }

  @Override
  public void start() throws AdminException {
    vm.startBridgeServer(cache, bridgeInfo);
  }

  @Override
  public boolean isRunning() {
    return bridgeInfo.isRunning();
  }

  @Override
  public void stop() throws AdminException {
    vm.stopBridgeServer(cache, bridgeInfo);
  }

  /**
   * Returns the VM-unique id of this cache server
   */
  protected int getBridgeId() {
    return bridgeInfo.getId();
  }

  @Override
  public void refresh() {
    try {
      bridgeInfo = vm.getBridgeInfo(cache, bridgeInfo.getId());

    } catch (AdminException ex) {
      throw new InternalGemFireException(
          "Unexpected exception while refreshing",
          ex);
    }
  }

  @Override
  public String getBindAddress() {
    return bridgeInfo.getBindAddress();
  }

  @Override
  public void setBindAddress(String address) throws AdminException {
    checkRunning();
    bridgeInfo.setBindAddress(address);
  }

  @Override
  public String getHostnameForClients() {
    return bridgeInfo.getHostnameForClients();
  }

  @Override
  public void setHostnameForClients(String name) throws AdminException {
    checkRunning();
    bridgeInfo.setHostnameForClients(name);
  }

  @Override
  public void setNotifyBySubscription(boolean b) throws AdminException {
    checkRunning();
    bridgeInfo.setNotifyBySubscription(b);
  }

  @Override
  public boolean getNotifyBySubscription() {
    return bridgeInfo.getNotifyBySubscription();
  }

  @Override
  public void setSocketBufferSize(int socketBufferSize) throws AdminException {
    checkRunning();
    bridgeInfo.setSocketBufferSize(socketBufferSize);
  }

  @Override
  public int getSocketBufferSize() {
    return bridgeInfo.getSocketBufferSize();
  }

  public void setTcpDelay(boolean setting) throws AdminException {
    checkRunning();
    bridgeInfo.setTcpNoDelay(setting);
  }

  public boolean getTcpDelay() {
    return bridgeInfo.getTcpNoDelay();
  }

  @Override
  public void setMaximumTimeBetweenPings(int maximumTimeBetweenPings) throws AdminException {
    checkRunning();
    bridgeInfo.setMaximumTimeBetweenPings(maximumTimeBetweenPings);
  }

  @Override
  public int getMaximumTimeBetweenPings() {
    return bridgeInfo.getMaximumTimeBetweenPings();
  }

  @Override
  public int getMaxConnections() {
    return bridgeInfo.getMaxConnections();
  }

  @Override
  public void setMaxConnections(int maxCons) throws AdminException {
    checkRunning();
    bridgeInfo.setMaxConnections(maxCons);
  }

  @Override
  public int getMaxThreads() {
    return bridgeInfo.getMaxThreads();
  }

  @Override
  public void setMaxThreads(int maxThreads) throws AdminException {
    checkRunning();
    bridgeInfo.setMaxThreads(maxThreads);
  }

  @Override
  public int getMaximumMessageCount() {
    return bridgeInfo.getMaximumMessageCount();
  }

  @Override
  public void setMaximumMessageCount(int maxMessageCount) throws AdminException {
    checkRunning();
    bridgeInfo.setMaximumMessageCount(maxMessageCount);
  }

  @Override
  public int getMessageTimeToLive() {
    return bridgeInfo.getMessageTimeToLive();
  }

  @Override
  public void setMessageTimeToLive(int messageTimeToLive) throws AdminException {
    checkRunning();
    bridgeInfo.setMessageTimeToLive(messageTimeToLive);
  }

  @Override
  public void setGroups(String[] groups) throws AdminException {
    checkRunning();
    bridgeInfo.setGroups(groups);
  }

  @Override
  public String[] getGroups() {
    return bridgeInfo.getGroups();
  }

  @Override
  public String getLoadProbe() {
    return bridgeInfo.getLoadProbe().toString();
  }

  @Override
  public void setLoadProbe(ServerLoadProbe loadProbe) throws AdminException {
    checkRunning();
    if (!(loadProbe instanceof Serializable)) {
      throw new IllegalArgumentException(
          "Load probe must be Serializable to be used with admin API");
    }
    bridgeInfo.setLoadProbe(loadProbe);
  }

  @Override
  public long getLoadPollInterval() {
    return bridgeInfo.getLoadPollInterval();
  }

  @Override
  public void setLoadPollInterval(long loadPollInterval) throws AdminException {
    checkRunning();
    bridgeInfo.setLoadPollInterval(loadPollInterval);
  }


}
