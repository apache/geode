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
  private CacheInfo cache;

  /** Information about the cache server */
  private AdminBridgeServer bridgeInfo;

  ///////////////////// Constructors /////////////////////

  /**
   * Creates a new <code>SystemMemberBridgeServerImpl</code> that administers the given bridge
   * server in the given VM.
   */
  protected SystemMemberBridgeServerImpl(SystemMemberCacheImpl cache, AdminBridgeServer bridgeInfo)

      throws AdminException {

    this.vm = cache.getVM();
    this.cache = cache.getCacheInfo();
    this.bridgeInfo = bridgeInfo;
  }

  //////////////////// Instance Methods ////////////////////

  /**
   * Throws an <code>AdminException</code> if this cache server is running.
   */
  private void checkRunning() throws AdminException {
    if (this.isRunning()) {
      throw new AdminException(
          "Cannot change the configuration of a running cache server.");
    }
  }

  @Override
  public int getPort() {
    return this.bridgeInfo.getPort();
  }

  @Override
  public void setPort(int port) throws AdminException {
    checkRunning();
    this.bridgeInfo.setPort(port);
  }

  @Override
  public void start() throws AdminException {
    this.vm.startBridgeServer(this.cache, this.bridgeInfo);
  }

  @Override
  public boolean isRunning() {
    return this.bridgeInfo.isRunning();
  }

  @Override
  public void stop() throws AdminException {
    this.vm.stopBridgeServer(this.cache, this.bridgeInfo);
  }

  /**
   * Returns the VM-unique id of this cache server
   */
  protected int getBridgeId() {
    return this.bridgeInfo.getId();
  }

  @Override
  public void refresh() {
    try {
      this.bridgeInfo = this.vm.getBridgeInfo(this.cache, this.bridgeInfo.getId());

    } catch (AdminException ex) {
      throw new InternalGemFireException(
          "Unexpected exception while refreshing",
          ex);
    }
  }

  @Override
  public String getBindAddress() {
    return this.bridgeInfo.getBindAddress();
  }

  @Override
  public void setBindAddress(String address) throws AdminException {
    checkRunning();
    this.bridgeInfo.setBindAddress(address);
  }

  @Override
  public String getHostnameForClients() {
    return this.bridgeInfo.getHostnameForClients();
  }

  @Override
  public void setHostnameForClients(String name) throws AdminException {
    checkRunning();
    this.bridgeInfo.setHostnameForClients(name);
  }

  @Override
  public void setNotifyBySubscription(boolean b) throws AdminException {
    checkRunning();
    this.bridgeInfo.setNotifyBySubscription(b);
  }

  @Override
  public boolean getNotifyBySubscription() {
    return this.bridgeInfo.getNotifyBySubscription();
  }

  @Override
  public void setSocketBufferSize(int socketBufferSize) throws AdminException {
    checkRunning();
    this.bridgeInfo.setSocketBufferSize(socketBufferSize);
  }

  @Override
  public int getSocketBufferSize() {
    return this.bridgeInfo.getSocketBufferSize();
  }

  public void setTcpDelay(boolean setting) throws AdminException {
    checkRunning();
    this.bridgeInfo.setTcpNoDelay(setting);
  }

  public boolean getTcpDelay() {
    return this.bridgeInfo.getTcpNoDelay();
  }

  @Override
  public void setMaximumTimeBetweenPings(int maximumTimeBetweenPings) throws AdminException {
    checkRunning();
    this.bridgeInfo.setMaximumTimeBetweenPings(maximumTimeBetweenPings);
  }

  @Override
  public int getMaximumTimeBetweenPings() {
    return this.bridgeInfo.getMaximumTimeBetweenPings();
  }

  @Override
  public int getMaxConnections() {
    return this.bridgeInfo.getMaxConnections();
  }

  @Override
  public void setMaxConnections(int maxCons) throws AdminException {
    checkRunning();
    this.bridgeInfo.setMaxConnections(maxCons);
  }

  @Override
  public int getMaxThreads() {
    return this.bridgeInfo.getMaxThreads();
  }

  @Override
  public void setMaxThreads(int maxThreads) throws AdminException {
    checkRunning();
    this.bridgeInfo.setMaxThreads(maxThreads);
  }

  @Override
  public int getMaximumMessageCount() {
    return this.bridgeInfo.getMaximumMessageCount();
  }

  @Override
  public void setMaximumMessageCount(int maxMessageCount) throws AdminException {
    checkRunning();
    this.bridgeInfo.setMaximumMessageCount(maxMessageCount);
  }

  @Override
  public int getMessageTimeToLive() {
    return this.bridgeInfo.getMessageTimeToLive();
  }

  @Override
  public void setMessageTimeToLive(int messageTimeToLive) throws AdminException {
    checkRunning();
    this.bridgeInfo.setMessageTimeToLive(messageTimeToLive);
  }

  @Override
  public void setGroups(String[] groups) throws AdminException {
    checkRunning();
    this.bridgeInfo.setGroups(groups);
  }

  @Override
  public String[] getGroups() {
    return this.bridgeInfo.getGroups();
  }

  @Override
  public String getLoadProbe() {
    return this.bridgeInfo.getLoadProbe().toString();
  }

  @Override
  public void setLoadProbe(ServerLoadProbe loadProbe) throws AdminException {
    checkRunning();
    if (!(loadProbe instanceof Serializable)) {
      throw new IllegalArgumentException(
          "Load probe must be Serializable to be used with admin API");
    }
    this.bridgeInfo.setLoadProbe(loadProbe);
  }

  @Override
  public long getLoadPollInterval() {
    return this.bridgeInfo.getLoadPollInterval();
  }

  @Override
  public void setLoadPollInterval(long loadPollInterval) throws AdminException {
    checkRunning();
    this.bridgeInfo.setLoadPollInterval(loadPollInterval);
  }


}
