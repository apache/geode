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
package org.apache.geode.distributed;

/**
 * GEODE-5256: Parameters containing startup options specified by the user.
 * Shared from ServerLauncher during startup, and available to other internal classes through static
 * accessors.
 */
public enum ServerLauncherParameters {
  INSTANCE;

  private Integer port = null;
  private Integer maxThreads = null;
  private String bindAddress = null;
  private Integer maxConnections = null;
  private Integer maxMessageCount = null;
  private Integer socketBufferSize = null;
  private Integer messageTimeToLive = null;
  private String hostnameForClients = null;
  private Boolean disableDefaultServer = null;

  public Integer getPort() {
    return port;
  }

  public Integer getMaxThreads() {
    return maxThreads;
  }

  public String getBindAddress() {
    return bindAddress;
  }

  public Integer getMaxConnections() {
    return maxConnections;
  }

  public Integer getMaxMessageCount() {
    return maxMessageCount;
  }

  public Integer getSocketBufferSize() {
    return socketBufferSize;
  }

  public Integer getMessageTimeToLive() {
    return messageTimeToLive;
  }

  public String getHostnameForClients() {
    return hostnameForClients;
  }

  public Boolean isDisableDefaultServer() {
    return disableDefaultServer;
  }

  public ServerLauncherParameters withPort(Integer port) {
    this.port = port;
    return this;
  }

  public ServerLauncherParameters withMaxThreads(Integer maxThreads) {
    this.maxThreads = maxThreads;
    return this;
  }

  public ServerLauncherParameters withBindAddress(String bindAddress) {
    this.bindAddress = bindAddress;
    return this;
  }

  public ServerLauncherParameters withMaxConnections(Integer maxConnections) {
    this.maxConnections = maxConnections;
    return this;
  }

  public ServerLauncherParameters withMaxMessageCount(Integer maxMessageCount) {
    this.maxMessageCount = maxMessageCount;
    return this;
  }

  public ServerLauncherParameters withSocketBufferSize(Integer socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
    return this;
  }

  public ServerLauncherParameters withMessageTimeToLive(Integer messageTimeToLive) {
    this.messageTimeToLive = messageTimeToLive;
    return this;
  }

  public ServerLauncherParameters withHostnameForClients(String hostnameForClients) {
    this.hostnameForClients = hostnameForClients;
    return this;
  }

  public ServerLauncherParameters withDisableDefaultServer(Boolean disableDefaultServer) {
    this.disableDefaultServer = disableDefaultServer;
    return this;
  }

  public void clear() {
    port = null;
    maxThreads = null;
    bindAddress = null;
    maxConnections = null;
    maxMessageCount = null;
    socketBufferSize = null;
    messageTimeToLive = null;
    hostnameForClients = null;
    disableDefaultServer = null;
  }
}
