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

package org.apache.geode.management.configuration;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.api.RestfulEndpoint;

@Experimental
public class MemberConfig extends CacheElement implements RuntimeCacheElement, RestfulEndpoint {

  private static final long serialVersionUID = -6262538068604902018L;

  public static final String MEMBER_CONFIG_ENDPOINT = "/members";

  private boolean isLocator;
  private boolean isCoordinator;
  private String id;
  private String host;
  private String status;
  private int pid;
  // Only relevant for locators - will be suppressed if null
  private Integer port;
  // Only relevant for servers - will be suppressed if empty
  private List<CacheServerConfig> cacheServers = new ArrayList<>();
  private long maxHeap;
  private long initialHeap;
  private long usedHeap;
  private String logFile;
  private String workingDirectory;
  private int clientConnections;

  public static class CacheServerConfig {
    private int port;
    private int maxConnections;
    private int maxThreads;

    public CacheServerConfig() {}

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public int getMaxConnections() {
      return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
    }

    public int getMaxThreads() {
      return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
      this.maxThreads = maxThreads;
    }
  }

  public MemberConfig() {}

  public void setId(String id) {
    this.id = id;
  }

  public boolean isLocator() {
    return isLocator;
  }

  public void setLocator(boolean locator) {
    isLocator = locator;
  }

  public boolean isCoordinator() {
    return isCoordinator;
  }

  public void setCoordinator(boolean coordinator) {
    isCoordinator = coordinator;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public int getPid() {
    return pid;
  }

  public void setPid(int pid) {
    this.pid = pid;
  }

  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  public List<CacheServerConfig> getCacheServers() {
    return cacheServers;
  }

  public void addCacheServer(CacheServerConfig cacheServer) {
    cacheServers.add(cacheServer);
  }

  @Override
  public String getEndpoint() {
    return MEMBER_CONFIG_ENDPOINT;
  }

  @Override
  public String getId() {
    return id;
  }

  public List<String> getGroups() {
    return groups;
  }

  public void setGroups(List<String> groups) {
    this.groups = groups;
  }

  public long getMaxHeap() {
    return maxHeap;
  }

  public void setMaxHeap(long maxHeap) {
    this.maxHeap = maxHeap;
  }

  public long getInitialHeap() {
    return initialHeap;
  }

  public void setInitialHeap(long initialHeap) {
    this.initialHeap = initialHeap;
  }

  public long getUsedHeap() {
    return usedHeap;
  }

  public void setUsedHeap(long usedHeap) {
    this.usedHeap = usedHeap;
  }

  public String getLogFile() {
    return logFile;
  }

  public void setLogFile(String logFile) {
    this.logFile = logFile;
  }

  public String getWorkingDirectory() {
    return workingDirectory;
  }

  public void setWorkingDirectory(String workingDirectory) {
    this.workingDirectory = workingDirectory;
  }

  public int getClientConnections() {
    return clientConnections;
  }

  public void setClientConnections(int clientConnections) {
    this.clientConnections = clientConnections;
  }
}
