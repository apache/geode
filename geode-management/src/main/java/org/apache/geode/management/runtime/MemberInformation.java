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
package org.apache.geode.management.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.api.JsonSerializable;


/***
 * Data class to hold the information of the member Used in describe member command
 *
 */
@Experimental
public class MemberInformation extends RuntimeInfo
    implements Identifiable<String>, JsonSerializable {
  private static final long serialVersionUID = 1L;
  private String id;
  private String workingDirPath;
  private String groups;
  private String logFilePath;
  private String statArchiveFilePath;
  private String serverBindAddress;
  private String locators;
  private String status;
  private long heapUsage;
  private long maxHeapSize;
  private long initHeapSize;
  private String cacheXmlFilePath;
  private String host;
  private int processId;
  private int locatorPort;
  private int httpServicePort;
  private String httpServiceBindAddress;
  private boolean isServer;
  private final List<CacheServerInfo> cacheServerList = new ArrayList<>();
  private int clientCount;
  private double cpuUsage;
  private Set<String> hostedRegions;
  private String offHeapMemorySize;
  private boolean webSSL;
  private boolean isSecured;
  private boolean isCoordinator;

  public boolean isSecured() {
    return isSecured;
  }

  public void setSecured(boolean secured) {
    isSecured = secured;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getWorkingDirPath() {
    return workingDirPath;
  }

  public void setWorkingDirPath(String workingDirPath) {
    this.workingDirPath = workingDirPath;
  }

  public String getGroups() {
    return groups;
  }

  public void setGroups(String groups) {
    this.groups = groups;
  }

  public String getLogFilePath() {
    return logFilePath;
  }

  public void setLogFilePath(String logFilePath) {
    this.logFilePath = logFilePath;
  }

  public String getStatArchiveFilePath() {
    return statArchiveFilePath;
  }

  public void setStatArchiveFilePath(String statArchiveFilePath) {
    this.statArchiveFilePath = statArchiveFilePath;
  }

  public double getCpuUsage() {
    return cpuUsage;
  }

  public void setCpuUsage(double cpuUsage) {
    this.cpuUsage = cpuUsage;
  }

  public String getServerBindAddress() {
    return serverBindAddress;
  }

  public void setServerBindAddress(String serverBindAddress) {
    this.serverBindAddress = serverBindAddress;
  }

  public String getLocators() {
    return locators;
  }

  public void setLocators(String locators) {
    this.locators = locators;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public long getHeapUsage() {
    return heapUsage;
  }

  public void setHeapUsage(long heapUsage) {
    this.heapUsage = heapUsage;
  }

  public long getMaxHeapSize() {
    return maxHeapSize;
  }

  public void setMaxHeapSize(long maxHeapSize) {
    this.maxHeapSize = maxHeapSize;
  }

  public String getCacheXmlFilePath() {
    return cacheXmlFilePath;
  }

  public void setCacheXmlFilePath(String cacheXmlFilePath) {
    this.cacheXmlFilePath = cacheXmlFilePath;
  }

  public long getInitHeapSize() {
    return initHeapSize;
  }

  public void setInitHeapSize(long initHeapSize) {
    this.initHeapSize = initHeapSize;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getProcessId() {
    return processId;
  }

  public void setProcessId(int processId) {
    this.processId = processId;
  }

  public int getLocatorPort() {
    return locatorPort;
  }

  public void setLocatorPort(int locatorPort) {
    this.locatorPort = locatorPort;
  }

  public int getHttpServicePort() {
    return httpServicePort;
  }

  public void setHttpServicePort(int httpServicePort) {
    this.httpServicePort = httpServicePort;
  }

  public String getHttpServiceBindAddress() {
    return httpServiceBindAddress;
  }

  public void setHttpServiceBindAddress(String httpServiceBindAddress) {
    this.httpServiceBindAddress = httpServiceBindAddress;
  }

  public boolean isServer() {
    return isServer;
  }

  public void setServer(boolean isServer) {
    this.isServer = isServer;
  }

  public List<CacheServerInfo> getCacheServerInfo() {
    return cacheServerList;
  }

  public void addCacheServerInfo(CacheServerInfo cacheServerInfo) {
    if (cacheServerInfo == null) {
      return;
    }
    cacheServerList.add(cacheServerInfo);
  }

  public int getClientCount() {
    return clientCount;
  }

  public void setClientCount(int clientCount) {
    this.clientCount = clientCount;
  }

  public Set<String> getHostedRegions() {
    return hostedRegions;
  }

  public void setHostedRegions(Set<String> hostedRegions) {
    this.hostedRegions = hostedRegions;
  }

  public String getOffHeapMemorySize() {
    return offHeapMemorySize;
  }

  public void setOffHeapMemorySize(String v) {
    offHeapMemorySize = v;
  }

  public boolean isWebSSL() {
    return webSSL;
  }

  public void setWebSSL(boolean webSSL) {
    this.webSSL = webSSL;
  }

  public boolean isCoordinator() {
    return isCoordinator;
  }

  public void setCoordinator(boolean coordinator) {
    isCoordinator = coordinator;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MemberInformation)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MemberInformation that = (MemberInformation) o;
    return getHeapUsage() == that.getHeapUsage() &&
        getMaxHeapSize() == that.getMaxHeapSize() &&
        getInitHeapSize() == that.getInitHeapSize() &&
        getProcessId() == that.getProcessId() &&
        getLocatorPort() == that.getLocatorPort() &&
        getHttpServicePort() == that.getHttpServicePort() &&
        isServer() == that.isServer() &&
        getClientCount() == that.getClientCount() &&
        Double.compare(that.getCpuUsage(), getCpuUsage()) == 0 &&
        isWebSSL() == that.isWebSSL() &&
        isSecured() == that.isSecured() &&
        isCoordinator() == that.isCoordinator() &&
        Objects.equals(getId(), that.getId()) &&
        Objects.equals(getWorkingDirPath(), that.getWorkingDirPath()) &&
        Objects.equals(getGroups(), that.getGroups()) &&
        Objects.equals(getLogFilePath(), that.getLogFilePath()) &&
        Objects.equals(getStatArchiveFilePath(), that.getStatArchiveFilePath()) &&
        Objects.equals(getServerBindAddress(), that.getServerBindAddress()) &&
        Objects.equals(getLocators(), that.getLocators()) &&
        Objects.equals(getStatus(), that.getStatus()) &&
        Objects.equals(getCacheXmlFilePath(), that.getCacheXmlFilePath()) &&
        Objects.equals(getHost(), that.getHost()) &&
        Objects.equals(getHttpServiceBindAddress(), that.getHttpServiceBindAddress()) &&
        Objects.equals(cacheServerList, that.cacheServerList) &&
        Objects.equals(getHostedRegions(), that.getHostedRegions()) &&
        Objects.equals(getOffHeapMemorySize(), that.getOffHeapMemorySize());
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(super.hashCode(), getId(), getWorkingDirPath(), getGroups(), getLogFilePath(),
            getStatArchiveFilePath(), getServerBindAddress(), getLocators(), getStatus(),
            getHeapUsage(),
            getMaxHeapSize(), getInitHeapSize(), getCacheXmlFilePath(), getHost(), getProcessId(),
            getLocatorPort(), getHttpServicePort(), getHttpServiceBindAddress(), isServer(),
            cacheServerList, getClientCount(), getCpuUsage(), getHostedRegions(),
            getOffHeapMemorySize(),
            isWebSSL(), isSecured(), isCoordinator());
  }
}
