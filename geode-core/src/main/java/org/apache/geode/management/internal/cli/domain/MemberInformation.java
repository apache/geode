/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;



/***
 * Data class to hold the information of the member
 * Used in describe member command
 *
 */
public class MemberInformation implements Serializable{
	/**
	 * 
	 */
	private static final long	serialVersionUID	= 1L;
	private String name;
	private String id;
	private String workingDirPath;
	private String groups;
	private String logFilePath;
	private String statArchiveFilePath;
	private String serverBindAddress;
	private String locators;
	private String heapUsage;
	private String maxHeapSize;
	private String initHeapSize;
	private String cacheXmlFilePath;
	private String host;
	private String processId;
	private String locatorBindAddress;
	private int locatorPort;
	private boolean isServer;
	private List<CacheServerInfo> cacheServerList;
	private int clientCount;
	private double cpuUsage;
	private Set<String> hostedRegions;
	private String offHeapMemorySize;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
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
	public String getHeapUsage() {
		return heapUsage;
	}
	public void setHeapUsage(String heapUsage) {
		this.heapUsage = heapUsage;
	}
	public String getMaxHeapSize() {
		return maxHeapSize;
	}
	public void setMaxHeapSize(String maxHeapSize) {
		this.maxHeapSize = maxHeapSize;
	}
	
	public String getCacheXmlFilePath() {
		return cacheXmlFilePath;
	}
	
	public void setCacheXmlFilePath(String cacheXmlFilePath) {
		this.cacheXmlFilePath = cacheXmlFilePath;
	}
	
	public String getInitHeapSize() {
		return initHeapSize;
	}
	public void setInitHeapSize(String initHeapSize) {
		this.initHeapSize = initHeapSize;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getProcessId() {
		return processId;
	}
	public void setProcessId(String processId) {
		this.processId = processId;
	}
	public String getLocatorBindAddress() {
		return locatorBindAddress;
	}
	public void setLocatorBindAddress(String locatorBindAddress) {
		this.locatorBindAddress = locatorBindAddress;
	}
	public int getLocatorPort() {
		return locatorPort;
	}
	public void setLocatorPort(int locatorPort) {
		this.locatorPort = locatorPort;
	}
  public boolean isServer() {
    return isServer;
  }
  public void setServer(boolean isServer) {
    this.isServer = isServer;
  }
  
  public List<CacheServerInfo> getCacheServeInfo() {
    return cacheServerList;
  }
  
  public void addCacheServerInfo(CacheServerInfo cacheServerInfo) {
    if (cacheServerInfo == null)
       return;
    if (cacheServerList == null) {
      cacheServerList = new ArrayList<CacheServerInfo>();
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
    return this.offHeapMemorySize;
  }
  public void setOffHeapMemorySize(String v) {
    this.offHeapMemorySize = v;
  }
}
