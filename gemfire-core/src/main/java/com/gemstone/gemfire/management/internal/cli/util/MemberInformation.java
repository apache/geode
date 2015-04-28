/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.util;

import java.io.Serializable;


/***
 * Data class to hold the information of the member
 * Used in describe member command
 * @author bansods
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
	private String cpuUsage;
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
	public String getCpuUsage() {
		return cpuUsage;
	}
	public void setCpuUsage(String cpuUsage) {
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
}
