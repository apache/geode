/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

import java.io.IOException;

import javax.management.NotificationBroadcasterSupport;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

public class ServerObject extends NotificationBroadcasterSupport implements
    ServerObjectMBean {
  private String name = null;

  private TabularDataSupport wanInfo;
  private static String[] itemNames = { "key", "value" };
  private static String[] itemDescriptions = { "Key", "Value" };
  private static OpenType[] itemTypes = { SimpleType.STRING, SimpleType.BOOLEAN };
  private static CompositeType wanInfoType = null;

  private static String[] indexNames = { "key" };
  private static TabularType wanType = null;

  static {
    try {
      wanInfoType = new CompositeType("wanInfo", "WAN Information", itemNames,
          itemDescriptions, itemTypes);

      wanType = new TabularType("wanInfo", "WAN Information", wanInfoType,
          indexNames);

    } catch (OpenDataException e) {
      e.printStackTrace();
    }
  }

  public ServerObject(String name) {
    this.name = name;
    this.wanInfo = new TabularDataSupport(wanType);
  }

  private String getKey(String propName) {
    return "server." + name + "." + propName;
  }

  @Override
  public String[] listCacheServers() {
    return JMXProperties.getInstance()
        .getProperty(getKey("listCacheServers"), "").split(" ");
  }

  @Override
  public String[] listServers() {
    return JMXProperties.getInstance()
            .getProperty(getKey("listServers"), "").split(" ");
  }

  @Override
  public TabularData viewRemoteClusterStatus() {
    wanInfo.clear();
    String[] wan = JMXProperties.getInstance()
        .getProperty(getKey("wanInfo"), "").split(" ");
    int cnt = 0;
    while (wan.length >= (cnt + 2)) {
      try {
        wanInfo.put(buildWanInfoType(new String(wan[cnt]),
            Boolean.parseBoolean(wan[cnt + 1])));
      } catch (OpenDataException e) {
        e.printStackTrace();
      }
      cnt += 2;
    }

    return (TabularData) wanInfo.clone();
  }

  @Override
  public int getMemberCount() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("memberCount")));
  }

  @Override
  public int getNumClients() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("numClients")));
  }

  @Override
  public int getDistributedSystemId() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("distributedSystemId")));
  }

  @Override
  public int getLocatorCount() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("locatorCount")));
  }

  @Override
  public int getTotalRegionCount() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("totalRegionCount")));
  }

  @Override
  public int getNumRunningFunctions() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("numRunningFunctions")));
  }

  @Override
  public long getRegisteredCQCount() {
    return Long.parseLong(JMXProperties.getInstance().getProperty(
        getKey("registeredCQCount")));
  }

  @Override
  public int getNumSubscriptions() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("numSubscriptions")));
  }

  // For SQLFire/GemFireXD
  @Override
  public int getTransactionCommitted() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("TransactionCommitted")));
  }

  // For SQLFire/GemFireXD
  @Override
  public int getTransactionRolledBack() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("TransactionRolledBack")));
  }

  @Override
  public long getTotalHeapSize() {
    return Long.parseLong(JMXProperties.getInstance().getProperty(
        getKey("totalHeapSize")));
  }

  @Override
  public long getUsedHeapSize() {
    return Long.parseLong(JMXProperties.getInstance().getProperty(
        getKey("usedHeapSize")));
  }

  @Override
  public long getMaxMemory() {
    return Long.parseLong(JMXProperties.getInstance().getProperty(
        getKey("MaxMemory")));
  }

  @Override
  public long getUsedMemory() {
    return Long.parseLong(JMXProperties.getInstance().getProperty(
        getKey("UsedMemory")));
  }

  @Override
  public long getTotalRegionEntryCount() {
    return Long.parseLong(JMXProperties.getInstance().getProperty(
        getKey("totalRegionEntryCount")));
  }

  @Override
  public int getCurrentQueryCount() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("currentQueryCount")));
  }

  @Override
  public long getTotalDiskUsage() {
    return Long.parseLong(JMXProperties.getInstance().getProperty(
        getKey("totalDiskUsage")));
  }

  @Override
  public float getDiskWritesRate() {
    return Float.parseFloat(JMXProperties.getInstance().getProperty(
        getKey("diskWritesRate")));
  }

  @Override
  public float getAverageWrites() {
    return Float.parseFloat(JMXProperties.getInstance().getProperty(
        getKey("averageWrites"), ""));
  }

  @Override
  public float getAverageReads() {
    return Float.parseFloat(JMXProperties.getInstance().getProperty(
        getKey("averageReads"), ""));
  }

  @Override
  public float getQueryRequestRate() {
    return Float.parseFloat(JMXProperties.getInstance().getProperty(
        getKey("queryRequestRate"), ""));
  }

  @Override
  public float getDiskReadsRate() {
    return Float.parseFloat(JMXProperties.getInstance().getProperty(
        getKey("diskReadsRate"), ""));
  }

  @Override
  public long getJVMPauses() {
    return Long.parseLong(JMXProperties.getInstance().getProperty(
        getKey("jvmPauses"), ""));
  }

  private CompositeData buildWanInfoType(String key, Boolean state)
      throws OpenDataException {
    Object[] itemValues = { key, state };
    CompositeData result = new CompositeDataSupport(wanInfoType, itemNames,
        itemValues);

    return result;
  }

  @Override
  public String queryData(String p0, String p1, int p2) {
    // p0 : query
    // p1 : comma separated members
    // p2 : limit
    
    DataBrowserResultLoader dbrLoader = DataBrowserResultLoader.getInstance();
    
    try {
      return dbrLoader.load(p0);
    } catch (IOException e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }
}
