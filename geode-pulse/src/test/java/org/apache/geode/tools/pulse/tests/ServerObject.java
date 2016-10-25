/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests;

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
  public double getDiskWritesRate() {
    return Double.parseDouble(JMXProperties.getInstance().getProperty(
        getKey("diskWritesRate")));
  }

  @Override
  public double getAverageWrites() {
    String val = JMXProperties.getInstance().getProperty(getKey("averageWrites"), "");
    double ret = Double.parseDouble(val);
    return ret;
//    return Double.parseDouble(JMXProperties.getInstance().getProperty(
//        getKey("averageWrites"), ""));
  }

  @Override
  public double getAverageReads() {
    return Double.parseDouble(JMXProperties.getInstance().getProperty(
        getKey("averageReads"), ""));
  }

  @Override
  public double getQueryRequestRate() {
    return Double.parseDouble(JMXProperties.getInstance().getProperty(
        getKey("queryRequestRate"), ""));
  }

  @Override
  public double getDiskReadsRate() {
    return Double.parseDouble(JMXProperties.getInstance().getProperty(
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
