/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

import javax.management.NotificationBroadcasterSupport;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

public class GemFireXDCluster extends NotificationBroadcasterSupport implements
    GemFireXDClusterMBean {
  private String name = null;

  private static String[] itemNames = { "connectionsAttempted",
      "connectionsActive", "connectionsClosed", "connectionsFailed" };;
  private static String[] itemDescriptions = { "connectionsAttempted",
      "connectionsActive", "connectionsClosed", "connectionsFailed" };
  private static OpenType[] itemTypes = { SimpleType.LONG, SimpleType.LONG,
      SimpleType.LONG, SimpleType.LONG };
  private static CompositeType networkServerClientConnectionStats = null;

  static {
    try {
      networkServerClientConnectionStats = new CompositeType(
          "NetworkServerClientConnectionStats",
          "Network Server Client Connection Stats Information", itemNames,
          itemDescriptions, itemTypes);

    } catch (OpenDataException e) {
      e.printStackTrace();
    }
  }

  public GemFireXDCluster(String name) {
    this.name = name;
  }

  private String getKey(String propName) {
    return "gemfirexd." + name + "." + propName;
  }

  @Override
  public int getProcedureCallsCompleted() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("ProcedureCallsCompleted")));
  }

  @Override
  public int getProcedureCallsInProgress() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("ProcedureCallsInProgress")));
  }

  @Override
  public CompositeData getNetworkServerClientConnectionStats() {
    String value = JMXProperties.getInstance().getProperty(
        getKey("NetworkServerClientConnectionStats"), "");
    String[] values = value.split(",");
    Long[] itemValues = new Long[values.length];
    for (int i = 0; i < values.length; i++) {
      itemValues[i] = Long.parseLong(values[i]);
    }

    CompositeData nscCompData;
    try {
      nscCompData = new CompositeDataSupport(
          networkServerClientConnectionStats, itemNames, itemValues);
    } catch (OpenDataException e) {
      e.printStackTrace();
      nscCompData = null;
    }
    return nscCompData;

  }

}