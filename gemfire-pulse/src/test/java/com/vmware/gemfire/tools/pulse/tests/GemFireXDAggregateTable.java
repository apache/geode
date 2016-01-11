/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

import javax.management.NotificationBroadcasterSupport;

public class GemFireXDAggregateTable extends NotificationBroadcasterSupport
    implements GemFireXDAggregateTableMBean {
  private String name = null;

  public GemFireXDAggregateTable(String name) {
    this.name = name;
  }

  private String getKey(String propName) {
    return "table." + name + "." + propName;
  }

  @Override
  public long getEntrySize() {
    return Long.parseLong(JMXProperties.getInstance().getProperty(
        getKey("EntrySize")));
  }

  @Override
  public int getNumberOfRows() {
    return Integer.parseInt(JMXProperties.getInstance().getProperty(
        getKey("NumberOfRows")));
  }
}
