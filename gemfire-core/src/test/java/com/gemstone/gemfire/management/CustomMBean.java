/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import javax.management.AttributeChangeNotification;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
/**
 * A simple MBean to test various aspects of federation
 * @author rishim
 *
 */
public class CustomMBean extends NotificationBroadcasterSupport implements
    CustomMXBean {

  private long systemTime;
  private String name;

  public CustomMBean(String name) {
    this.name = name;
  }

  @Override
  public long getSystemTime() {
    return System.currentTimeMillis();
  }

  @Override
  public void setSystemTime(long systemTIme) {
    this.systemTime = systemTime;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String fetchName() {
    // TODO Auto-generated method stub
    return name;
  }

  @Override
  public void writeName(String name) {
    this.name = name;

    Notification n = new AttributeChangeNotification(this, sequenceNumber++,
        System.currentTimeMillis(), "staticField changed", "staticField",
        "int", name, this.name);

    sendNotification(n);
  }

  private long sequenceNumber = 1;

}