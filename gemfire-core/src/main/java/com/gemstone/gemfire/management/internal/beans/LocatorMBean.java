/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import javax.management.NotificationBroadcasterSupport;

import com.gemstone.gemfire.management.LocatorMXBean;

/**
 * 
 * @author rishim
 * 
 */
public class LocatorMBean extends NotificationBroadcasterSupport implements
    LocatorMXBean {
  
  private LocatorMBeanBridge bridge;
  
  public LocatorMBean(LocatorMBeanBridge bridge){
    this.bridge = bridge;
  }

  @Override
  public String getBindAddress() {
    return bridge.getBindAddress();
  }

  @Override
  public String getHostnameForClients() {
    return bridge.getHostnameForClients();
  }

  @Override
  public String viewLog() {
    return bridge.viewLog();
  }

  @Override
  public int getPort() {
    return bridge.getPort();
  }

  @Override
  public boolean isPeerLocator() {
    return bridge.isPeerLocator();
  }

  @Override
  public boolean isServerLocator() {
    return bridge.isServerLocator();
  }

  @Override
  public String[] listManagers() {
    return bridge.listManagers();
  }

  @Override
  public String[] listPotentialManagers() {
    return bridge.listPotentialManagers();
  }

 

}
