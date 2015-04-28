/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import javax.management.JMException;

import com.gemstone.gemfire.management.ManagerMXBean;

/**
 * 
 * @author rishim
 *
 */
public class ManagerMBean implements ManagerMXBean {
  
  private ManagerMBeanBridge bridge;
  
  public ManagerMBean(ManagerMBeanBridge bridge){
    this.bridge = bridge;
  }

  @Override
  public boolean isRunning() {
    return bridge.isRunning();
  }

  @Override
  public boolean start() throws JMException{
     return bridge.start();
  }

  @Override
  public boolean stop() throws JMException {
    return bridge.stop();
  }

  @Override
  public String getPulseURL() {
    return bridge.getPulseURL();
  }

  @Override
  public void setPulseURL(String pulseURL) {
    bridge.setPulseURL(pulseURL);
  }

  @Override
  public String getStatusMessage() {
    return bridge.getStatusMessage();
  }

  @Override
  public void setStatusMessage(String message) {
    bridge.setStatusMessage(message);
  }

}
