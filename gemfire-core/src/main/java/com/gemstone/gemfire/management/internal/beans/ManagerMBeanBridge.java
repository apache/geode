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

import com.gemstone.gemfire.management.internal.SystemManagementService;

/**
 * Bridge for ManagerMBean
 * @author rishim
 *
 */
public class ManagerMBeanBridge {

  private SystemManagementService service;
  
  private String pulseURL ;

  private String statusMessage;

  public ManagerMBeanBridge(SystemManagementService service) {
    this.service = service;
  }

  public boolean isRunning() {
    return service.isManager();
  }

  public boolean start() throws JMException {
    try {
      service.startManager();
    } catch (Exception e) {
      throw new JMException(e.getMessage());
    }
    return true;
  }

  public boolean stop() throws JMException {
    try {
      service.stopManager();
    } catch (Exception e) {
      throw new JMException(e.getMessage());
    }
    return true;
  }
  
  public String getPulseURL() {
    return pulseURL;
  }

  public void setPulseURL(String pulseURL) {
    this.pulseURL = pulseURL;

  }

  public String getStatusMessage() {
    return statusMessage;
  }

  public void setStatusMessage(String message) {
    this.statusMessage = message;
  }

}
