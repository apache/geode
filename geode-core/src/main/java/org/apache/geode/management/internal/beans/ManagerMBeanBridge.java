/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.beans;


import javax.management.JMException;

import org.apache.geode.management.internal.SystemManagementService;

/**
 * Bridge for ManagerMBean
 *
 */
public class ManagerMBeanBridge {

  private final SystemManagementService service;

  private String pulseURL;

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
    statusMessage = message;
  }

}
