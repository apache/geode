/*
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
 */
package com.gemstone.gemfire.management.internal.beans;

import javax.management.JMException;

import com.gemstone.gemfire.management.ManagerMXBean;

/**
 * 
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
