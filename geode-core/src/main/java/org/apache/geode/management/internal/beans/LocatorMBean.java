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

import javax.management.NotificationBroadcasterSupport;

import org.apache.geode.management.LocatorMXBean;

public class LocatorMBean extends NotificationBroadcasterSupport implements LocatorMXBean {

  private final LocatorMBeanBridge bridge;

  public LocatorMBean(LocatorMBeanBridge bridge) {
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
