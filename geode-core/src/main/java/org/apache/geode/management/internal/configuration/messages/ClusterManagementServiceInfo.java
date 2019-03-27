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

package org.apache.geode.management.internal.configuration.messages;

import java.io.Serializable;

public class ClusterManagementServiceInfo implements Serializable {
  private String hostName;
  // -1 indicates the service is not running
  private int httpPort = -1;

  private boolean isSecured;
  private boolean isSSL;

  public boolean isSecured() {
    return isSecured;
  }

  public void setSecured(boolean secured) {
    isSecured = secured;
  }

  public boolean isSSL() {
    return isSSL;
  }

  public void setSSL(boolean SSL) {
    isSSL = SSL;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public boolean isRunning() {
    return httpPort > 0;
  }
}
