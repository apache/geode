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

package org.apache.geode.management.runtime;

import java.util.Arrays;
import java.util.Objects;

import org.apache.geode.annotations.Experimental;

@Experimental
public class GatewayReceiverInfo extends RuntimeInfo {
  private boolean running;
  private int port;
  private String hostnameForSenders;
  private String bindAddress;
  private int senderCount;
  private String[] connectedSenders = new String[0];

  public boolean isRunning() {
    return running;
  }

  public void setRunning(boolean running) {
    this.running = running;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getHostnameForSenders() {
    return hostnameForSenders;
  }

  public void setHostnameForSenders(String hostnameForSenders) {
    this.hostnameForSenders = hostnameForSenders;
  }

  public String getBindAddress() {
    return bindAddress;
  }

  public void setBindAddress(String bindAddress) {
    this.bindAddress = bindAddress;
  }

  public int getSenderCount() {
    return senderCount;
  }

  public void setSenderCount(int senderCount) {
    this.senderCount = senderCount;
  }

  public String[] getConnectedSenders() {
    return connectedSenders;
  }

  public void setConnectedSenders(String[] connectedSenders) {
    this.connectedSenders = connectedSenders;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GatewayReceiverInfo)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    GatewayReceiverInfo that = (GatewayReceiverInfo) o;
    return isRunning() == that.isRunning() &&
        getPort() == that.getPort() &&
        getSenderCount() == that.getSenderCount() &&
        Objects.equals(getHostnameForSenders(), that.getHostnameForSenders()) &&
        Objects.equals(getBindAddress(), that.getBindAddress()) &&
        Arrays.equals(getConnectedSenders(), that.getConnectedSenders());
  }

  @Override
  public int hashCode() {
    int result =
        Objects
            .hash(super.hashCode(), isRunning(), getPort(), getHostnameForSenders(),
                getBindAddress(),
                getSenderCount());
    result = 31 * result + Arrays.hashCode(getConnectedSenders());
    return result;
  }
}
