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

package org.apache.geode.internal.cache;

import java.io.Serializable;
import java.util.List;

import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * Class <code>CacheClientStatus</code> provides status about a client from the server's
 * perspective. This class is used by the monitoring tool.
 *
 *
 * @since GemFire 4.3
 */
public class CacheClientStatus implements Serializable {
  private static final long serialVersionUID = -56148046466517217L;

  protected ClientProxyMembershipID _id;
  protected String _memberId;
  protected List _socketPorts;
  protected List _socketAddresses;
  protected int _numberOfConnections;

  public CacheClientStatus(ClientProxyMembershipID id) {
    this._id = id;
  }

  public String getMemberId() {
    return this._memberId;
  }

  public void setMemberId(String memberId) {
    this._memberId = memberId;
  }

  public int getNumberOfConnections() {
    return this._numberOfConnections;
  }

  public void setNumberOfConnections(int numberOfConnections) {
    this._numberOfConnections = numberOfConnections;
  }

  public List getSocketPorts() {
    return this._socketPorts;
  }

  public void setSocketPorts(List socketPorts) {
    this._socketPorts = socketPorts;
  }

  public List getSocketAddresses() {
    return this._socketAddresses;
  }

  public void setSocketAddresses(List socketAddresses) {
    this._socketAddresses = socketAddresses;
  }

  public String getHostAddress() {
    if (_id != null && _id.getDistributedMember() != null) {
      return _id.getDistributedMember().getHost();
    }
    return null;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CacheClientStatus[").append("id=").append(this._id).append("; memberId=")
        .append(this._memberId).append("; numberOfConnections=").append(this._numberOfConnections)
        .append("; socketAddresses=").append(this._socketAddresses).append("; socketPorts=")
        .append(this._socketPorts).append("]");
    return buffer.toString();
  }
}
