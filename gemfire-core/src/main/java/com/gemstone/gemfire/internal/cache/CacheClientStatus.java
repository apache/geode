/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import java.io.Serializable;
import java.util.List;

/**
 * Class <code>CacheClientStatus</code> provides status about a client
 * from the server's perspective. This class is used by the monitoring tool.
 *
 * @author Barry Oglesby
 *
 * @since 4.3
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
    if(_id != null && _id.getDistributedMember() != null)
     return _id.getDistributedMember().getHost();
    return null;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer
      .append("CacheClientStatus[")
      .append("id=")
      .append(this._id)
      .append("; memberId=")
      .append(this._memberId)
      .append("; numberOfConnections=")
      .append(this._numberOfConnections)
      .append("; socketAddresses=")
      .append(this._socketAddresses)
      .append("; socketPorts=")
      .append(this._socketPorts)
      .append("]");
    return buffer.toString();
  }
}
