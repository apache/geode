/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * Class <code>IncomingGatewayStatus</code> provides status about an incoming
 * <code>Gateway</code> client from the server's perspective. This class is
 * used by the monitoring tool.
 *
 * @author Barry Oglesby
 *
 * @since 4.3
 */
public class IncomingGatewayStatus implements Serializable {
  private static final long serialVersionUID = -4579815367602658353L;

  protected String _memberId;
  protected int _socketPort;
  protected InetAddress _socketAddress;

  public IncomingGatewayStatus(String memberId, InetAddress socketAddress, int socketPort) {
    this._memberId = memberId;
    this._socketAddress = socketAddress;
    this._socketPort = socketPort;
  }

  public String getMemberId() {
    return this._memberId;
  }

  protected void setMemberId(String memberId) {
    this._memberId = memberId;
  }

  public int getSocketPort() {
    return this._socketPort;
  }

  protected void setSocketPort(int socketPort) {
    this._socketPort = socketPort;
  }

  public InetAddress getSocketAddress() {
    return this._socketAddress;
  }

  protected void setSocketAddress(InetAddress socketAddress) {
    this._socketAddress = socketAddress;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer
      .append("IncomingGatewayStatus[")
      .append("memberId=")
      .append(this._memberId)
      .append("; socketAddress=")
      .append(this._socketAddress)
      .append("; socketPort=")
      .append(this._socketPort)
      .append("]");
    return buffer.toString();
  }
}
