/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import com.gemstone.gemfire.management.internal.cli.util.ConnectionEndpoint;

/**
 * 
 * @author Sourabh Bansod
 * 
 * @since 7.0
 */
public class ConnectToLocatorResult {
  private ConnectionEndpoint memberEndpoint = null;
  private String resultMessage;
  final private boolean isJmxManagerSslEnabled;
  
  public ConnectToLocatorResult (ConnectionEndpoint memberEndpoint, String resultMessage, boolean isJmxManagerSslEnabled) {
    this.memberEndpoint = memberEndpoint;
    this.resultMessage = resultMessage;
    this.isJmxManagerSslEnabled = isJmxManagerSslEnabled;
  }
  
  public ConnectionEndpoint getMemberEndpoint() {
    return this.memberEndpoint;
  }
  
  public String getResultMessage() {
    return this.resultMessage;
  }
  
  public boolean isJmxManagerSslEnabled() {
    return isJmxManagerSslEnabled;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Member Endpoint :" + memberEndpoint.toString());
    sb.append("\nResult Message : " + resultMessage);
    return sb.toString();
  }
}
