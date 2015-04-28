/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;

public class CacheServerInfo implements Serializable {

  private static final long serialVersionUID = 1L;
  
  private String bindAddress;
  private int port;
  private boolean isRunning;
  
  public CacheServerInfo(String bindAddress, int port, boolean isRunning) {
    this.setBindAddress(bindAddress);
    this.setPort(port);
    this.setRunning(isRunning);
  }

  public String getBindAddress() {
    return bindAddress;
  }

  public void setBindAddress(String bindAddress) {
    this.bindAddress = bindAddress;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public boolean isRunning() {
    return isRunning;
  }

  public void setRunning(boolean isRunning) {
    this.isRunning = isRunning;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Bind Address :"); 
    sb.append(this.bindAddress);
    sb.append('\n');
    sb.append("Port :"); 
    sb.append(this.port);
    sb.append('\n');
    sb.append("Running :"); 
    sb.append(this.isRunning);
    return sb.toString();
  }

}
