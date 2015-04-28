/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.management.internal.cli.util;

/**
 *  
 * @author abhishek
 *
 */
public class ConnectionEndpoint {
  public static final String JMXMANAGER_OPTION_CONTEXT  = "__jmx-manager__";
  public static final String LOCATOR_OPTION_CONTEXT = "__locator__";
  
  private final String host;
  private final int port;
  
  /**
   * @param host
   * @param port
   */
  public ConnectionEndpoint(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * @return the host
   */
  public String getHost() {
    return host;
  }

  /**
   * @return the port
   */
  public int getPort() {
    return port;
  }
  
  public String toString(boolean includeClassName) {
    StringBuilder builder = new StringBuilder();
    
    if (includeClassName) {
      builder.append(ConnectionEndpoint.class.getSimpleName());
    }
    builder.append("[host=").append(host).
            append(", port=").append(port).append("]");
    
    return builder.toString();
  }

  @Override
  public String toString() {
    return toString(true);
  }
}
