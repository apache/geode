/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;


/**
 * An exception indicating that a gateway configuration
 * will not work with the remote side of the gateway's configuration.
 * @since 6.6
 */
public class GatewayConfigurationException extends GatewayException {

  public GatewayConfigurationException() {
    super();
  }

  public GatewayConfigurationException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public GatewayConfigurationException(String msg) {
    super(msg);
  }

  public GatewayConfigurationException(Throwable cause) {
    super(cause);
  }

  

}
