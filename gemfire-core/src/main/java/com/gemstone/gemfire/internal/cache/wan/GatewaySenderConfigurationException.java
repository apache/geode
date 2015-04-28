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
package com.gemstone.gemfire.internal.cache.wan;

/**
 * An exception indicating that a gateway configuration will not work with the
 * remote side of the gateway's configuration.
 * 
 * @since 7.0
 * @author skumar
 * 
 */
public class GatewaySenderConfigurationException extends GatewaySenderException {
  private static final long serialVersionUID = 1L;

  public GatewaySenderConfigurationException() {
    super();
  }

  public GatewaySenderConfigurationException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public GatewaySenderConfigurationException(String msg) {
    super(msg);
  }

  public GatewaySenderConfigurationException(Throwable cause) {
    super(cause);
  }

}
