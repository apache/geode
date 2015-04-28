/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.util;

/**
 * An <code>EndpointDoesNotExistException</code> indicates a client
 * <code>Endpoint</code> does not exist for the input name, host and
 * port.
 *
 * @author Barry Oglesby
 *
 * @since 5.0.2
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */
@Deprecated
public class EndpointDoesNotExistException extends EndpointException {
private static final long serialVersionUID = 1654241470788247283L;

  /**
   * Constructs a new <code>EndpointDoesNotExistException</code>.
   * 
   * @param name The name of the requested <code>Endpoint</code>
   * @param host The host of the requested <code>Endpoint</code>
   * @param port The port of the requested <code>Endpoint</code>
   */
  public EndpointDoesNotExistException(String name, String host, int port) {
    super(name+"->"+host+":"+port);
  }
}
