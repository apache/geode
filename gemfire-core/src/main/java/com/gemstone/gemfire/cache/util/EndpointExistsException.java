/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.util;


/**
 * An <code>EndpointExistsException</code> indicates a client
 * <code>Endpoint</code> already exists.
 *
 * @author Barry Oglesby
 *
 * @since 5.0.2
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */
@Deprecated
public class EndpointExistsException extends EndpointException {
private static final long serialVersionUID = 950617116786308012L;

  public EndpointExistsException(String msg) {
    super(msg);
  }
  
}
