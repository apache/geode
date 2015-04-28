/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.util;


/**
 * An <code>EndpointInUseException</code> indicates a client <code>Endpoint</code>
 * is in use (meaning that it contains one or more <code>Connection</code>s.
 *
 * @author Barry Oglesby
 *
 * @since 5.0.2
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */
@Deprecated
public class EndpointInUseException extends EndpointException {
private static final long serialVersionUID = -4087729485272321469L;

  public EndpointInUseException(String msg) {
    super(msg);
  }
}
