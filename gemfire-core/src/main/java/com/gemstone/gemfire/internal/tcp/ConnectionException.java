/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.GemFireException;

/**
    @author Bruce Schuchardt
    @since 3.0
   
 */
public class ConnectionException extends GemFireException
{
  private static final long serialVersionUID = -1977443644277412122L;

  public ConnectionException(String message) {
     super(message);
  }

  public ConnectionException(String message, Throwable cause) {
     super(message, cause);
  }
  
}
