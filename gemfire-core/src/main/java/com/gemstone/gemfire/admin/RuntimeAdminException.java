/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
   
package com.gemstone.gemfire.admin;

/**
 * A <code>RuntimeAdminException</code> is thrown when a runtime errors occurs
 * during administration or monitoring of GemFire. 
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class RuntimeAdminException 
extends com.gemstone.gemfire.GemFireException {

  private static final long serialVersionUID = -7512771113818634005L;

  public RuntimeAdminException() {
    super();
  }

  public RuntimeAdminException(String message) {
    super(message);
  }

  public RuntimeAdminException(String message, Throwable cause) {
    super(message, cause);
  }

  public RuntimeAdminException(Throwable cause) {
    super(cause);
  }
    
}
