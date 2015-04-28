/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * Thrown when a problem is encountered while working with
 * admin-related XML data.
 *
 * @see DistributedSystemConfig#getEntityConfigXMLFile
 *
 * @author David Whitlock
 * @since 4.0
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class AdminXmlException extends RuntimeAdminException {
  private static final long serialVersionUID = -6848726449157550169L;

  /**
   * Creates a new <code>AdminXmlException</code> with the given
   * descriptive message.
   */
  public AdminXmlException(String s) {
    super(s);
  }

  /**
   * Creates a new <code>AdminXmlException</code> with the given
   * descriptive message and cause.
   */
  public AdminXmlException(String s, Throwable cause) {
    super(s, cause);
  }

}
