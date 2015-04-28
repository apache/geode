/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management;

/**
 * Indicates that a request to start a management service
 * failed because it was already running.
 * 
 * @author darrel
 * @since 7.0
 * 
 */
public class AlreadyRunningException extends ManagementException {

  private static final long serialVersionUID = 8947734854770335071L;

  public AlreadyRunningException() {
  }

  public AlreadyRunningException(String message) {
    super(message);
  }

  public AlreadyRunningException(String message, Throwable cause) {
    super(message, cause);
  }

  public AlreadyRunningException(Throwable cause) {
    super(cause);
  }

}
