/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached.commands;

/**
 * thrown when there is some sort of error in input line
 * when this exception is thrown "CLIENT_ERROR <error>\r\n"
 * is sent to the client 
 * 
 * @author Swapnil Bawaskar
 */
public class ClientError extends RuntimeException {
  private static final long serialVersionUID = -2426928000696680541L;

  public ClientError() {
  }

  public ClientError(String message) {
    super(message);
  }

  public ClientError(String message, Throwable cause) {
    super(message, cause);
  }
}
