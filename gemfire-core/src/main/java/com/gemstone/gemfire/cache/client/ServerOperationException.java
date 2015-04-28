/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

/**
 * An exception indicating that a failure has happened on the server
 * while processing an operation that was sent to it by a client.
 * @author darrel
 * @since 5.7
 */
public class ServerOperationException extends ServerConnectivityException {
private static final long serialVersionUID = -3106323103325266219L;

  /**
   * Create a new instance of ServerOperationException without a detail message or cause.
   */
  public ServerOperationException() {
  }

  /**
   * 
   * Create a new instance of ServerOperationException with a detail message
   * @param message the detail message
   */
  public ServerOperationException(String message) {
    super(getServerMessage(message));
  }

  /**
   * Create a new instance of ServerOperationException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public ServerOperationException(String message, Throwable cause) {
    super(getServerMessage(message), cause);
  }

  /**
   * Create a new instance of ServerOperationException with a cause
   * @param cause the cause
   */
  public ServerOperationException(Throwable cause) {
    super(getServerMessage(cause), cause);
  }

  private static String getServerMessage(Throwable cause) {
    return getServerMessage(cause != null ? cause.toString() : null);
  }

  private static String getServerMessage(String msg) {
    // To fix bug 44679 add a description of the member the server is on.
    // Do this without changing how this class gets serialized so that old
    // clients will still work.
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      if (msg != null) {
        return "remote server on " + ids.getMemberId() + ": " + msg;
      } else {
        return "remote server on " + ids.getMemberId();
      }
    } else {
      if (msg != null) {
        return "remote server on unknown location: " + msg;
      } else {
        return "remote server on unknown location";
      }
    }
  }

}
