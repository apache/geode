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
package com.gemstone.gemfire.distributed.internal;

/**
 * thrown by {@linkplain ReliableReplyProcessor21} when a message has not been delivered
 * to at least one member in the original recipient list.
 * @author sbawaska
 */
public class ReliableReplyException extends ReplyException {
  private static final long serialVersionUID = 472566058783450438L;

  public ReliableReplyException(String message) {
    super(message);
  }
  
  public ReliableReplyException(String message, Throwable cause) {
    super(message, cause);
  }
}
