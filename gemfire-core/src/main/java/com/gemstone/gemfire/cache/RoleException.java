/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * <code>RoleException</code> is the superclass of those exceptions
 * that can be thrown to indicate a reliability failure on one or more {@link
 * Region regions} that have been configured with required roles using 
 * {@link MembershipAttributes}.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public abstract class RoleException extends CacheRuntimeException {
  private static final long serialVersionUID = -7521056108445887394L;

  /**
   * Creates a new instance of <code>RoleException</code> without
   * detail message.
   */
  public RoleException() {
  }
  
  
  /**
   * Constructs an instance of <code>RoleException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public RoleException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>RoleException</code> with the
   * specified detail message and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public RoleException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>RoleException</code> with the
   * specified cause.
   * @param cause the causal Throwable
   */
  public RoleException(Throwable cause) {
    super(cause);
  }

}

