/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * Indicates that a Clear Operation happened while an entry operation
 * was in progress, which would result in the ongoing entry operation to abort
 * @author Asif Shahid
 * @since 5.1
 */
public class RegionClearedException extends Exception  {
private static final long serialVersionUID = 1266503771775907997L;
  /**
   * Constructs a new <code>RegionClearedException</code>.
   */
  public RegionClearedException() {
    super();
  }
  
  /**
   * Constructs a new <code>RegionClearedException</code> with a message string.
   *
   * @param msg a message string
   */
  public RegionClearedException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs a new <code>RegionClearedException</code> with a message string
   * and a cause.
   *
   * @param msg the message string
   * @param cause a causal Throwable
   */
  public RegionClearedException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs a new <code>RegionClearedException</code> with a cause.
   *
   * @param cause a causal Throwable
   */
  public RegionClearedException(Throwable cause) {
    super(cause);
  }
}
