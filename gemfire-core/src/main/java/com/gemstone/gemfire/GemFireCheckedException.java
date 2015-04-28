/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
   
package com.gemstone.gemfire;

/**
 * This is the abstract superclass of exceptions that are thrown and
 * declared.
 * <p>
 * This class ought to be called <em>GemFireException</em>, but that name
 * is reserved for an older class that extends {@link java.lang.RuntimeException}.
 * 
 * @see com.gemstone.gemfire.GemFireException
 * @since 5.1
 */
public abstract class GemFireCheckedException extends Exception {

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>GemFireException</code> with no detailed message.
   */
  public GemFireCheckedException() {
    super();
  }

  /**
   * Creates a new <code>GemFireCheckedException</code> with the given detail
   * message.
   */
  public GemFireCheckedException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>GemFireException</code> with the given detail
   * message and cause.
   */
  public GemFireCheckedException(String message, Throwable cause) {
    super(message);
    this.initCause(cause);
  }
  
  /**
   * Creates a new <code>GemFireCheckedException</code> with the given cause and
   * no detail message
   */
  public GemFireCheckedException(Throwable cause) {
    super();
    this.initCause(cause);
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the root cause of this <code>GemFireCheckedException</code> or
   * <code>null</code> if the cause is nonexistent or unknown.
   */
  public Throwable getRootCause() {
      if ( this.getCause() == null ) return null;
      Throwable root = this.getCause();
      while ( root != null ) {
//          if ( ! ( root instanceof GemFireCheckedException )) {
//              break;
//          }
//          GemFireCheckedException tmp = (GemFireCheckedException) root;
          if ( root.getCause() == null ) {
              break;
          } else {
              root = root.getCause();
          }
      }
      return root;
  }
  
}
