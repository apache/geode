/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
   
package com.gemstone.gemfire;

/**
 * This is the abstract superclass of exceptions that are thrown to
 * indicate incorrect usage of GemFire.
 *
 * Since these exceptions are unchecked, this class really
 * <em>ought</em> to be called <code>GemFireRuntimeException</code>;
 * however, the current name is retained for compatibility's sake.
 * 
 * @author David Whitlock
 * @see com.gemstone.gemfire.GemFireCheckedException
 * @see com.gemstone.gemfire.cache.CacheRuntimeException
 */
// Implementation note: This class is abstract so that we are forced
// to have more specific exception types.  We want to avoid using
// GemFireException to describe an arbitrary error condition (think
// GsError).
public abstract class GemFireException extends RuntimeException {

  /** The cause of this <code>GemFireException</code> */
//  private Throwable cause;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>GemFireException</code> with no detailed message.
   */
  public GemFireException() {
    super();
  }

  /**
   * Creates a new <code>GemFireException</code> with the given detail
   * message.
   */
  public GemFireException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>GemFireException</code> with the given detail
   * message and cause.
   */
  public GemFireException(String message, Throwable cause) {
    super(message, cause);
//    this.cause = cause;
  }
  
  /**
   * Creates a new <code>GemFireException</code> with the given cause and
   * no detail message
   */
  public GemFireException(Throwable cause) {
    super(cause);
//    this.cause = cause;
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the cause of this <code>GemFireException</code> or
   * <code>null</code> if the cause is nonexistent or unknown.
   */
//  public Throwable getCause() {
//    return this.cause;
//  }

  /**
   * Returns the root cause of this <code>GemFireException</code> or
   * <code>null</code> if the cause is nonexistent or unknown.
   */
  public Throwable getRootCause() {
    if ( this.getCause() == null ) {
      return null;
    }
    Throwable root = this.getCause();
    while ( root.getCause() != null ) {
      root = root.getCause();
    }
    return root;
  }
  
//  public void printStackTrace() {
//    super.printStackTrace();
//    if (this.cause != null) {
//      System.err.println("Caused by:");
//      this.cause.printStackTrace();
//    }
//  }
  
//  public void printStackTrace(java.io.PrintWriter pw) {
//    super.printStackTrace(pw);
//
//    if (this.cause != null) {
//      pw.println("Caused by:");
//      this.cause.printStackTrace(pw);
//    }
//  }
//  
//  public String getMessage() {
//    if (this.cause != null) {
//      String ourMsg = super.getMessage();
//      if (ourMsg == null || ourMsg.length() == 0) {
//        //ourMsg = super.toString(); //causes inifinite recursion
//        ourMsg = "";
//      }
//      StringBuffer sb = new StringBuffer(ourMsg);
//      sb.append(" Caused by: ");
//      String causeMsg = this.cause.getMessage();
//      if (causeMsg == null || causeMsg.length() == 0) {
//        causeMsg = this.cause.toString();
//      }
//      sb.append(causeMsg);
//      return sb.toString();
//    } else {
//      return super.getMessage();
//    }
//  }

  /**
   * Represent the receiver as well as the cause
   */
//  public String toString() {
//    String result = super.toString();
//    if (cause != null) {
//      result = result + ", caused by " + cause.toString();
//    }
//    return result;
//  }

}
