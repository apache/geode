/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
   
package org.apache.geode;

/**
 * This is the abstract superclass of exceptions that are thrown to
 * indicate incorrect usage of GemFire.
 *
 * Since these exceptions are unchecked, this class really
 * <em>ought</em> to be called <code>GemFireRuntimeException</code>;
 * however, the current name is retained for compatibility's sake.
 * 
 * @see org.apache.geode.GemFireCheckedException
 * @see org.apache.geode.cache.CacheRuntimeException
 */
// Implementation note: This class is abstract so that we are forced
// to have more specific exception types.  We want to avoid using
// GemFireException to describe an arbitrary error condition (think
// GsError).
public abstract class GemFireException extends RuntimeException {
  public static final long serialVersionUID = -6972360779789402295L;

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
