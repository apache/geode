/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

/**
 * This error is used by GemFire for internal purposes.
 * It does not indicate an error condition.
 * For this reason it is named "Rethrowable" instead of the standard "Error".
 * It was made an <code>Error</code> to make it easier for user code that typically would
 * catch <code>Exception</code> to not accidently catch this exception.
 * <p> Note: if user code catches this error (or its subclasses) then it <em>must</em>
 * be rethrown.
 * 
 * @author darrel
 * @since 5.7
 */
public class GemFireRethrowable extends Error {
  private static final long serialVersionUID = 8349791552668922571L;

  /**
   * Create a GemFireRethrowable.
   */
  public GemFireRethrowable() {
  }

  /**
   * Create a GemFireRethrowable with the specified message.
   * @param message
   */
  public GemFireRethrowable(String message) {
    super(message);
  }
}
