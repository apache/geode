/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import java.io.DataInput;

/**
 * An <code>InvalidDeltaException</code> is thrown when a delta cannot be
 * successfully applied by the receiving peer/client. The class implementing
 * {@link Delta} may also choose to throw this in
 * {@link Delta#fromDelta(DataInput in)}. GemFire, on encountering this
 * exception distributes the full application object.
 * 
 * @since 6.1
 */
public class InvalidDeltaException extends GemFireException {

  /**
   * Creates a new <code>InvalidDeltaException</code>. 
   */
  public InvalidDeltaException() {
  }

  /**
   * Creates a new <code>InvalidDeltaException</code>. 
   * @param msg String explaining the exception
   */
  public InvalidDeltaException(String msg) {
    super(msg);
  }

  /**
   * Creates a new <code>InvalidDeltaException</code>. 
   * @param e Throwable
   */
  public InvalidDeltaException(Throwable e) {
    super(e);
  }

  /**
   * Creates a new <code>InvalidDeltaException</code>. 
   * @param msg String explaining the exception
   * @param e Throwable
   */
  public InvalidDeltaException(String msg, Throwable e) {
    super(msg, e);
  }

}
