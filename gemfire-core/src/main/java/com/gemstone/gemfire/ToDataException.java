/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * A <code>ToDataException</code> is thrown during serialization if
 * {@link DataSerializable#toData} throws an exception or if
 * {@link DataSerializer#toData} is called and returns false.
 * 
 * @since 6.5
 * @author darrel
 */
public class ToDataException extends SerializationException {
  private static final long serialVersionUID = -2329606027453879918L;
  /**
   * Creates a new <code>ToDataException</code> with the given message
   */
  public ToDataException(String message) {
      super(message);
  }
  /**
   * Creates a new <code>ToDataException</code> with the given message
   * and cause.
   */
  public ToDataException(String message, Throwable cause) {
      super(message, cause);
  }
}
