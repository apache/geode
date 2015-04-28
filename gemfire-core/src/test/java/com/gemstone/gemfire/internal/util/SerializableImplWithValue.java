/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

//import java.io.*;

/**
 * A {@link SerializableImpl} that implements an interface and has
 * some object state.
 *
 * @see DeserializerTest
 *
 * @author David Whitlock
 *
 * @since 2.0.3
 */
public class SerializableImplWithValue extends SerializableImpl 
  implements Valuable {

  /** This object's state */
  private Object value;

  /**
   * Zero-argument constructor
   */
  public SerializableImplWithValue() {

  }

  /**
   * Creates a new <code>SerializableImplWithValue</code> with a given
   * value
   */
  public SerializableImplWithValue(Object value) {
    this.value = value;
  }

  public Object getValue() {
    return this.value;
  }

  public void setValue(Object value) {
    this.value = value;
  }
}
