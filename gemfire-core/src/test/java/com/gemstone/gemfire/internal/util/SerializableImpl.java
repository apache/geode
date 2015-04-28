/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

import java.io.*;

/**
 * A {@link Serializable} class that is loaded by a class loader other
 * than the one that is used to load test classes.
 *
 * @see DeserializerTest
 *
 * @author David Whitlock
 *
 * @since 2.0.1
 */
public class SerializableImpl implements Serializable {

  /**
   * Creates a new <code>SerializableImpl</code>
   */
  public SerializableImpl() {

  }

}
