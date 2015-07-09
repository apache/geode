/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples;

/**
 * A simple test object used by the {@link
 * com.gemstone.gemfire.internal.enhancer.serializer.SerializingStreamPerfTest} * that must be in a non-<code>com.gemstone</code> package.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class TestObject {

  private int intField;
  private String stringField;
  private Object objectField;

  /**
   * Creates a new <code>TestObject</code>
   */
  public TestObject() {
    this.intField = 42;
    this.stringField = "123456789012345678901234567890";
    this.objectField = new Integer(67);
  }

  //////////////////////  Inner Classes  //////////////////////

  /**
   * A <code>Serializable</code> object that is serialized
   */
  public static class SerializableTestObject extends TestObject
    implements java.io.Serializable {
    
  }
  
}

