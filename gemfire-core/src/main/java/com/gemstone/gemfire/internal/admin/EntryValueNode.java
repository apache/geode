/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */      

package com.gemstone.gemfire.internal.admin;

/**
 * Represents an arbitrary object that has been placed into a GemFire
 * <code>Region</code>.
 */
public interface EntryValueNode {

  /**
   * Returns true if this node represents a primitive value or String
   */
  public boolean isPrimitiveOrString();

  /**
   * Returns the field name, if any
   */
  public String getName();

  /**
   * Returns the class name
   */
  public String getType();

  /**
   * Returns the fields in physical inspection, or the logical elements
   * in logical inspection
   */
  public EntryValueNode[] getChildren();

  /**
   * Returns the wrapped primitive value if this is a primitive
   * or the result of calling <code>toString()</code> if this is
   * an object.
   */
  public Object getPrimitiveValue();
}
