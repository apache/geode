/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.types;


/**
 * Describes the field names and types for each field in a {@link
 * com.gemstone.gemfire.cache.query.Struct}.
 *
 * @author Eric Zoerner
 * @since 4.0
 */
public interface StructType extends ObjectType {
  
  /**
   * The the types of the fields for this struct
   * @return the array of Class for the fields
   */
  ObjectType[] getFieldTypes();

  /**
   * Get the names of the fields for this struct
   * @return the array of field names
   */
  String[] getFieldNames();

  /**
   * Returns the index of the field with the given name in this
   * <code>StructType</code>. 
   *
   * @throws IllegalArgumentException
   *         If this <code>StructType</code> does not contain a field
   *         named <code>fieldName</code>.
   */
  public int getFieldIndex(String fieldName);
  
}
