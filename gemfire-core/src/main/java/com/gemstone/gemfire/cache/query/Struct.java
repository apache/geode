/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

//import java.util.*;
import com.gemstone.gemfire.cache.query.types.StructType;

/**
 * An immutable and thread-safe data type used by the result of some
 * <code>SELECT</code> queries.  It allows
 * us to represent of "tuple" of values.  It has a fixed number of
 * "fields", each of which has a name and a value.  The names and
 * types of these fields are described by a {@link StructType}.
 *
 * @see SelectResults
 *
 * @author Eric Zoerner
 * @since 4.0
 */
public interface Struct {
  
  /**
   * Return the value associated with the given field name
   *
   * @param fieldName the String name of the field
   * @return the value associated with the specified field
   * @throws IllegalArgumentException If this struct does not have a field named fieldName
   *
   * @see StructType#getFieldIndex
   */
  public Object get(String fieldName);
  
  /**
   * Get the values in this struct
   * @return the array of values
   */
  public Object[] getFieldValues();
  
  /**
   * Returns the <code>StructType</code> that describes the fields of
   * this <code>Struct</code>.
   */
  public StructType getStructType();
}
