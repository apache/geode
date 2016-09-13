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
 * @since GemFire 4.0
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
