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

package org.apache.geode.cache.query.types;


/**
 * Describes the field names and types for each field in a {@link
 * org.apache.geode.cache.query.Struct}.
 *
 * @since GemFire 4.0
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
