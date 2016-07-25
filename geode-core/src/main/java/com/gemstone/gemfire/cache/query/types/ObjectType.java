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

package com.gemstone.gemfire.cache.query.types;

import com.gemstone.gemfire.DataSerializable;

/**
 * An ObjectType represents the type of an object in a query.
 * An ObjectType is similar to a Class, except unlike a Class it can be
 * extended to add more information such as a subtype for collection Classes,
 * a key type for a map class, or a field information for structs.
 *
 * Note that multiple instances of are allowed of the same type, so ObjectTypes
 * should always be compared using equals.
 *
 * @see StructType
 * @see CollectionType
 * @see MapType
 *
 * @since GemFire 4.0
 */
public interface ObjectType extends DataSerializable {

  /**
   * Return true if this is a CollectionType. Note that MapTypes, Region types,
   * and array types are also considered CollectionTypes in the context of the
   * query language and therefore return true to this method.
   */
  public boolean isCollectionType();
  
  /** Return true if this is a MapType */
  public boolean isMapType();
  
  /** Return true if this is a StructType */
  public boolean isStructType();
  
  /** @return the simple name for the class this resolves to without including
    * the package */
  public String getSimpleClassName();
  
  /** @return the Class that this type corresponds to.
   */
  public Class resolveClass();
}
