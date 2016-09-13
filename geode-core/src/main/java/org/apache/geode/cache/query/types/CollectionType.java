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

/**
 * Represents the type of a collection, an object that can contain element
 * objects.
 *
 * @since GemFire 4.0
 */
public interface CollectionType extends ObjectType {
  
  /** Return the type of the elements of this collection type.
   */
  public ObjectType getElementType();
  
  /**
   * Return whether duplicates are kept in this type of collection. Duplicates
   * are two objects are equal to each other as defined by the <code>equals</code>
   * method. 
   * @return true if duplicates have been retained, false if duplicates have
   * been eliminated
   */
  public boolean allowsDuplicates();
  
  /**
   * Return whether this collection type has ordered elements. 
   * @return true if this collection type is ordered, false if not
   */
  public boolean isOrdered();
}
