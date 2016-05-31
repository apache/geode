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
 * Represents the type of a Map, a collection that contains keys as well
 * as values and maintains an association between key-value pairs.
 * The type of the keys is obtained from the getKeyType method, and the type
 * of the values is obtained from the getElementType method.
 *
 * @since GemFire 4.0
 */
public interface MapType extends CollectionType {
  
  /**
   * Return the type of the keys in this type of map.
   * @return the ObjectType of the keys in this type of map.
   */
  public ObjectType getKeyType();
  
  /** Return the type of the entries in this map.
   *  In the context of the query language, the entries in a map are
   *  structs with key and value fields.
   */
  public StructType getEntryType();
}
