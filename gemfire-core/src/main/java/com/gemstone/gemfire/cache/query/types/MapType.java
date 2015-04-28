/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.types;

/**
 * Represents the type of a Map, a collection that contains keys as well
 * as values and maintains an association between key-value pairs.
 * The type of the keys is obtained from the getKeyType method, and the type
 * of the values is obtained from the getElementType method.
 *
 * @since 4.0
 * @author Eric Zoerner
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
