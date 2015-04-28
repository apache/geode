/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
 * @since 4.0
 * @author Eric Zoerner
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
