/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.types;

/**
 * Represents the type of a collection, an object that can contain element
 * objects.
 *
 * @since 4.0
 * @author Eric Zoerner
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
