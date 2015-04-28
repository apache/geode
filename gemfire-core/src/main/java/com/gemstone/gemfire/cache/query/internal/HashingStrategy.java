/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal;

import java.io.Serializable;

/**
 * This interface defines a Hashing strategy for keys(OR values)
 * in a HashMap(OR HashSet) for calculation of hash-code for
 * custom objects and primitive types.
 * 
 * @author shobhit
 * @since 8.0
 *
 */
public interface HashingStrategy extends Serializable {

  /**
   * Computes a hash code for the specified object.  Implementors
   * can use the object's own <tt>hashCode</tt> method, the Java
   * runtime's <tt>identityHashCode</tt>, or a custom scheme.
   * 
   * @param o object for which the hash-code is to be computed
   * @return the hashCode
   */
  public int hashCode(Object o);

  /**
   * Compares o1 and o2 for equality.  Strategy implementors may use
   * the objects' own equals() methods, compare object references,
   * or implement some custom scheme.
   *
   * @param o1 an <code>Object</code> value
   * @param o2 an <code>Object</code> value
   * @return true if the objects are equal according to this strategy.
   */
  public boolean equals(Object o1, Object o2);
}
