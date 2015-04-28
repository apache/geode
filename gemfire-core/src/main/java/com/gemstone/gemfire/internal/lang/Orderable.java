/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.lang;

/**
 * The Orderable interface defines a contract for classes whose Objects can be sorted, or ordered according to the
 * order property of a Comparable type.
 * <p/>
 * @author John Blum
 * @see java.lang.Comparable
 * @since 6.8
 */
public interface Orderable<T extends Comparable<T>> {

  /**
   * Returns the value of the order property used in ordering instances of this implementing Object relative to other
   * type compatible Object instances.  The order property value can also be used in sorting operations, or in
   * maintaining Orderable objects in ordered data structures like arrays or Lists, or even for defining a precedence
   * not directly related order.
   * <p/>
   * @return a value that is Comparable to other value of the same type and defines the relative order of this Object
   * instance to it's peers.
   */
  public T getOrder();

}
