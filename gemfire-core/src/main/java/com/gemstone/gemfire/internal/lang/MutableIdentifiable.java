  /*
   * =========================================================================
   *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
   *  This product is protected by U.S. and international copyright
   *  and intellectual property laws. Pivotal products are covered by
   *  more patents listed at http://www.pivotal.io/patents.
   * =========================================================================
   */
package com.gemstone.gemfire.internal.lang;

import com.gemstone.gemfire.lang.Identifiable;

/**
 * The MutableIdentifiable interface defines a contract for classes whose mutable Object instances can
 * be uniquely identified relative to other Object instances within the same class type hierarchy.
 * <p/>
 * @author John Blum
 * @param <T> the class type of the identifier.
 * @see java.lang.Comparable
 * @since 7.0
 */
public interface MutableIdentifiable<T>  extends Identifiable {

  /**
   * Set the identifier uniquely identifying this Object instance.
   * <p/>
   * @param id an identifier uniquely identifying this Object.
   */
  public void setId(T id);

}
