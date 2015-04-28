/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.lang;

import java.io.Serializable;

/**
 * The Identifiable interface defines a contract for classes whose Object instances can be uniquely identified relative
 * to other Object instances within the same class type hierarchy.
 * <p/>
 * @author John Blum
 * @param <T> the class type of the identifier.
 * @see java.lang.Comparable
 * @since 7.0
 */
public interface Identifiable<T extends Comparable<T>> extends Serializable {

  /**
   * Gets the identifier uniquely identifying this Object instance.
   * <p/>
   * @return an identifier uniquely identifying this Object.
   */
  public T getId();

}
