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
 * The Ordered interface defines a contract for implementing classes who's instances must participate in some type
 * of ordered data structure, such as an array or List, or exist in a context where order relative to other
 * peer instances matter.
 * <p/>
 * @author John Blum
 * @since 6.8
 */
public interface Ordered {

  /**
   * Gets the order of this instance relative to it's peers.
   * <p/>
   * @return an integer value indicating the order of this instance relative to it's peers.
   */
  public int getIndex();

  /**
   * Sets the order of this instance relative to it's peers.
   * <p/>
   * @param index an integer value specifying the the order of this instance relative to it's peers.
   */
  public void setIndex(int index);

}
