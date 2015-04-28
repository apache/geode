/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;

/**
 * Delegates object comparisons to one or more embedded comparators.
 *  
 * @author bakera
 */
public interface DelegatingSerializedComparator extends SerializedComparator {
  /**
   * Injects the embedded comparators.
   * @param comparators the comparators for delegation
   */
  void setComparators(SerializedComparator[] comparators);
  
  /**
   * Returns the embedded comparators.
   * @return the comparators
   */
  SerializedComparator[] getComparators();
}
