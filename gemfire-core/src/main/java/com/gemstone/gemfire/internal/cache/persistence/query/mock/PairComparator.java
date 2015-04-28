/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.query.mock;

import java.util.Comparator;

/**
 * A comparator for Pair objects that uses two
 * passed in comparators.
 * @author dsmith
 *
 */
public class PairComparator implements Comparator<Pair> {
  private Comparator xComparator;
  private Comparator yComparator;

  public PairComparator(Comparator xComparator, Comparator yComparator) {
    this.xComparator = xComparator;
    this.yComparator = yComparator;
    
  }

  @Override
  public int compare(Pair o1, Pair o2) {
    int result = xComparator.compare(o1.getX(), o2.getX());
    if(result == 0) {
      result = yComparator.compare(o1.getY(), o2.getY());
    }
    return result;
  }

}
