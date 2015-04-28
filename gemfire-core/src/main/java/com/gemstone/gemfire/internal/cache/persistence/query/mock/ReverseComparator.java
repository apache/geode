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
 * A comparator which reverses the comparison order
 * @author dsmith
 *
 */
public class ReverseComparator implements Comparator<Comparable> {
  public final Comparator<Comparable> comparator;

  public ReverseComparator(Comparator<Comparable> comparator) {
    this.comparator = comparator;
  }



  @Override
  public int compare(Comparable o1, Comparable o2) {
    return -comparator.compare(o1, o2);
  }

}
