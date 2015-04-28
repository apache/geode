/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.Comparator;

import com.gemstone.gemfire.cache.query.SelectResults;

/**
 * Comparator used by the sorted set for storing the results obtained from
 * evaluation of various filter operands in an increasing order of the size ,
 * which will ensure that the intersection of the results for evaluation of AND
 * junction is optimum in performance.
 * 
 * @author ketan
 */

class SelectResultsComparator implements Comparator {

  /**
   * Sort the array in ascending order of collection sizes.
   */
  public int compare(Object obj1, Object obj2) {
    if (!(obj1 instanceof SelectResults) || !(obj2 instanceof SelectResults)) {
      Support.assertionFailed("The objects need to be of type SelectResults");
    }
    int answer = -1;
    SelectResults sr1 = (SelectResults) obj1;
    SelectResults sr2 = (SelectResults) obj2;
    int sizeDifference = sr1.size() - sr2.size();
    if (obj1 == obj2) {
      answer = 0;
    }
    else if (sizeDifference > 0) {
      answer = 1;
    }
    return answer;
  }

  /**
   * Overwrite default equals implementation.
   */
  @Override
  public boolean equals(Object o1) {
    return this == o1;
  }
}
