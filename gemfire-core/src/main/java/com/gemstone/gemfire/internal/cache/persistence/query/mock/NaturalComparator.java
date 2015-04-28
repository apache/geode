package com.gemstone.gemfire.internal.cache.persistence.query.mock;

import java.util.Comparator;

/**
 * A comparator which compares to objects in natural order.
 * @author dsmith
 *
 */
public class NaturalComparator implements Comparator<Comparable> {

  @Override
  public int compare(Comparable o1, Comparable o2) {
    return o1.compareTo(o2);
  }

}
