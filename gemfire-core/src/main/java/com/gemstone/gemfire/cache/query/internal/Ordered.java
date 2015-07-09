package com.gemstone.gemfire.cache.query.internal;

import java.util.Comparator;

/**
 *
 * This interface is to be implemented by all the query SelectResults implementation which have ordered
 * data. This encompasses those classes which have data stored in a List, LinkedMap, LinkedSet, TreeMap
 * , TreeSet etc.
 * @see NWayMergeResults
 * @see SortedResultsBag
 * @see SortedStructBag
 * @see SortedStructSet
 * @see SortedResultSet
 * @see LinkedResultSet
 * @see LinkedStructSet
 * @author ashahid
 *
 */
public interface Ordered {
  Comparator comparator(); 
  
  //Implies that underlying structure is a LinkedHashMap or LinkedHashSet & the structs are stored
  // directly , ie not in terms of Object[]
  // SortedResultsBag, LinkedResultSet are two such types.
  
  boolean dataPreordered();
}
