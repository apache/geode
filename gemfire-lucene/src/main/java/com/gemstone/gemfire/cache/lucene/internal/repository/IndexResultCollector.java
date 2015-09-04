package com.gemstone.gemfire.cache.lucene.internal.repository;

/**
 * Interface for collection results of a query on
 * an IndexRepository. See {@link IndexRepository#query(org.apache.lucene.search.Query, int, IndexResultCollector)}
 */
public interface IndexResultCollector {
  /**
   * @return Name/identifier of this collector
   */
  public String getName();

  /**
   * @return Number of results collected by this collector
   */
  public int size();

  /**
   * Collect a single document
   * 
   * @param key the gemfire key of the object
   * @param score the lucene score of this object
   */
  void collect(Object key, float score);
}
