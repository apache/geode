package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.Collection;

import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;

/**
 * This class is used to aggregate search results from multiple search requests.
 * 
 * @param <T> Type of reduce result
 * @param <C> Type of IndexResultCollector created by this manager
 */
public interface CollectorManager<T, C extends IndexResultCollector> {
  /**
   * @param name Name/Identifier for this collector. For e.g. region/bucketId.
   * @return a new {@link IndexResultCollector}. This must return a different instance on
   *         each call. A new collector would be created for each bucket on a member node.
   */
  C newCollector(String name);

  /**
   * Reduce the results of individual collectors into a meaningful result. This method must be called after collection
   * is finished on all provided collectors.
   * 
   * @throws IOException
   */
  T reduce(Collection<IndexResultCollector> results) throws IOException;
}
