package com.gemstone.gemfire.cache.lucene.internal.mergers;

import java.util.List;

import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;

/**
 * Search results collected from individual shards/members are combined into one result set by {@link ResultMergers}.
 */
public abstract class ResultMerger<E> {
  /**
   * @param results
   *          List of the result collected from the shards
   * @return merge result set
   */
  public abstract LuceneQueryResults mergeResults(List<LuceneQueryResults> results);

  /**
   * @param query
   *          Merge operation will be executed within the context of a query For e.g. the query context could be used to control the total number of results
   *          returned.
   */
  public abstract void init(LuceneQuery query);
}
