package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;

/**
 * Contains function arguments for text / lucene search
 */
public class LuceneFunctionContext<C extends IndexResultCollector> implements DataSerializable {
  private static final long serialVersionUID = 1L;

  private final CollectorManager<C> manager;
  private final int limit;

  final LuceneQueryProvider queryProvider;

  public LuceneFunctionContext(LuceneQueryProvider provider) {
    this(provider, null);
  }

  public LuceneFunctionContext(LuceneQueryProvider provider, CollectorManager<C> manager) {
    this(provider, manager, LuceneQueryFactory.DEFAULT_LIMIT);
  }

  public LuceneFunctionContext(LuceneQueryProvider provider, CollectorManager<C> manager, int limit) {
    this.queryProvider = provider;
    this.manager = manager;
    this.limit = limit;
  }

  /**
   * @return The maximum count of result objects to be produced by the function
   */
  public int getLimit() {
    return limit;
  }

  /**
   * On each member, search query is executed on one or more {@link IndexRepository}s. A {@link CollectorManager} could
   * be provided to customize the way results from these repositories is collected and merged.
   * 
   * @return {@link CollectorManager} instance to be used by function
   */
  public CollectorManager<C> getCollectorManager() {
    return this.manager;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // TODO Auto-generated method stub

  }

  public LuceneQueryProvider getQueryProvider() {
    return queryProvider;
  }
}
