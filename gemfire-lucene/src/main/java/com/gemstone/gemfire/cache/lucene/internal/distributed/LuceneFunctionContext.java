package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataSerializable;

/**
 * Contains function arguments for text / lucene search
 */
public class LuceneFunctionContext<C extends IndexResultCollector> implements VersionedDataSerializable {
  private static final long serialVersionUID = 1L;
  private final CollectorManager<C> manager;
  private final int limit;

  public LuceneFunctionContext(CollectorManager<C> manager) {
    this.manager = manager;
    this.limit = LuceneQueryFactory.DEFAULT_LIMIT;
  }
  
  public LuceneFunctionContext(int limit) {
    this.limit = limit;
    this.manager = null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // TODO Auto-generated method stub

  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
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
}
