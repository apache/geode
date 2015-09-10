package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataSerializable;

/**
 * Contains function arguments for text / lucene search
 */
public class LuceneSearchFunctionArgs implements VersionedDataSerializable {
  private static final long serialVersionUID = 1L;

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
    return LuceneQueryFactory.DEFAULT_LIMIT;
  }

  /**
   * On each member, search query is executed on one or more {@link IndexRepository}s. A {@link CollectorManager} could
   * be provided to customize the way results from these repositories is collected and merged.
   * 
   * @return {@link CollectorManager} instance to be used by function
   */
  public <T, C extends IndexResultCollector> CollectorManager<T, C> getCollectorManager() {
    return null;
  }
}
