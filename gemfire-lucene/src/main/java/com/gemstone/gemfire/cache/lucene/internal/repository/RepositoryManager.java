package com.gemstone.gemfire.cache.lucene.internal.repository;

import java.util.Collection;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;

/**
 * {@link RepositoryManager} instances will be used to get {@link IndexRepository} instances hosting index data for
 * {@link Region}s
 */
public interface RepositoryManager {

  IndexRepository getRepository(Region region, Object key, Object callbackArg) throws BucketNotFoundException;

  /**
   * Returns a collection of {@link IndexRepository} instances hosting index data of the input list of bucket ids. The
   * bucket needs to be present on this member.
   * 
   * @param localDataSet The local data set of a function
   * @return a collection of {@link IndexRepository} instances
   * @throws BucketNotFoundException if any of the requested buckets is not found on this member
   */
  Collection<IndexRepository> getRepositories(RegionFunctionContext context) throws BucketNotFoundException;
}
