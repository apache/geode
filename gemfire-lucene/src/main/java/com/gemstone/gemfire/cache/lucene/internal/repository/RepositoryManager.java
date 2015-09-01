package com.gemstone.gemfire.cache.lucene.internal.repository;

import java.util.Collection;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;

/**
 * {@link RepositoryManager} instances will be used to get {@link IndexRepository} instances hosting index data for
 * {@link Region}s
 */
public interface RepositoryManager {

  IndexRepository getRepository(Region region, Object key);

  /**
   * Returns a collection of {@link IndexRepository} instances hosting index data of the input list of bucket ids. The
   * bucket needs to be present on this member.
   * 
   * @param region
   * @param buckets buckets of a Partitioned region for which {@link IndexRepository}s needs to be discovered. null
   *          for all primary buckets on this member or if the region is Replicated.
   * @return a collection of {@link IndexRepository} instances
   * @throws BucketNotFoundException if any of the requested buckets is not found on this member
   */
  Collection<IndexRepository> getRepositories(Region region, Set<Integer> buckets) throws BucketNotFoundException;
}
