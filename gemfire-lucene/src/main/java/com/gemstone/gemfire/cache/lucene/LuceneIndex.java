/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.lucene;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;

import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;


/**
 * An lucene index is built over the data stored in a GemFire Region.
 * <p>
 * An index is specified using a index name, field names, region name.
 * <p>
 * The index name and region name together uniquely identifies the lucene index.
 * <p>
 * 
 * @author Xiaojian Zhou
 * @since 8.5
 */
public interface LuceneIndex {

  /**
   * @return the index name of this index
   */
  public String getName();

  /**
   * @return the region name for this index
   */
  public String getRegionPath();
      
  /**
   * @return the indexed field names in a Set
   */
  public String[] getFieldNames();
  
  /**
   * @return the indexed PDX field names in a Set
   */
  public String[] getPDXFieldNames();
  
  /**
   * @return the field to analyzer map
   */
  public Map<String, Analyzer> getFieldAnalyzerMap();
  
  /**
   * Returns a collection of {@link IndexRepository} instances hosting index data of the input list of bucket ids. The
   * bucket needs to be present on this member.
   * 
   * @param ctx {@link RegionFunctionContext} function context. It's either a replicated region
   * or local buckets of a Partitioned region for which {@link IndexRepository}s needs to be discovered. 
   * empty for all primary buckets are not on this member.
   * @return a collection of {@link IndexRepository} instances
   */
  public Collection<IndexRepository> getRepository(RegionFunctionContext ctx);
}
