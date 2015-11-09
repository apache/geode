package com.gemstone.gemfire.cache.lucene;

import com.gemstone.gemfire.annotations.Experimental;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.internal.InternalLuceneService;
import com.gemstone.gemfire.internal.cache.InternalCache;

/**
 * Class for retrieving or creating the currently running
 * instance of the LuceneService.
 *
 */
@Experimental
public class LuceneServiceProvider {
  
  /**
   * Retrieve or create the lucene service for this cache
   */
  public static LuceneService get(Cache cache) {
    InternalCache internalCache = (InternalCache) cache;
    return internalCache.getService(InternalLuceneService.class);
  }
  
  private LuceneServiceProvider() {
    
  }
}
