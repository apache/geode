package com.gemstone.gemfire.cache.lucene;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.LuceneService;

public interface LuceneServiceFactory {
  
  public void initialize();
  
  /**
   * Create a new LuceneService for the given cache
   */
  public LuceneService create(Cache cache);
}
