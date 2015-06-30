package com.gemstone.gemfire.cache.lucene.internal;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceFactory;

public class LuceneServiceFactoryImpl implements LuceneServiceFactory {
  
  public void initialize() {
  }

  @Override
  public LuceneService create(Cache cache) {
    return LuceneServiceImpl.getInstance(cache);
  }
}
