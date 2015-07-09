package com.gemstone.gemfire.cache.lucene;

import java.util.Iterator;
import java.util.ServiceLoader;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.LuceneServiceFactory;

public class LuceneServiceProvider {
  
  private static final LuceneServiceFactory factory;

  static {
    ServiceLoader<LuceneServiceFactory> loader = ServiceLoader.load(LuceneServiceFactory.class);
    Iterator<LuceneServiceFactory> itr = loader.iterator();
    if(!itr.hasNext()) {
      factory = null;
    } else {
      factory = itr.next();
      factory.initialize();
    }
  }
  
  public static LuceneService create(Cache cache) {
    
    if(factory == null) {
      return null;
    }
    
    return factory.create(cache);
  }
  
  private LuceneServiceProvider() {
    
  }
}
