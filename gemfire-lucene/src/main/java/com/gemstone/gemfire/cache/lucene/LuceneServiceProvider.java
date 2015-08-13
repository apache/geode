package com.gemstone.gemfire.cache.lucene;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.internal.cache.extension.Extensible;

/**
 * Class for retrieving or creating the currently running
 * instance of the LuceneService.
 *
 */
public class LuceneServiceProvider {
  
  /**
   * Retrieve or create the lucene service for this cache
   */
  public static LuceneService get(Cache cache) {
    synchronized(LuceneService.class) {
      Extensible<Cache> extensible = (Extensible<Cache>) cache;
      LuceneServiceImpl service = (LuceneServiceImpl) extensible.getExtensionPoint().getExtension(LuceneService.class);
      if(service == null) {
        service = new LuceneServiceImpl(cache);
        extensible.getExtensionPoint().addExtension(LuceneService.class, service);
      }
      
      return service;
    }
  }
  
  private LuceneServiceProvider() {
    
  }
}
