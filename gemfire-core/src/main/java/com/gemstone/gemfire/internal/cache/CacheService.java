package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.Cache;

/**
 * Interface for a service that is linked to a cache.
 * 
 * These services are loaded during cache initialization using the java
 * ServiceLoader and can be retrieved from the cache by calling
 * Cache.getService(YourInterface.class)
 */
public interface CacheService {
  /**
   * Initialize the service with a cache.
   * 
   * Services are initialized in random order, fairly early on in cache
   * initialization. In particular, the cache.xml has not yet been parsed.
   */
  public void init(Cache cache);

  /**
   * Return the class or interface used to look up
   * this service. 
   */
  public Class<? extends CacheService> getInterface();
}
