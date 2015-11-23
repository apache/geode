package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.Cache;

public interface MockCacheService extends CacheService {
  
  public Cache getCache();
}
