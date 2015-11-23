package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.Cache;

public class MockCacheServiceImpl implements MockCacheService {
  
  private Cache cache;

  @Override
  public void init(Cache cache) {
    this.cache = cache;
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return MockCacheService.class;
  }

  @Override
  public Cache getCache() {
    return cache;
  }
}
