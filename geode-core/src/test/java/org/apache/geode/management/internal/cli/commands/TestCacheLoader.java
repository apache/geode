package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;

public class TestCacheLoader implements CacheLoader {
  @Override
  public Object load(LoaderHelper helper) throws CacheLoaderException {
    return null;
  }

  @Override
  public void close() {}
}
