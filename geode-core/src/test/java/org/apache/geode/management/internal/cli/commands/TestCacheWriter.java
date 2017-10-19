package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;

public class TestCacheWriter implements CacheWriter {
  @Override
  public void beforeUpdate(EntryEvent event) throws CacheWriterException {}

  @Override
  public void beforeCreate(EntryEvent event) throws CacheWriterException {}

  @Override
  public void beforeDestroy(EntryEvent event) throws CacheWriterException {}

  @Override
  public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {}

  @Override
  public void beforeRegionClear(RegionEvent event) throws CacheWriterException {}

  @Override
  public void close() {}
}
