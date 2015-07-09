/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collection;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.lru.EnableLRU;
import com.gemstone.gemfire.internal.cache.lru.LRUEntry;
import com.gemstone.gemfire.internal.cache.lru.NewLRUClockHand;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Implementation of RegionMap that reads data from HDFS and adds LRU behavior
 * 
 * @author sbawaska
 */
public class HDFSLRURegionMap extends AbstractLRURegionMap implements HDFSRegionMap {

  private static final Logger logger = LogService.getLogger();

  private final HDFSRegionMapDelegate delegate;

  /**
   *  A tool from the eviction controller for sizing entries and
   *  expressing limits.
   */
  private EnableLRU ccHelper;

  /**  The list of nodes in LRU order */
  private NewLRUClockHand lruList;

  private static final boolean DEBUG = Boolean.getBoolean("hdfsRegionMap.DEBUG");

  public HDFSLRURegionMap(LocalRegion owner, Attributes attrs,
      InternalRegionArguments internalRegionArgs) {
    super(internalRegionArgs);
    assert owner instanceof BucketRegion;
    initialize(owner, attrs, internalRegionArgs);
    this.delegate = new HDFSRegionMapDelegate(owner, attrs, internalRegionArgs, this);
  }

  @Override
  public RegionEntry getEntry(Object key) {
    return delegate.getEntry(key, null);
  }

  @Override
  protected RegionEntry getEntry(EntryEventImpl event) {
    return delegate.getEntry(event);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<RegionEntry> regionEntries() {
    return delegate.regionEntries();
  }
    
  @Override
  public int size() {
    return delegate.size();
  }
    
  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  protected void _setCCHelper(EnableLRU ccHelper) {
    this.ccHelper = ccHelper;
  }

  @Override
  protected EnableLRU _getCCHelper() {
    return this.ccHelper;
  }

  @Override
  protected void _setLruList(NewLRUClockHand lruList) {
    this.lruList = lruList;
  }

  @Override
  protected NewLRUClockHand _getLruList() {
    return this.lruList;
  }

  @Override
  public HDFSRegionMapDelegate getDelegate() {
    return this.delegate;
  }
}
