/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collection;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.internal.size.SingleObjectSizer;

/**
 * Implementation of RegionMap that reads data from HDFS.
 * 
 * @author sbawaska
 */
public class HDFSRegionMapImpl extends AbstractRegionMap implements HDFSRegionMap {

  private final HDFSRegionMapDelegate delegate;

  private static final boolean DEBUG = Boolean.getBoolean("hdfsRegionMap.DEBUG");

  public HDFSRegionMapImpl(LocalRegion owner, Attributes attrs,
      InternalRegionArguments internalRegionArgs) {
    super(internalRegionArgs);
    assert owner instanceof BucketRegion;
    initialize(owner, attrs, internalRegionArgs, false);
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
  public HDFSRegionMapDelegate getDelegate() {
    return this.delegate;
  }

}
