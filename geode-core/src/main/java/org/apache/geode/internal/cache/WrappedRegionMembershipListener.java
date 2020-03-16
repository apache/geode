/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import org.jgroups.annotations.GuardedBy;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionMembershipListener;
import org.apache.geode.distributed.DistributedMember;

/**
 * WrappedRegionMembershipListener is used during initialization of new cache listeners at runtime,
 * after the region has already been initialized and is active.
 */
class WrappedRegionMembershipListener implements RegionMembershipListener {

  private RegionMembershipListener wrappedListener;

  private Object initLock = new Object();

  /**
   * has initMembers been invoked?
   */
  @GuardedBy("initLock")
  private boolean initialized;


  public WrappedRegionMembershipListener(RegionMembershipListener listener) {
    this.wrappedListener = listener;
  }

  /** has initMembers been invoked on this object? */
  public boolean isInitialized() {
    synchronized (initLock) {
      return this.initialized;
    }
  }

  /** return the wrapped listener object */
  public RegionMembershipListener getWrappedListener() {
    return this.wrappedListener;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.RegionMembershipListener#afterRemoteRegionCrash(org.apache.geode.cache.
   * RegionEvent)
   */
  @Override
  public void afterRemoteRegionCrash(RegionEvent event) {
    synchronized (this.initLock) {
      if (this.initialized) {
        this.wrappedListener.afterRemoteRegionCrash(event);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.RegionMembershipListener#afterRemoteRegionCreate(org.apache.geode.cache.
   * RegionEvent)
   */
  @Override
  public void afterRemoteRegionCreate(RegionEvent event) {
    synchronized (this.initLock) {
      if (this.initialized) {
        this.wrappedListener.afterRemoteRegionCreate(event);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.RegionMembershipListener#afterRemoteRegionDeparture(org.apache.geode.
   * cache.RegionEvent)
   */
  @Override
  public void afterRemoteRegionDeparture(RegionEvent event) {
    synchronized (this.initLock) {
      if (this.initialized) {
        this.wrappedListener.afterRemoteRegionDeparture(event);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.RegionMembershipListener#initialMembers(org.apache.geode.cache.Region,
   * org.apache.geode.distributed.DistributedMember[])
   */
  @Override
  public void initialMembers(Region region, DistributedMember[] initialMembers) {
    synchronized (this.initLock) {
      if (!this.initialized) {
        this.wrappedListener.initialMembers(region, initialMembers);
        this.initialized = true;
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.CacheListener#afterCreate(org.apache.geode.cache.EntryEvent)
   */
  @Override
  public void afterCreate(EntryEvent event) {
    this.wrappedListener.afterCreate(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.CacheListener#afterDestroy(org.apache.geode.cache.EntryEvent)
   */
  @Override
  public void afterDestroy(EntryEvent event) {
    this.wrappedListener.afterDestroy(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.CacheListener#afterInvalidate(org.apache.geode.cache.EntryEvent)
   */
  @Override
  public void afterInvalidate(EntryEvent event) {
    this.wrappedListener.afterInvalidate(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.CacheListener#afterRegionClear(org.apache.geode.cache.RegionEvent)
   */
  @Override
  public void afterRegionClear(RegionEvent event) {
    this.wrappedListener.afterRegionClear(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.CacheListener#afterRegionCreate(org.apache.geode.cache.RegionEvent)
   */
  @Override
  public void afterRegionCreate(RegionEvent event) {
    this.wrappedListener.afterRegionCreate(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.CacheListener#afterRegionDestroy(org.apache.geode.cache.RegionEvent)
   */
  @Override
  public void afterRegionDestroy(RegionEvent event) {
    this.wrappedListener.afterRegionDestroy(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.CacheListener#afterRegionInvalidate(org.apache.geode.cache.RegionEvent)
   */
  @Override
  public void afterRegionInvalidate(RegionEvent event) {
    this.wrappedListener.afterRegionInvalidate(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.CacheListener#afterRegionLive(org.apache.geode.cache.RegionEvent)
   */
  @Override
  public void afterRegionLive(RegionEvent event) {
    this.wrappedListener.afterRegionLive(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.CacheListener#afterUpdate(org.apache.geode.cache.EntryEvent)
   */
  @Override
  public void afterUpdate(EntryEvent event) {
    this.wrappedListener.afterUpdate(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.CacheCallback#close()
   */
  @Override
  public void close() {
    this.wrappedListener.close();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    return this.wrappedListener.equals(obj);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.wrappedListener.hashCode();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.wrappedListener.toString();
  }

}
