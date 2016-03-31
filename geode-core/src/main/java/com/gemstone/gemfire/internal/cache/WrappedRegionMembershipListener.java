/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionMembershipListener;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * WrappedRegionMembershipListener is used during initialization of new
 * cache listeners at runtime, after the region has already been initialized
 * and is active.
 */
class WrappedRegionMembershipListener implements
    RegionMembershipListener {

  private RegionMembershipListener wrappedListener;
  
  private Object initLock = new Object();
  
  /**
   * has initMembers been invoked?
   * @guarded.By initLock
   */
  private boolean initialized;

  
  public WrappedRegionMembershipListener(RegionMembershipListener listener) {
    this.wrappedListener = listener;
  }
  
  /** has initMembers been invoked on this object? */
  public boolean isInitialized() {
    synchronized(initLock) {
      return this.initialized;
    }
  }
  
  /** return the wrapped listener object */
  public RegionMembershipListener getWrappedListener() {
    return this.wrappedListener;
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.RegionMembershipListener#afterRemoteRegionCrash(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRemoteRegionCrash(RegionEvent event) {
    synchronized(this.initLock) {
      if (this.initialized) {
        this.wrappedListener.afterRemoteRegionCrash(event);
      }
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.RegionMembershipListener#afterRemoteRegionCreate(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRemoteRegionCreate(RegionEvent event) {
    synchronized(this.initLock) {
      if (this.initialized) {
        this.wrappedListener.afterRemoteRegionCreate(event);
      }
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.RegionMembershipListener#afterRemoteRegionDeparture(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRemoteRegionDeparture(RegionEvent event) {
    synchronized(this.initLock) {
      if (this.initialized) {
        this.wrappedListener.afterRemoteRegionDeparture(event);
      }
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.RegionMembershipListener#initialMembers(com.gemstone.gemfire.cache.Region, com.gemstone.gemfire.distributed.DistributedMember[])
   */
  public void initialMembers(Region region, DistributedMember[] initialMembers) {
    synchronized(this.initLock) {
      if (!this.initialized) {
        this.wrappedListener.initialMembers(region, initialMembers);
        this.initialized = true;
      }
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterCreate(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void afterCreate(EntryEvent event) {
    this.wrappedListener.afterCreate(event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterDestroy(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void afterDestroy(EntryEvent event) {
    this.wrappedListener.afterDestroy(event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterInvalidate(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void afterInvalidate(EntryEvent event) {
    this.wrappedListener.afterInvalidate(event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionClear(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionClear(RegionEvent event) {
    this.wrappedListener.afterRegionClear(event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionCreate(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionCreate(RegionEvent event) {
    this.wrappedListener.afterRegionCreate(event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionDestroy(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionDestroy(RegionEvent event) {
    this.wrappedListener.afterRegionDestroy(event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionInvalidate(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionInvalidate(RegionEvent event) {
    this.wrappedListener.afterRegionInvalidate(event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionLive(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionLive(RegionEvent event) {
    this.wrappedListener.afterRegionLive(event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterUpdate(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void afterUpdate(EntryEvent event) {
    this.wrappedListener.afterUpdate(event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheCallback#close()
   */
  public void close() {
    this.wrappedListener.close();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    return this.wrappedListener.equals(obj);
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.wrappedListener.hashCode();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.wrappedListener.toString();
  }

}
