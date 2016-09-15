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
package org.apache.geode.admin;

import org.apache.geode.cache.RegionAttributes;

/**
 * Administrative interface that represent's the {@link SystemMember}'s view
 * of its {@link org.apache.geode.cache.Cache}.
 *
 * @since GemFire     3.5
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code> package instead
 */
public interface SystemMemberCache {
  // attributes
  /**
   * The name of the cache.
   */
  public String getName();
  /**
   * Value that uniquely identifies an instance of a cache for a given member.
   */
  public int getId();
  /**
   * Indicates if this cache has been closed.
   * @return true, if this cache is closed; false, otherwise
   */
  public boolean isClosed();
  /**
   * Gets the number of seconds a cache operation will wait to obtain
   * a distributed lock lease.
   */
  public int getLockTimeout();
  /**
   * Sets the number of seconds a cache operation may wait to obtain a
   * distributed lock lease before timing out.
   *
   * @throws AdminException
   *         If a problem is encountered while setting the lock
   *         timeout 
   *
   * @see org.apache.geode.cache.Cache#setLockTimeout
   */
  public void setLockTimeout(int seconds) throws AdminException;
  
  /**
   * Gets the length, in seconds, of distributed lock leases obtained
   * by this cache.
   */
  public int getLockLease();
  /**
   * Sets the length, in seconds, of distributed lock leases obtained
   * by this cache.
   *
   * @throws AdminException
   *         If a problem is encountered while setting the lock
   *         lease
   *
   * @see org.apache.geode.cache.Cache#setLockLease
   */
  public void setLockLease(int seconds) throws AdminException;
  
  /**
   * Gets the number of seconds a cache
   * {@link org.apache.geode.cache.Region#get(Object) get} operation
   * can spend searching for a value before it times out.
   * The search includes any time spent loading the object.
   * When the search times out it causes the get to fail by throwing
   * an exception.
   */
  public int getSearchTimeout();
  /**
   * Sets the number of seconds a cache get operation can spend searching
   * for a value.
   *
   * @throws AdminException
   *         If a problem is encountered while setting the search
   *         timeout 
   *
   * @see org.apache.geode.cache.Cache#setSearchTimeout
   */
  public void setSearchTimeout(int seconds) throws AdminException;
  /**
   * Returns number of seconds since this member's cache has been created.
   * Returns <code>-1</code> if this member does not have a cache or its cache
   * has been closed.
   */
  public int getUpTime();

  /**
   * Returns the names of all the root regions currently in this cache.
   */
  public java.util.Set getRootRegionNames();

  // operations

  /**
   * Returns statistics related to this cache's performance.
   */
  public Statistic[] getStatistics();

  /**
   * Return the existing region (or subregion) with the specified
   * path that already exists in the cache.
   * Whether or not the path starts with a forward slash it is interpreted as a
   * full path starting at a root.
   *
   * @param path the path to the region
   * @return the Region or null if not found
   * @throws IllegalArgumentException if path is null, the empty string, or "/"
   */
  public SystemMemberRegion getRegion(String path) throws AdminException;

  /**
   * Creates a VM root <code>Region</code> in this cache.
   *
   * @param name
   *        The name of the region to create
   * @param attrs
   *        The attributes of the root region
   *
   * @throws AdminException
   *         If the region cannot be created
   *
   * @since GemFire 4.0
   * @deprecated as of GemFire 5.0, use {@link #createRegion} instead
   */
  @Deprecated
  public SystemMemberRegion createVMRegion(String name,
                                           RegionAttributes attrs)
    throws AdminException;

  /**
   * Creates a root <code>Region</code> in this cache.
   *
   * @param name
   *        The name of the region to create
   * @param attrs
   *        The attributes of the root region
   *
   * @throws AdminException
   *         If the region cannot be created
   *
   * @since GemFire 5.0
   */
  public SystemMemberRegion createRegion(String name,
                                         RegionAttributes attrs)
    throws AdminException;

  /**
   * Updates the state of this cache instance. Note that once a cache
   * instance is closed refresh will never change the state of that instance.
   */
  public void refresh();

  /**
   * Adds a new, unstarted cache server that will serve the contents
   * of this cache to clients.
   *
   * @see org.apache.geode.cache.Cache#addCacheServer
   *
   * @since GemFire 5.7
   */
  public SystemMemberCacheServer addCacheServer()
    throws AdminException;

  /**
   * Returns the cache servers that run in this member's VM.  Note
   * that this list will not be updated until {@link #refresh} is
   * called.
   *
   * @see org.apache.geode.cache.Cache#getCacheServers
   *
   * @since GemFire 5.7
   */
  public SystemMemberCacheServer[] getCacheServers()
    throws AdminException;

  /**
   * Returns whether or not this cache acts as a server.  This method
   * will always return <code>true</code> for the
   * <code>SystemMemberCache</code> obtained from a {@link
   * CacheServer}.  Note that this value will not be updated until
   * {@link #refresh} is invoked.
   *
   * @since GemFire 4.0
   */
  public boolean isServer() throws AdminException;
}

