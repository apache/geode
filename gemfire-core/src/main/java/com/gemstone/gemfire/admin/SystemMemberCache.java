/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.cache.RegionAttributes;

/**
 * Administrative interface that represent's the {@link SystemMember}'s view
 * of its {@link com.gemstone.gemfire.cache.Cache}.
 *
 * @author    Darrel Schneider
 * @since     3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
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
   * @see com.gemstone.gemfire.cache.Cache#setLockTimeout
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
   * @see com.gemstone.gemfire.cache.Cache#setLockLease
   */
  public void setLockLease(int seconds) throws AdminException;
  
  /**
   * Gets the number of seconds a cache
   * {@link com.gemstone.gemfire.cache.Region#get(Object) get} operation
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
   * @see com.gemstone.gemfire.cache.Cache#setSearchTimeout
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
   * @since 4.0
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
   * @since 5.0
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
   * @see com.gemstone.gemfire.cache.Cache#addCacheServer
   *
   * @since 5.7
   */
  public SystemMemberCacheServer addCacheServer()
    throws AdminException;

  /**
   * Returns the cache servers that run in this member's VM.  Note
   * that this list will not be updated until {@link #refresh} is
   * called.
   *
   * @see com.gemstone.gemfire.cache.Cache#getCacheServers
   *
   * @since 5.7
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
   * @since 4.0
   */
  public boolean isServer() throws AdminException;
}

