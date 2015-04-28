/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

/**
 * Describes a cache from a GemFireVM's point of view.
 * 
 * @author Darrel Schneider
 * @since 3.5
 */
public interface CacheInfo {
  /**
   * Returns the name of this cache.
   */
  public String getName();
  /**
   * Return and Id that can be used to determine what instance of the
   * cache the information pertains to.
   */
  public int getId();
  /**
   * Returns true if the current cache is closed.
   */
  public boolean isClosed();

  /**
   * Gets the number of seconds a cache operation will wait to obtain
   * a distributed lock lease.
   */
  public int getLockTimeout();
  
  /**
   * Gets the length, in seconds, of distributed lock leases obtained
   * by this cache.
   */
  public int getLockLease();
  
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
   * Returns the number of seconds that have elapsed since
   * this cache was created. Returns <code>-1</code>
   * if this cache is closed.
   */
  public int getUpTime();

  /**
   * Returns the names of all the root regions currently in this cache.
   * Returns null if cache is closed.
   */
  public java.util.Set getRootRegionNames();

  /**
   * Returns the statistic resource that contains this cache's performance
   * statistics.
   * Returns null if the cache is closed;
   */
  public StatResource getPerfStats();

  /**
   * Forces this instance to be closed. Does not actually close the cache.
   */
  public void setClosed();

  /**
   * Returns the ids of all of the bridge servers that are associated
   * with this cache.
   *
   * @since 4.0
   */
  public int[] getBridgeServerIds();

  /**
   * Returns whether or not this is a cache "server"
   *
   * @since 4.0
   */
  public boolean isServer();
}
