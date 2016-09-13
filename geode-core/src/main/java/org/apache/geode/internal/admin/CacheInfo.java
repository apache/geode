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

package org.apache.geode.internal.admin;

/**
 * Describes a cache from a GemFireVM's point of view.
 * 
 * @since GemFire 3.5
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
   * {@link org.apache.geode.cache.Region#get(Object) get} operation
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
   * @since GemFire 4.0
   */
  public int[] getBridgeServerIds();

  /**
   * Returns whether or not this is a cache "server"
   *
   * @since GemFire 4.0
   */
  public boolean isServer();
}
