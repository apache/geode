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
package org.apache.geode.cache.client;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.compression.Compressor;

/**
 * A factory for constructing {@link ClientCache client cache} {@link Region
 * regions}. Instances of this interface can be created using region shortcuts
 * by calling
 * {@link ClientCache#createClientRegionFactory(ClientRegionShortcut)} or using
 * named region attributes by calling
 * {@link ClientCache#createClientRegionFactory(String)}.
 * <p>
 * The factory can then be customized using its methods.
 * <p>
 * The final step is to produce a {@link Region} by calling
 * {@link #create(String)}.
 * <p>
 * Client regions may be:
 * <ul>
 * <li>PROXY: which pass through to server and have no local data.
 * <li>CACHING_PROXY: which fetch data from servers and cache it locally.
 * <li>LOCAL: which only have local data; they do not communicate with the
 * servers.
 * </ul>
 * See {@link ClientRegionShortcut} for the shortcuts for these three types of
 * client regions.
 * <p>
 * Example: Create a client region with a CacheListener
 * 
 * <PRE>
 * ClientCache c = new ClientCacheFactory().addLocator(host, port).create();
 * // Create local caching region that is connected to a server side region
 * // Add a cache listener before creating region
 * Region r = c.createClientRegionFactory(CACHING_PROXY).addCacheListener(
 *     myListener).create(&quot;customers&quot;);
 * </PRE>
 * 
 * @since GemFire 6.5
 */

public interface ClientRegionFactory<K,V> {
  /**
   * Adds a cache listener to the end of the list of cache listeners on this factory.
   * @param aListener the cache listener to add
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException if <code>aListener</code> is null
   * @see AttributesFactory#addCacheListener
   */
  public ClientRegionFactory<K,V> addCacheListener(CacheListener<K,V> aListener);

  /**
   * Removes all cache listeners and then adds each listener in the specified array.
   * for the next <code>RegionAttributes</code> created.
   * @param newListeners a possibly null or empty array of listeners to add to this factory.
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException if the <code>newListeners</code> array has a null element
   * @see AttributesFactory#initCacheListeners
   */
  public ClientRegionFactory<K,V> initCacheListeners(CacheListener<K,V>[] newListeners);

  /**
   * Sets the eviction attributes that controls growth of the Region to be created.
   *
   * @param evictionAttributes for the Region to create
   * @return a reference to this ClientRegionFactory object
   */
  public ClientRegionFactory<K,V> setEvictionAttributes(EvictionAttributes evictionAttributes);

  /**
   * Sets the idleTimeout expiration attributes for region entries for the next
   * <code>RegionAttributes</code> created.
   *
   * @param idleTimeout
   *          the idleTimeout ExpirationAttributes for entries in this region
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException
   *           if idleTimeout is null
   * @see AttributesFactory#setEntryIdleTimeout
   */
  public ClientRegionFactory<K,V> setEntryIdleTimeout(ExpirationAttributes idleTimeout);

  /**
   * Sets the custom idleTimeout for the next <code>RegionAttributes</code>
   * created.
   * 
   * @param custom the custom method
   * @return the receiver
   * @see AttributesFactory#setCustomEntryIdleTimeout(CustomExpiry)
   */
  public ClientRegionFactory<K,V> setCustomEntryIdleTimeout(CustomExpiry<K,V> custom);
  
  /**
   * Sets the timeToLive expiration attributes for region entries for the next
   * <code>RegionAttributes</code> created.
   *
   * @param timeToLive
   *          the timeToLive ExpirationAttributes for entries in this region
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException
   *           if timeToLive is null
   * @see AttributesFactory#setEntryTimeToLive
   */
  public ClientRegionFactory<K,V> setEntryTimeToLive(ExpirationAttributes timeToLive);

  /**
   * Sets the custom timeToLive expiration method for the next 
   * <code>RegionAttributes</code> created.
   * @param custom the custom method
   * @return the receiver
   * @see AttributesFactory#setCustomEntryTimeToLive(CustomExpiry)
   */
  public ClientRegionFactory<K,V> setCustomEntryTimeToLive(CustomExpiry<K,V> custom);
  
  /**
   * Sets the idleTimeout expiration attributes for the region itself for the
   * next <code>RegionAttributes</code> created.
   *
   * @param idleTimeout
   *          the ExpirationAttributes for this region idleTimeout
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException
   *           if idleTimeout is null
   * @see AttributesFactory#setRegionIdleTimeout
   */
  public ClientRegionFactory<K,V> setRegionIdleTimeout(ExpirationAttributes idleTimeout);

  /**
   * Sets the timeToLive expiration attributes for the region itself for the
   * next <code>RegionAttributes</code> created.
   *
   * @param timeToLive
   *          the ExpirationAttributes for this region timeToLive
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException
   *           if timeToLive is null
   * @see AttributesFactory#setRegionTimeToLive
   */
  public ClientRegionFactory<K,V> setRegionTimeToLive(ExpirationAttributes timeToLive);

  /**
   * Sets the key constraint for the next <code>RegionAttributes</code>
   * created. Keys in the region will be constrained to this class (or
   * subclass). Any attempt to store a key of an incompatible type in the region
   * will cause a <code>ClassCastException</code> to be thrown.
   *
   * @param keyConstraint
   *          The Class to constrain the keys to, or null if no constraint
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException
   *           if <code>keyConstraint</code> is a class denoting a primitive
   *           type
   * @see AttributesFactory#setKeyConstraint
   */
  public ClientRegionFactory<K,V> setKeyConstraint(Class<K> keyConstraint);

  /**
   * Sets the value constraint for the next <code>RegionAttributes</code>
   * created. Values in the region will be constrained to this class (or
   * subclass). Any attempt to store a value of an incompatible type in the
   * region will cause a <code>ClassCastException</code> to be thrown.
   *
   * @param valueConstraint
   *          The Class to constrain the values to, or null if no constraint
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException
   *           if <code>valueConstraint</code> is a class denoting a primitive
   *           type
   * @see AttributesFactory#setValueConstraint
   */
  public ClientRegionFactory<K,V> setValueConstraint(Class<V> valueConstraint);

  /**
   * Sets the entry initial capacity for the next <code>RegionAttributes</code>
   * created. This value is used in initializing the map that holds the entries.
   *
   * @param initialCapacity
   *          the initial capacity of the entry map
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException if initialCapacity is negative.
   * @see java.util.HashMap
   * @see AttributesFactory#setInitialCapacity
   */
  public ClientRegionFactory<K,V> setInitialCapacity(int initialCapacity);

  /**
   * Sets the entry load factor for the next <code>RegionAttributes</code>
   * created. This value is used in initializing the map that holds the entries.
   *
   * @param loadFactor
   *          the load factor of the entry map
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException
   *           if loadFactor is nonpositive
   * @see java.util.HashMap
   * @see AttributesFactory#setLoadFactor
   */
  public ClientRegionFactory<K,V> setLoadFactor(float loadFactor);

  /**
   * Sets the concurrency level tof the next <code>RegionAttributes</code>
   * created. This value is used in initializing the map that holds the entries.
   *
   * @param concurrencyLevel
   *          the concurrency level of the entry map
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalArgumentException
   *           if concurrencyLevel is nonpositive
   * @see AttributesFactory#setConcurrencyLevel
   */
  public ClientRegionFactory<K,V> setConcurrencyLevel(int concurrencyLevel);

  /**
   * Enables or disabled concurrent modification checks
   * @since GemFire 7.0
   * @param concurrencyChecksEnabled whether to perform concurrency checks on operations
   */
  public void setConcurrencyChecksEnabled(boolean concurrencyChecksEnabled);

  /**
   * Sets the DiskStore name attribute.
   * This causes the region to belong to the DiskStore.
   * @param name the name of the diskstore
   * @return a reference to this ClientRegionFactory object
   * 
   * @see AttributesFactory#setDiskStoreName
   */
  public ClientRegionFactory<K,V> setDiskStoreName(String name);

  /**
   * Sets whether or not the writing to the disk is synchronous.
   * 
   * @param isSynchronous
   *          boolean if true indicates synchronous writes
   * @return a reference to this ClientRegionFactory object
   */
  public ClientRegionFactory<K,V> setDiskSynchronous(boolean isSynchronous);

  /**
   * Sets whether statistics are enabled for this region and its entries.
   *
   * @param statisticsEnabled
   *          whether statistics are enabled
   * @return a reference to this ClientRegionFactory object
   * @see AttributesFactory#setStatisticsEnabled
   */
  public ClientRegionFactory<K,V> setStatisticsEnabled(boolean statisticsEnabled);

  /**
   * Sets cloning on region
   * Note: off-heap regions always behave as if cloning is enabled.
   * @param cloningEnable
   * @return a reference to this ClientRegionFactory object
   * @see AttributesFactory#setCloningEnabled
   */
  public ClientRegionFactory<K,V> setCloningEnabled(boolean cloningEnable);

  /**
   * Sets the pool name attribute.
   * This causes regions that use these attributes
   * to be a client region which communicates with the
   * servers that the connection pool communicates with.
   * <p>The named connection pool must exist on the cache at the time these
   * attributes are used to create a region. See {@link PoolManager#createFactory}
   * for how to create a connection pool.
   * @param poolName the name of the connection pool to use
   * @return a reference to this ClientRegionFactory object
   * @throws IllegalStateException if a cache loader or cache writer has already
   * been set.
   * @see PoolManager
   */
  public ClientRegionFactory<K,V> setPoolName(String poolName);

  /**
   * Set the compressor to be used by this region for compressing
   * region entry values.
   * @param compressor a compressor
   * @return a reference to this RegionFactory instance
   * @since GemFire 8.0
   */
  public ClientRegionFactory<K,V> setCompressor(Compressor compressor);
  
  /**
   * Creates a region in the {@link ClientCache} using
   * the configuration contained in this ClientRegionFactory. Validation of the
   * provided attributes may cause exceptions to be thrown if there are problems
   * with the configuration data.
   *
   * @param name
   *          the name of the region to create
   *
   * @return the region object
   * @throws RegionExistsException
   *           if a region with the given name already exists in this cache
   * @throws CacheClosedException
   *           if the cache is closed
   */
  public Region<K,V> create(String name) throws RegionExistsException;
  /**
   * Creates a sub-region in the {@link ClientCache} using
   * the configuration contained in this ClientRegionFactory. Validation of the
   * provided attributes may cause exceptions to be thrown if there are problems
   * with the configuration data.
   *
   * @param parent
   *          the existing region that will contain the created sub-region
   * @param name
   *          the name of the region to create
   *
   * @return the region object
   * @throws RegionExistsException
   *           if a region with the given name already exists in this cache
   * @throws RegionDestroyedException
   *           if the parent region has been closed or destroyed
   * @throws CacheClosedException
   *           if the cache is closed
   * @since GemFire 7.0
   */
  public Region<K,V> createSubregion(Region<?,?> parent, String name) throws RegionExistsException;
  

}
