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

package org.apache.geode.cache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.SubscriptionNotEnabledException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.snapshot.RegionSnapshotService;

/**
 * Manages subregions and cached data. Each region can contain multiple subregions and entries for
 * data. Regions provide a hierarchical name space within the cache. Also, a region can be used to
 * group cached objects for management purposes.
 * <p>
 *
 * The Region interface basically contains two set of APIs: Region management APIs; and
 * (potentially) distributed operations on entries. Non-distributed operations on entries are
 * provided by the inner interface, {@link org.apache.geode.cache.Region.Entry}.
 * <p>
 *
 * Each {@link org.apache.geode.cache.Cache} defines a single top region called the root region.
 * User applications can use the root region to create subregions for isolated name space and object
 * grouping.
 * <p>
 *
 * A region's name can be any String except that it should not contain the region name separator, a
 * forward slash (/).
 *
 * <code>Region</code>s can be referenced by a relative path name from any region higher in the
 * hierarchy in {@link #getSubregion}. You can get the relative path from the root region with
 * {@link #getFullPath}. The name separator is used to concatenate all the region names together
 * from the root, starting with the root's subregions.
 * <p>
 * Relative region names can provide a convenient method to locate a subregion directly from some
 * higher region. For example, a region structure is as follows: a region named
 * <i>3rd_level_region</i> has parent region <i>2nd_level_region</i>; region <i>2nd_level_region</i>
 * in turn has parent region <i>1st_level_region</i>; and region <i>1st_level_region</i> is a child
 * of the root region. Then,the user can get the region <i>3rd_level_region</i> from the root region
 * by issuing:
 *
 * <pre>
 *  <code>
 *  region3 = root.getSubregion("1st_level_region/2nd_level_region/3rd_level_region");
 *  </code>
 * </pre>
 *
 * or the user can get the region <i>3rd_level_region</i> from region <i>1st_level_region</i> by
 * issuing
 *
 * <pre>
 *  <code>
 *  region3 = region1.getSubregion("2nd_level_region/3rd_level_region");
 *  </code>
 * </pre>
 * <p>
 * Region entries are identified by their key. Any Object can be used as a key as long as the key
 * Object is region-wide unique and implements both the equals and hashCode methods. For regions
 * with distributed scope, the key must also be Serializable.
 * <p>
 * Regions and their entries can be locked. The <code>Lock</code> obtained from
 * {@link #getRegionDistributedLock} is a distributed lock on the entire Region, and the
 * <code>Lock</code> obtained from {@link Region#getDistributedLock} is a distributed lock on the
 * individual entry.
 *
 * <p>
 * If the scope is <code>Scope.GLOBAL</code>, the methods that modify, destroy, or invalidate the
 * entries in this region will also get a distributed lock. See the documentations for
 * {@link #getDistributedLock} and {@link #getRegionDistributedLock} for details on the implicit
 * locking that occurs for regions with <code>Scope.GLOBAL</code>.
 * <p>
 * Unless otherwise specified, all of these methods throw a <code>CacheClosedException</code> if the
 * Cache is closed at the time of invocation, or a <code>RegionDestroyedException</code> if this
 * region has been destroyed. Serializability Requirements for arguments: Several methods in the
 * region API take parameters such as key, value and callback parameters.All of these parameters are
 * typed as objects. For distributed regions, keys, values and callback parameters have to be
 * serializable Failure to meet these serialization requirements causes API methods to throw
 * IllegalArgumentException.
 * <p>
 * Implementation of the java.util.concurrent.ConcurrentMap interface was added in version 6.5.
 * These methods give various levels of concurrency guarantees based on the scope and data policy of
 * the region. They are implemented in the peer cache and client/server cache but are disallowed in
 * peer Regions having NORMAL or EMPTY data policies.
 * <p>
 * The semantics of the ConcurrentMap methods on a Partitioned Region are consistent with those
 * expected on a ConcurrentMap. In particular multiple writers in different JVMs of the same key in
 * the same Partitioned Region will be done atomically.
 * <p>
 * The same is true for a region with GLOBAL scope. All operations will be done atomically since a
 * distributed lock will be held while the operation is done.
 * <p>
 * The same is true for a region with LOCAL scope. All ops will be done atomically since the
 * underlying map is a concurrent hash map and no distribution is involved.
 * <p>
 * For peer REPLICATE and PRELOADED regions atomicity is limited to threads in the JVM the operation
 * starts in. There is no coordination with other members of the system unless the operation is
 * performed in a transaction.
 * <p>
 * For client server regions the atomicity is determined by the scope and data policy of the server
 * region as described above. The operation is actually performed on the server as described above.
 * Clients will always send the ConcurrentMap operation to the server and the result returned by the
 * ConcurrentMap method in client will reflect what was done on the server. Same goes for any
 * CacheListener called on the client. Any local state on the client will be updated to be
 * consistent with the state change made on the server.
 * <p>
 *
 * @see RegionAttributes
 * @see AttributesFactory
 * @see AttributesMutator
 * @see Region.Entry
 * @since GemFire 2.0
 */

public interface Region<K, V> extends ConcurrentMap<K, V> {
  /** The region name separator character. */
  char SEPARATOR_CHAR = '/';

  /** The region name separator character, represented as a string for convenience. */
  String SEPARATOR = "/";

  /**
   * Returns the name of this region. A region's name can be any non-empty String providing it does
   * not contain the name separator, a forward slash (/). If this is the root region, returns
   * "root".
   * <p>
   * Does not throw a <code>CacheClosedException</code> or a <code>RegionDestroyedException</code>.
   *
   * @return the name of this region
   */
  String getName();

  /**
   * Returns the full path of this region starting with a forward slash, followed by the root,
   * including every subregion in the path to this region. The path separator is a forward slash.
   * <p>
   * Does not throw a <code>CacheClosedException</code> or a <code>RegionDestroyedException</code>.
   *
   * @return the full path of this region
   */
  String getFullPath();

  /**
   * Gets the parent region of this region. If this region is a root region (i.e. has no parents),
   * returns null.
   * <p>
   * Does not throw a <code>CacheClosedException</code> or a <code>RegionDestroyedException</code>.
   *
   * @return the parent region which contains this region; null, if this region is the root region
   * @see Region#createSubregion(String, RegionAttributes) createSubregion
   */
  <PK, PV> Region<PK, PV> getParentRegion();

  /**
   * Returns the <code>RegionAttributes</code> for this region. This object is backed by this
   * region, so if attributes are modified using this region's <code>AttributesMutator</code>, this
   * <code>RegionAttributes</code> object will immediately reflect the change.
   * <p>
   * Does not throw a <code>CacheClosedException</code> or a <code>RegionDestroyedException</code>.
   *
   * @return the <code>RegionAttributes</code> of this region
   * @see Region#createSubregion(String, RegionAttributes)
   * @see AttributesMutator
   * @see AttributesFactory
   * @see #getAttributesMutator
   */
  RegionAttributes<K, V> getAttributes();

  /**
   * Returns a mutator object used for modifying this region's attributes after region creation.
   * Note that some attributes are immutable after region creation.
   *
   * @return the <code>AttributesMutator</code> object
   * @see #getAttributes
   */
  AttributesMutator<K, V> getAttributesMutator();

  /**
   * Returns the <code>CacheStatistics</code> for this region.
   *
   * @return the <code>CacheStatistics</code> of this region
   * @throws StatisticsDisabledException if statistics have been disabled for this region
   * @throws UnsupportedOperationException If the region is a partitioned region
   */
  CacheStatistics getStatistics() throws StatisticsDisabledException;

  /**
   * Invalidates this region. Invalidation cascades to all entries and subregions. After the
   * <code>invalidateRegion</code>, this region and the entries in it still exist. To remove all the
   * entries and this region, <code>destroyRegion</code> should be used. The region invalidate will
   * be distributed to other caches if the scope is not <code>Scope.LOCAL</code>.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for <code>Scope.GLOBAL</code>
   * @see CacheListener#afterRegionInvalidate
   */
  void invalidateRegion() throws TimeoutException;

  /**
   * Invalidates this region. The invalidation will cascade to all the subregions and cached
   * entries. After the <code>invalidateRegion</code>, the region and the entries in it still exist.
   * In order to remove all the entries and the region, <code>destroyRegion</code> should be used.
   * The region invalidate will be distributed to other caches if the scope is not
   * <code>Scope.LOCAL</code>.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. Can be null. Should be serializable.
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for <code>Scope.GLOBAL</code>
   * @throws TimeoutException if timed out getting distributed lock for <code>Scope.GLOBAL</code>
   * @throws IllegalArgumentException if aCallbackArgument is not serializable
   * @see CacheListener#afterRegionInvalidate
   */
  void invalidateRegion(Object aCallbackArgument) throws TimeoutException;



  /**
   * Invalidates this region in the local cache only. Invalidation cascades to all entries and
   * subregions. After the <code>invalidateRegion</code>, this region and the entries in it still
   * exist. To remove all the entries and this region, destroyRegion should be used.
   *
   * Does not update any <code>CacheStatistics</code>.
   *
   * @throws IllegalStateException if this region is distributed and
   *         {@link DataPolicy#withReplication replicated}
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @see CacheListener#afterRegionInvalidate
   */
  void localInvalidateRegion();

  /**
   * Invalidates this region in the local cache only, and provides a user-defined argument to the
   * <code>CacheListener</code>. The invalidation will cascade to all the subregions and cached
   * entries. After the <code>invalidateRegion</code>, the region and the entries in it still exist.
   * In order to remove all the entries and the region, destroyRegion should be used.
   *
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. Can be null.
   * @throws IllegalStateException if the region is distributed and
   *         {@link DataPolicy#withReplication replicated}
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @see CacheListener#afterRegionInvalidate
   */

  void localInvalidateRegion(Object aCallbackArgument);


  /**
   * Destroys the whole region. Destroy cascades to all entries and subregions. After the destroy,
   * this region object can not be used any more and any attempt to use this region object will get
   * <code>RegionDestroyedException</code>. The region destroy will be distributed to other caches
   * if the scope is not <code>Scope.LOCAL</code>.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @throws CacheWriterException if a CacheWriter aborts the operation; if this occurs some
   *         subregions may have already been successfully destroyed.
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @see CacheListener#afterRegionDestroy
   * @see CacheWriter#beforeRegionDestroy
   */
  void destroyRegion() throws CacheWriterException, TimeoutException;

  /**
   * Destroys the whole region and provides a user-defined parameter object to any
   * <code>CacheWriter</code> invoked in the process. Destroy cascades to all entries and
   * subregions. After the destroy, this region object can not be used any more. Any attempt to use
   * this region object will get a <code>RegionDestroyedException</code> exception. The region
   * destroy is distributed to other caches if the scope is not <code>Scope.LOCAL</code>.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. Can be null. Should be serializable.
   * @throws CacheWriterException if a CacheWriter aborts the operation; if this occurs some
   *         subregions may have already been successfully destroyed.
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws IllegalArgumentException if aCallbackArgument is not serializable
   * @see CacheListener#afterRegionDestroy
   * @see CacheWriter#beforeRegionDestroy
   */
  void destroyRegion(Object aCallbackArgument) throws CacheWriterException, TimeoutException;

  /**
   * Destroys the whole region in the local cache only. No <code>CacheWriter</code> is invoked.
   * Destroy cascades to all entries and subregions. After the destroy, this region object can not
   * be used any more and any attempt to use this region object will get
   * {@link RegionDestroyedException} exception. This operation is not distributed to any other
   * cache.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @see CacheListener#afterRegionDestroy
   */
  void localDestroyRegion();

  /**
   * Destroys the whole region in the local cache only, and provides a user-defined argument to a
   * <code>CacheListener</code> if any.
   *
   * No <code>CacheWriter</code> is invoked. Destroy will cascade to all the entries and subregions.
   * After the destroy, this region object can not be used any more. Any attempt to use this region
   * object will get {@link RegionDestroyedException} exception.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        call. Can be null.
   * @see CacheListener#afterRegionDestroy
   */
  void localDestroyRegion(Object aCallbackArgument);

  /**
   * Does a localDestroyRegion, but leaves behind the disk files if this is a region with
   * persistBackup set to true. Calls {@link CacheListener#afterRegionDestroy} on each cache
   * listener on the closed region(s). Also calls {@link CacheCallback#close} on each callback on
   * the closed region(s).
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @see Region#localDestroyRegion()
   * @see CacheListener#afterRegionDestroy
   */
  void close();

  /**
   * Obtains the snapshot service to allow the cache data to be imported or exported.
   *
   * @return the snapshot service for the region
   */
  RegionSnapshotService<K, V> getSnapshotService();

  /**
   * Saves the data in this region in a snapshot file. The data is a "concurrent" snapshot in that
   * modifications to the region while the snapshot is being written are not guaranteed to be
   * included or excluded from the snapshot. In other words, if there are concurrent modifications
   * to the region while the snapshot is being written, the snapshot may not reflect a consistent
   * state of the entire region at any moment in time.
   *
   * @param outputStream the output stream to write to
   * @throws IOException if encountered while writing the file
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @see #loadSnapshot
   *
   * @deprecated as of 7.0 use {@link #getSnapshotService()}
   */
  void saveSnapshot(OutputStream outputStream) throws IOException;

  /**
   * Loads data from a file that was previously created with the saveSnapshot method. This method
   * essentially destroys the region and automatically recreates it with the data in the snapshot.
   * Any current data in the region is lost, replaced with the data in the snapshot file. Causes
   * this region and all other regions with the same name in remote caches in this distributed
   * system to be reinitialized: remote regions are cleared of all data and distributed
   * {@link DataPolicy#withReplication replicated} remote regions will do a new getInitialImage
   * operation to get the data from this snapshot. Any existing references to this region or any
   * region that is reinitialized in this manner become unusable in that any subsequent methods
   * invoked on those references will throw a RegionReinitializedException (which is a subclass of
   * RegionDestroyedException).
   * <p>
   *
   * In order to continue working with this region, a new reference needs to be acquired using
   * Cache#getRegion or Region#getSubregion (which will block until reinitialization is complete).
   * <p>
   *
   * NOTE: SUBREGIONS ARE DESTROYED. Since loading a snapshot effectively destroys the region and
   * recreates it, all subregions of this region in this cache as well as other remote caches in the
   * same distributed system are destroyed.
   * <p>
   *
   * If any error occurs while loading the snapshot, this region is destroyed and threads in remote
   * caches that are attempting to get a reference to this region will get null instead of a region
   * reference.
   *
   * The only callbacks that are invoked are called for the destroyRegion operation, i.e. the
   * CacheWriter and the close methods on callbacks. CacheListeners and other callback objects have
   * their close() methods called when the region is destroyed, and then those same callback objects
   * will be reinstalled on the new region. Therefore, the callback objects should be able to handle
   * a close() followed by events associated with the newly created region.
   *
   * @param inputStream the inputStream to load the snapshot from
   * @throws ClassNotFoundException if a class cannot be found while loading data
   * @throws IOException if error encountered while reading file.
   * @throws CacheWriterException if a CacheWriter aborts the destroyRegion operation; if this
   *         occurs some subregions may have already been successfully destroyed.
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws UnsupportedOperationException If the region is a partitioned region
   *
   * @see RegionReinitializedException
   *
   * @deprecated as of 7.0 use {@link #getSnapshotService()}
   */
  void loadSnapshot(InputStream inputStream)
      throws IOException, ClassNotFoundException, CacheWriterException, TimeoutException;

  /**
   * Returns a subregion with the specified name or null if doesn't exist. The name is relative from
   * this region, so it can be either a simple region name or a relative region path. If
   * <code>regionName</code> is the empty string, then this region itself is returned.
   *
   * @param path the path to the subregion
   * @return a subregion with the specified relative path from this region, or null if it doesn't
   *         exist
   * @throws IllegalArgumentException if path starts with a forward slash or
   * @see Region Region
   * @see Region#getFullPath
   */
  <SK, SV> Region<SK, SV> getSubregion(String path);


  /**
   * Creates a subregion with the specified name and <code>RegionAttributes</code>. The name must
   * not contain a region name separator. If the subregion is a distributed
   * {@link DataPolicy#withReplication replicated} region, it will be initialized with data from all
   * other caches in this distributed system that have the same region.
   *
   * <p>
   * Updates the {@link CacheStatistics#getLastAccessedTime} and
   * {@link CacheStatistics#getLastModifiedTime} for this region.
   *
   * @param subregionName the subregion name
   * @param aRegionAttributes the RegionAttributes to be used for the subregion
   * @return a subregion with the specified name
   * @throws IllegalArgumentException if aRegionAttributes is null or if regionName is null, the
   *         empty string, or contains a '/'
   * @throws IllegalStateException If the supplied RegionAttributes violate the
   *         <a href="AttributesFactory.html#creationConstraints">region creation constraints</a>
   *         with a region of the same name in another cache in the distributed system or with this
   *         (parent) region.
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws RegionExistsException if a subregion by the specified name already exists
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @see AttributesFactory
   * @see Region#getFullPath
   * @deprecated as of 7.0 use {@link RegionFactory#createSubregion(Region, String)} or
   *             {@link ClientRegionFactory#createSubregion(Region, String)}.
   */
  <SK, SV> Region<SK, SV> createSubregion(String subregionName,
      RegionAttributes<SK, SV> aRegionAttributes) throws RegionExistsException, TimeoutException;

  /**
   * Returns a Set of all subregions. If the recursive parameter is set to true, this call will
   * recursively collect all subregions contained in this region. Otherwise, this call will only
   * return the Set of direct subregions.
   *
   * <p>
   * This <code>Set</code> is unmodifiable. It is backed by this region. Synchronization is not
   * necessary to access or iterate over this set. No <code>ConcurrentModificationException</code>s
   * will be thrown, but subregions may be added or removed while a thread is iterating. Iterators
   * are intended to be used by one thread at a time. If a stable "snapshot" view of the set is
   * required, then call one of the toArray methods on the set and iterate over the array.
   *
   * @param recursive if false, collects direct subregions only; if true, collects all subregions
   *        recursively
   * @return a Set of subregions
   */
  Set<Region<?, ?>> subregions(boolean recursive);


  /**
   * Returns the <code>Region.Entry</code> for the specified key, or null if it doesn't exist.
   *
   * @param key the key corresponding to the Entry to return
   * @return the Region.Entry for the specified key or null if not found in this region
   * @throws NullPointerException if key is null
   */
  Entry<K, V> getEntry(Object key);

  /**
   * Returns the value associated with the specified key. If the value is not present locally for
   * this entry, a netSearch and/or a CacheLoader may be invoked to get the value, depending on the
   * scope of this region. A netSearch looks for a value in every node of the system that defines
   * this region. A netLoad invokes remote loaders one at a time until one returns a value or throws
   * an exception. If any of these methods successfully retrieves a value than the value is
   * immediately returned.
   * <p>
   * For local scope, a local CacheLoader will be invoked if there is one. For global scope, the
   * order is netSearch, localLoad, netLoad. For any other distributed scope, the order is
   * localLoad, netSearch, netLoad.
   * <p>
   * netSearch and netLoad are never performed more than once, so if a loader attempts to do a
   * netSearch and one was already done, then another one will not be done.
   * <p>
   * The value returned by get is not copied, so multi-threaded applications should not modify the
   * value directly, but should use the update methods.
   * <p>
   * Updates the {@link CacheStatistics#getLastAccessedTime}, {@link CacheStatistics#getHitCount},
   * {@link CacheStatistics#getMissCount}, and {@link CacheStatistics#getLastModifiedTime} (if a new
   * value is loaded) for this region and the entry.
   * <p>
   *
   * If the <code>CacheWriter</code> throws a <code>CacheWriterException</code> when a new value is
   * retrieved from a loader, the value will not be put into the cache (a new entry will not be
   * created) but the get will return the value and not propagate the exception.
   *
   * @param key whose associated value is to be returned. The key Object must implement the equals
   *        and hashCode methods. Note that even though key is typed as "Object" if a CacheLoader is
   *        used then make sure you only pass instances of "K" in this parameter.
   * @return the value with specified key, or null if the value is not found and can't be loaded
   * @throws NullPointerException if the key is null
   * @throws IllegalArgumentException if the key does not meet the serializability requirements
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out doing a {@link Cache#getSearchTimeout search} for
   *         distributed or getting a distributed lock for Scope.GLOBAL
   * @throws CacheLoaderException if a cache loader throws an exception, or if the cache loader
   *         returns an object that is not serializable and this is a distributed region
   * @throws PartitionedRegionStorageException for a partitioned region fails to invoke a
   *         {@link CacheLoader}
   * @see CacheLoader#load
   * @see CacheListener#afterCreate
   * @see CacheListener#afterUpdate
   * @see CacheWriter#beforeCreate
   * @see CacheWriter#beforeUpdate
   */
  V get(Object key) throws CacheLoaderException, TimeoutException;

  /**
   * Returns the value associated with the specified key, passing the callback argument to any cache
   * loaders or cache writers that are invoked in the operation. If the value is not present locally
   * for this entry, a netSearch and/or a CacheLoader may be invoked to get the value, depending on
   * the scope of this region. A netSearch looks for a value in every node of the system that
   * defines this region. A netLoad invokes remote loaders one at a time until one returns a value
   * or throws an exception. If any of these methods successfully retrieves a value than the value
   * is immediately returned.
   * <p>
   * For local scope, a local CacheLoader will be invoked if there is one. For global scope, the
   * order is netSearch, localLoad, netLoad. For any other distributed scope, the order is
   * localLoad, netSearch, netLoad.
   * <p>
   * netSearch and netLoad are never performed more than once, so if a loader attempts to do a
   * netSearch and one was already done, then another one will not be done.
   * <p>
   * The value returned by get is not copied, so multi-threaded applications should not modify the
   * value directly, but should use the update methods.
   * <p>
   * Updates the {@link CacheStatistics#getLastAccessedTime}, {@link CacheStatistics#getHitCount},
   * {@link CacheStatistics#getMissCount}, and {@link CacheStatistics#getLastModifiedTime} (if a new
   * value is loaded) for this region and the entry.
   *
   * If the <code>CacheWriter</code> throws a <code>CacheWriterException</code> when a new value is
   * retrieved from a loader, then the value will not be put into the cache (a new entry will not be
   * created) but the get will return the value and not propagate the exception.
   *
   * @param key whose associated value is to be returned. The key Object must implement the equals
   *        and hashCode methods. Note that even though key is typed as "Object" if a CacheLoader is
   *        used then make sure you only pass instances of "K" in this parameter.
   * @param aCallbackArgument an argument passed into the CacheLoader if loader is used. This same
   *        argument will also be subsequently passed to a CacheWriter if the loader returns a
   *        non-null value to be placed in the cache. Modifications to this argument made in the
   *        CacheLoader will be visible to the CacheWriter even if the loader and the writer are
   *        installed in different cache VMs. It will also be passed to any other callback events
   *        triggered by this method. May be null. Should be serializable.
   * @return the value with specified key, or null if the value is not found and can't be loaded
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if aCallbackArgument is not serializable
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out doing a {@link Cache#getSearchTimeout search} for
   *         distributed or getting a distributed lock for Scope.GLOBAL
   * @throws CacheLoaderException if a cache loader throws an exception, or if the cache loader
   *         returns an object that is not serializable and this is a distributed region
   * @throws PartitionedRegionStorageException for a partitioned region fails to invoke a
   *         {@link CacheLoader}
   * @see RegionAttributes
   * @see CacheLoader#load
   * @see CacheListener#afterCreate
   * @see CacheListener#afterUpdate
   * @see CacheWriter#beforeCreate
   * @see CacheWriter#beforeUpdate
   */
  V get(Object key, Object aCallbackArgument) throws TimeoutException, CacheLoaderException;

  /**
   * Places a new value into an entry in this region with the specified key. If there is already an
   * entry associated with the specified key in this region, the entry's previous value is
   * overwritten.
   *
   * <p>
   * Updates the {@link CacheStatistics#getLastAccessedTime} and
   * {@link CacheStatistics#getLastModifiedTime} for this region and the entry.
   *
   * @param key a key associated with the value to be put into this region. The key object must
   *        implement the equals and hashCode methods.
   * @param value the value to be put into the cache
   * @return the previous value stored locally for the key. If the entry did not exist then
   *         <code>null</code> is returned. If the entry was "invalid" then <code>null</code> is
   *         returned. In some cases <code>null</code> may be returned even if a previous value
   *         exists. If the region is a client proxy then <code>null</code> is returned. If the
   *         region is partitioned and the put is done on a non-primary then <code>null</code> is
   *         returned. If the value is not currently stored in memory but is on disk and if the
   *         region does not have cqs then <code>null</code> is returned.
   * @throws NullPointerException if key is null or if value is null (use invalidate instead), or if
   *         the key or value do not meet serializability requirements
   * @throws ClassCastException if key does not satisfy the keyConstraint
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @throws PartitionedRegionStorageException if the operation could not be completed on a
   *         partitioned region.
   * @throws LowMemoryException if a low memory condition is detected.
   * @see #invalidate(Object)
   * @see CacheLoader#load
   * @see CacheListener#afterCreate
   * @see CacheListener#afterUpdate
   * @see CacheWriter#beforeCreate
   * @see CacheWriter#beforeUpdate
   */
  V put(K key, V value) throws TimeoutException, CacheWriterException;


  /**
   * Places a new value into an entry in this region with the specified key, providing a
   * user-defined parameter object to any <code>CacheWriter</code> invoked in the process. If there
   * is already an entry associated with the specified key in this region, the entry's previous
   * value is overwritten.
   *
   * <p>
   * Updates the {@link CacheStatistics#getLastAccessedTime} and
   * {@link CacheStatistics#getLastModifiedTime} for this region and the entry.
   *
   * @param key a key associated with the value to be put into this region. The key object must
   *        implement the equals and hashCode methods.
   * @param value the value to be put into the cache
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. Can be null. Should be serializable.
   * @return the previous value stored locally for the key. If the entry did not exist then
   *         <code>null</code> is returned. If the entry was "invalid" then <code>null</code> is
   *         returned. In some cases <code>null</code> may be returned even if a previous value
   *         exists. If the region is a client proxy then <code>null</code> is returned. If the
   *         region is off-heap and the old value was stored in off-heap memory then
   *         <code>null</code> is returned. If the region is partitioned and the put is done on a
   *         non-primary then <code>null</code> is returned. If the value is not currently stored in
   *         memory but is on disk and if the region does not have cqs then <code>null</code> is
   *         returned.
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if key, value, or aCallbackArgument do not meet
   *         serializability requirements
   * @throws ClassCastException if key does not satisfy the keyConstraint
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @throws PartitionedRegionStorageException if the operation could not be completed on a
   *         partitioned region.
   * @throws LowMemoryException if a low memory condition is detected.
   * @see #invalidate(Object)
   * @see CacheLoader#load
   * @see CacheListener#afterCreate
   * @see CacheListener#afterUpdate
   * @see CacheWriter#beforeCreate
   * @see CacheWriter#beforeUpdate
   */
  V put(K key, V value, Object aCallbackArgument) throws TimeoutException, CacheWriterException;

  /**
   * Creates a new entry in this region with the specified key and value if and only if an entry
   * does not already exist for the specified key. If an entry already exists for the specified
   * key, throws {@link EntryExistsException}.
   *
   * <p>
   * Updates the {@link CacheStatistics#getLastAccessedTime} and
   * {@link CacheStatistics#getLastModifiedTime} for this region and the entry.
   * <p>
   * If this region has a distributed scope, this operation may cause update events in caches that
   * already have this region with this entry, and it will cause create events in other caches that
   * have {@link InterestPolicy#ALL all events} configured.
   * <p>
   * This operation gets a distributed lock on the entry if the scope is <code>Scope.GLOBAL</code>.
   *
   * @param key the key for which to create the entry in this region
   * @param value the value for the new entry, which may be null meaning the new entry starts as if
   *        it had been locally invalidated
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if the key or value is not serializable and this is a
   *         distributed region
   * @throws ClassCastException if key does not satisfy the keyConstraint
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for <code>Scope.GLOBAL</code>
   * @throws EntryExistsException if an entry with this key already exists
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @throws PartitionedRegionStorageException if the operation could not be completed on a
   *         partitioned region.
   * @throws LowMemoryException if a low memory condition is detected.
   * @see CacheListener#afterCreate
   * @see CacheListener#afterUpdate
   * @see CacheWriter#beforeCreate
   * @see CacheWriter#beforeUpdate
   */
  void create(K key, V value) throws TimeoutException, EntryExistsException, CacheWriterException;

  /**
   * Creates a new entry in this region with the specified key and value, providing a user-defined
   * parameter object to any <code>CacheWriter</code> invoked in the process. If this region has a
   * distributed scope, then the value may be updated subsequently if other caches update the value.
   *
   * <p>
   * Updates the {@link CacheStatistics#getLastAccessedTime} and
   * {@link CacheStatistics#getLastModifiedTime} for this region and the entry.
   * <p>
   * If this region has a distributed scope, this operation may cause update events in caches that
   * already have this region with this entry, and it will cause create events in other caches that
   * have {@link InterestPolicy#ALL all events} configured.
   *
   * @param key the key for which to create the entry in this region
   * @param value the value for the new entry, which may be null meaning the new entry starts as if
   *        it had been locally invalidated.
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. Can be null. Should be serializable.
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if the key, value, or aCallbackArgument do not meet
   *         serializability requirements
   * @throws ClassCastException if key does not satisfy the keyConstraint
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for <code>Scope.GLOBAL</code>
   * @throws EntryExistsException if an entry with this key already exists
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @throws PartitionedRegionStorageException if the operation could not be completed on a
   *         partitioned region.
   * @throws LowMemoryException if a low memory condition is detected.
   * @see CacheListener#afterCreate
   * @see CacheListener#afterUpdate
   * @see CacheWriter#beforeCreate
   * @see CacheWriter#beforeUpdate
   */
  void create(K key, V value, Object aCallbackArgument)
      throws TimeoutException, EntryExistsException, CacheWriterException;

  /**
   * Invalidates the entry with the specified key. Invalidate only removes the value from the entry,
   * the key is kept intact. To completely remove the entry, destroy should be used. The invalidate
   * will be distributed to other caches if the scope is not Scope.LOCAL.
   *
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param key the key of the value to be invalidated
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if the key does not meet serializability requirements
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for <code>Scope.GLOBAL</code>
   * @throws EntryNotFoundException if the entry does not exist in this region
   * @see CacheListener#afterInvalidate
   */
  void invalidate(Object key) throws TimeoutException, EntryNotFoundException;

  /**
   * Invalidates the entry with the specified key, and provides a user-defined argument to the
   * <code>CacheListener</code>. Invalidate only removes the value from the entry, the key is kept
   * intact. To completely remove the entry, destroy should be used.
   *
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param key the key of the value to be invalidated
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. Can be null. Should be serializable.
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if the key or the aCallbackArgument do not meet
   *         serializability requirements
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for <code>Scope.GLOBAL</code>
   * @throws TimeoutException if timed out getting distributed lock for <code>Scope.GLOBAL</code>
   * @throws EntryNotFoundException if this entry does not exist in this region
   * @see CacheListener#afterInvalidate
   */

  void invalidate(Object key, Object aCallbackArgument)
      throws TimeoutException, EntryNotFoundException;

  /**
   * Invalidates the value with the specified key in the local cache only. Invalidate will only
   * remove the value from the entry, the key will be kept intact. In order to completely remove the
   * key, entry and value, destroy should be called.
   *
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param key the key of the value to be invalidated
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if the key does not meet serializability requirements
   * @throws IllegalStateException if this region is distributed and
   *         {@link DataPolicy#withReplication replicated}
   * @throws EntryNotFoundException if the entry does not exist in this region locally
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @throws UnsupportedOperationInTransactionException If called in a transactional context
   * @see CacheListener#afterInvalidate
   */
  void localInvalidate(Object key) throws EntryNotFoundException;

  /**
   * Invalidates the value with the specified key in the local cache only, and provides a
   * user-defined argument to the <code>CacheListener</code>. Invalidate will only remove the value
   * from the entry, the key will be kept intact. In order to completely remove the key, entry and
   * value, destroy should be called.
   *
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param key the key of the value to be invalidated
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. Can be null.
   * @throws NullPointerException if key is null
   * @throws IllegalStateException if this region is distributed and
   *         {@link DataPolicy#withReplication replicated}
   * @throws EntryNotFoundException if this entry does not exist in this region locally
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @throws UnsupportedOperationInTransactionException If called in a transactional context
   * @see CacheListener#afterInvalidate
   */
  void localInvalidate(Object key, Object aCallbackArgument) throws EntryNotFoundException;


  /**
   * Destroys the entry with the specified key. Destroy removes not only the value but also the key
   * and entry from this region. Destroy will be distributed to other caches if the scope is not
   * <code>Scope.LOCAL</code>.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param key the key of the entry
   * @return the previous value stored locally for the key. If the entry was "invalid" then
   *         <code>null</code> is returned. In some cases <code>null</code> may be returned even if
   *         a previous value exists. If the region is a client proxy then <code>null</code> is
   *         returned. If the region is off-heap and the old value was stored in off-heap memory
   *         then <code>null</code> is returned. If the region is partitioned and the destroy is
   *         done on a non-primary then <code>null</code> is returned. If the value is not currently
   *         stored in memory but is on disk and if the region does not have cqs then
   *         <code>null</code> is returned.
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if key does not meet serializability requirements
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws EntryNotFoundException if the entry does not exist in this region
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @see CacheListener#afterDestroy
   * @see CacheWriter#beforeDestroy
   */
  V destroy(Object key) throws TimeoutException, EntryNotFoundException, CacheWriterException;


  /**
   * Destroys the entry with the specified key, and provides a user-defined parameter object to any
   * <code>CacheWriter</code> invoked in the process. Destroy removes not only the value but also
   * the key and entry from this region. Destroy will be distributed to other caches if the scope is
   * not <code>Scope.LOCAL</code>.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param key the key of the entry to destroy
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. Can be null. Should be serializable.
   * @return the previous value stored locally for the key. If the entry was "invalid" then
   *         <code>null</code> is returned. In some cases <code>null</code> may be returned even if
   *         a previous value exists. If the region is a client proxy then <code>null</code> is
   *         returned. If the region is off-heap and the old value was stored in off-heap memory
   *         then <code>null</code> is returned. If the region is partitioned and the destroy is
   *         done on a non-primary then <code>null</code> is returned. If the value is not currently
   *         stored in memory but is on disk and if the region does not have cqs then
   *         <code>null</code> is returned.
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if the key or aCallbackArgument do not meet serializability
   *         requirements
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws EntryNotFoundException if the entry does not exist in this region
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @see CacheListener#afterDestroy
   * @see CacheWriter#beforeDestroy
   */
  V destroy(Object key, Object aCallbackArgument)
      throws TimeoutException, EntryNotFoundException, CacheWriterException;

  /**
   * Destroys the value with the specified key in the local cache only, No <code>CacheWriter</code>
   * is invoked. Destroy removes not only the value but also the key and entry from this region.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param key the key of the entry to destroy
   * @throws NullPointerException if key is null
   * @throws IllegalStateException if this region is distributed and
   *         {@link DataPolicy#withReplication replicated}
   * @throws EntryNotFoundException if the entry does not exist in this region locally
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @throws UnsupportedOperationInTransactionException If called in a transactional context
   * @see CacheListener#afterDestroy
   */
  void localDestroy(Object key) throws EntryNotFoundException;

  /**
   * Destroys the value with the specified key in the local cache only, and provides a user-defined
   * parameter object to the <code>CacheListener</code>, if any. No <code>CacheWriter</code> is
   * invoked. Destroy removes not only the value but also the key and entry from this region.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param key the key of the entry to destroy
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. Can be null.
   * @throws NullPointerException if key is null
   * @throws IllegalStateException if this region is distributed and
   *         {@link DataPolicy#withReplication replicated}
   * @throws EntryNotFoundException if the entry does not exist in this region locally
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @throws UnsupportedOperationInTransactionException If called in a transactional context
   * @see CacheListener#afterDestroy
   */
  void localDestroy(Object key, Object aCallbackArgument) throws EntryNotFoundException;

  /**
   * Returns a set of keys in the region.
   *
   * <p>
   * This <code>Set</code> is unmodifiable. It is backed by this region. Synchronization is not
   * necessary to access or iterate over this set. No <code>ConcurrentModificationException</code>s
   * will be thrown, but keys may be added or removed to this set while a thread is iterating.
   * Iterators are intended to be used by one thread at a time. If a stable "snapshot" view of the
   * set is required, then call one of the toArray methods on the set and iterate over the array. If
   * you need to lock down the region so this set is not modified while it is being accessed, use
   * global scope with a distributed lock.
   *
   *
   * @return a Set of all the keys
   */
  Set<K> keySet();

  /**
   * Returns a Collection of values in this region.
   *
   * <p>
   * This <code>Collection</code> is unmodifiable. It is backed by this region. Synchronization is
   * not necessary to access or iterate over this collection. No
   * <code>ConcurrentModificationException</code>s will be thrown, but values may be added or
   * removed to this collection while a thread is iterating. Iterators are intended to be used by
   * one thread at a time. If a stable "snapshot" view of the collection is required, then call one
   * of the toArray methods on the collection and iterate over the array. If you need to lock down
   * the region so this set is not modified while it is being accessed, use global scope with a
   * distributed lock on the region.
   *
   * <p>
   * Null values are not included in the result collection.
   *
   * @return a Collection of all the objects cached in this region
   */
  Collection<V> values();

  /**
   * Returns the <code>Set</code> of <code>Region.Entry</code> objects in this region. If the
   * recursive parameter is set to true, this call will recursively collect all the entries in this
   * region and its subregions and return them in the Set; if false, it only returns entries
   * directly contained in this region.
   *
   * <p>
   * This <code>Set</code> is unmodifiable. It is backed by this region. Synchronization is not
   * necessary to access or iterate over this set. No <code>ConcurrentModificationException</code>s
   * will be thrown, but entries may be added or removed to this set while a thread is iterating.
   * Iterators are intended to be used by one thread at a time. If a stable "snapshot" view of the
   * set is required, then call one of the toArray methods on the set and iterate over the array. If
   * you need to lock down the region so this set is not modified while it is being accessed, use
   * global scope with a distributed lock.
   *
   * @param recursive if true, this call recursively collects all the entries in this region and its
   *        subregions; if false, it only returns the entries directly contained in this region
   * @return a Set of all the cached objects
   * @see Region.Entry
   */
  Set<Region.Entry<?, ?>> entrySet(boolean recursive);

  /**
   * Returns the <code>Cache</code> associated with this region.
   * <p>
   * Does not throw a <code>CacheClosedException</code> if the Cache is closed.
   *
   * @return the Cache
   * @deprecated as of 6.5 use {@link #getRegionService()} instead.
   */
  @Deprecated
  Cache getCache();

  /**
   * Returns the <code>cache</code> associated with this region.
   * <p>
   * Does not throw a <code>CacheClosedException</code> if the cache is closed.
   *
   * @return the cache
   * @since GemFire 6.5
   */
  RegionService getRegionService();

  /**
   * Returns the application-defined object associated with this region. GemFire does not use this
   * object for any purpose.
   *
   * @return the user attribute object or null if it has not been set
   */
  Object getUserAttribute();

  /**
   * Sets the application-defined object associated with this region. GemFire does not use this
   * object for any purpose.
   *
   * @param value the application-defined object
   */
  void setUserAttribute(Object value);

  /**
   * Returns whether this region has been destroyed.
   *
   * <p>
   * Does not throw a <code>RegionDestroyedException</code> if this region has been destroyed.
   *
   * @return true if this region has been destroyed
   */
  boolean isDestroyed();

  /**
   * Returns whether there is a valid (non-null) value present for the specified key. This method is
   * equivalent to:
   *
   * <pre>
   * Entry e = getEntry(key);
   * return e != null && e.getValue() != null;
   * </pre>
   *
   * @param key the key to check for a valid value
   * @return true if there is an entry in this region for the specified key and it has a valid value
   */
  boolean containsValueForKey(Object key);

  /**
   * Returns whether the specified key currently exists in this region. This method is equivalent to
   * <code>getEntry(key) != null</code>.
   *
   * @param key the key to check for an existing entry
   * @return true if there is an entry in this region for the specified key
   */
  boolean containsKey(Object key);


  /**
   * For {@link Scope#GLOBAL} regions, gets a <em>distributed</em> lock on this whole region. This
   * region lock cannot be acquired until all other caches release both region locks and any entry
   * locks they hold in this region. Likewise, new entry locks cannot be acquired until outstanding
   * region locks are released. The only place that a region distributed lock is acquired
   * automatically is during region creation for distributed {@link DataPolicy#withReplication
   * replicated} regions when they acquire their initial data.
   * <p>
   * The operations invalidateRegion and destroyRegion do <em>not</em> automatically acquire a
   * distributed lock at all, so it is possible for these operations to cause an entry to be
   * invalidated or the region to be destroyed even if a distributed lock is held on an entry. If an
   * application requires all entry level locks to be released when a region is destroyed or
   * invalidated as a whole, then it can call this method explicitly to get a ock on the entire
   * region before calling invalidateRegion or destroyRegion.
   * <p>
   * See {@link #getDistributedLock} for the list of operations that automatically acquire
   * distributed entry locks for regions with global scope.
   * <p>
   * Note that Region locks are potentially very expensive to acquire.
   *
   * @return a <code>Lock</code> used for acquiring a distributed lock on the entire region
   * @throws IllegalStateException if the scope of this region is not global
   * @throws UnsupportedOperationException If the region is a partitioned region
   */
  Lock getRegionDistributedLock() throws IllegalStateException;

  /**
   * For {@link Scope#GLOBAL} regions, gets a <em>distributed</em> lock on the entry with the
   * specified key. Use of this <code>Lock</code> enables an application to synchronize operations
   * on entries at a higher level than provided for by {@link Scope#GLOBAL}. This is the same lock
   * that GemFire uses internally for operations that modify the cache with global scope, so this
   * lock can be used for high-level synchronization with other caches that have this region with
   * global scope. For example, if an application needs to get two values out of a region with
   * global scope and guarantee that the first value is not modified before the second value is
   * retrieved, it can use this lock in the following manner:
   *
   * <pre>
   * Lock entry1Lock = myRegion.getDistributedLock(key1);
   * Lock entry2Lock = myRegion.getDistributedLock(key2);
   * entry1Lock.lock();
   * entry2Lock.lock();
   * try {
   *   Object val1 = myRegion.get(key1);
   *   Object val2 = myRegion.get(key2);
   *   // do something with val1 and val2
   * } finally {
   *   entry2Lock.unlock();
   *   entry1Lock.unlock();
   * }
   * </pre>
   *
   * You can also get a lock on an entry that does not exist in the local cache. Doing so guarantees
   * that no other cache with the same region using global scope or using the same lock will create
   * or update that entry while you have the lock.
   *
   * When a region has global scope, the following operations automatically acquire a distributed
   * lock on an entry: <code>create</code>, <code>put</code>, <code>destroy</code>,
   * <code>invalidate</code>, and <code>get</code> that causes a loader to be invoked.
   *
   * @return a <code>Lock</code> used for acquiring a distributed lock on an entry
   * @throws IllegalStateException if the scope of this region is not global
   * @throws NullPointerException if key is null
   * @throws UnsupportedOperationException If the region is a partitioned region
   */
  Lock getDistributedLock(Object key) throws IllegalStateException;

  /**
   * Initiates a flush to asynchronously write unwritten region entries to disk.
   *
   * @throws IllegalStateException If this region is not configured to write to disk
   * @throws DiskAccessException If problems are encounter while writing to disk
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @see AttributesFactory#setPersistBackup
   * @since GemFire 3.2
   * @deprecated use {@link DiskStore#flush} instead.
   */
  @Deprecated
  void writeToDisk();

  /**
   * Determines whether there is a value in this <code>Region</code> that matches the given
   * <code>queryPredicate</code>. Filters the values of this region using the predicate given as a
   * string with the syntax of the <code>WHERE</code> clause of the query language. The predefined
   * variable <code>this</code> may be used inside the predicate to denote the current value being
   * filtered.
   *
   * @param queryPredicate A query language boolean query predicate.
   *
   * @return <code>true</code> if there is a value in region that matches the predicate, otherwise
   *         <code>false</code>.
   *
   * @throws QueryInvalidException If predicate does not correspond to valid query language syntax.
   *
   * @see QueryService
   *
   * @since GemFire 4.0
   */
  boolean existsValue(String queryPredicate) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException;

  /**
   * Filters the values of this region using the <code>queryPredicate</code>. The queryPredicate
   * should follow the syntax of query WHERE clause.
   *
   * When executed from a client, this method always runs on the server. However application should
   * use QueryService to execute queries.
   *
   *
   * @see Pool#getQueryService
   * @see Cache#getQueryService()
   *
   * @param queryPredicate A query language boolean query predicate.
   *
   * @return A <code>SelectResults</code> containing the values of this <code>Region</code> that
   *         match the <code>predicate</code>.
   *
   * @throws QueryInvalidException If exception occurs during query compilation or processing.
   *
   * @see QueryService
   *
   * @since GemFire 4.0
   */
  <E> SelectResults<E> query(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException;

  /**
   * Selects the single value in this <code>Region</code> that matches the given query
   * <code>predicate</code>. Filters the values of this region using the predicate given as a string
   * with the syntax of the where clause of the query language. The predefined variable
   * <code>this</code> may be used inside the predicate to denote the element currently being
   * filtered.
   *
   * @param queryPredicate A query language boolean query predicate.
   *
   * @return The single element that evaluates to true for the predicate. If no value in this
   *         <code>Region</code> matches the predicate, <code>null</code> is returned.
   *
   * @throws QueryInvalidException If predicate does not correspond to valid query language syntax.
   * @throws FunctionDomainException If more than one element evaluates to true.
   *
   * @see QueryService
   * @since GemFire 4.0
   */
  Object selectValue(String queryPredicate) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException;

  /**
   * Asks the region to start writing to a new oplog (if persistence/overflow is turned on). The old
   * one will be asynchronously compressed if compaction is set to true. If the region is not
   * persistent/overflow no change in the region will be reflected. The new log will be created in
   * the next available directory with free space. If there is no directory with free space
   * available and compaction is set to false, then a <code>DiskAccessException</code> saying that
   * the disk is full will be thrown. If compaction is true, the application will wait for the other
   * oplogs to be compressed and more space to be created.
   *
   * @since GemFire 5.1
   * @deprecated use {@link DiskStore#forceRoll} instead.
   */
  @Deprecated
  void forceRolling();

  /**
   * Specifies this member to become the grantor for this region's lock service. The grantor will be
   * the lock authority which is responsible for handling all lock requests for this service. Other
   * members will request locks from this member. Locking for this member will be optimal as it will
   * not require messaging to acquire a given lock.
   * <p>
   * Calls to this method will block until grantor authority has been transferred to this member.
   * <p>
   * If another member later calls <code>becomeLockGrantor</code>, that member will transfer grantor
   * authority from this member to itself. Multiple calls to this operation will have no effect
   * unless another member has transferred grantor authority, in which case, this member will
   * transfer grantor authority back to itself.
   * <p>
   * This region's scope must be <code>Scope.GLOBAL</code> to become a lock grantor.
   * <p>
   * This operation should not be invoked repeatedly in an application. It is possible to create a
   * lock service and have two or more members endlessly calling becomeLockGrantor to transfer
   * grantorship back and forth.
   *
   * @throws IllegalStateException if scope is not GLOBAL
   * @throws UnsupportedOperationException If the region is a partitioned region
   *
   * @since GemFire 4.0
   */
  void becomeLockGrantor();

  /**
   * Removes all local entries from this region. This is not a distributed operation. This operation
   * is not allowed on replicated regions.
   *
   * @since GemFire 5.0
   * @throws UnsupportedOperationException If the region is a replicated region
   * @throws UnsupportedOperationException If the region is a partitioned region
   * @see CacheListener#afterRegionClear
   *
   */
  void localClear();

  ////// Map API's ////
  /**
   * Removes all entries from this region. Clear will be distributed to other caches if the scope is
   * not <code>Scope.LOCAL</code>.
   * <p>
   *
   * @since GemFire 5.0
   * @see java.util.Map#clear()
   * @see CacheListener#afterRegionClear
   * @see CacheWriter#beforeRegionClear
   * @throws UnsupportedOperationException If the region is a partitioned region
   */
  void clear();

  /**
   * Returns true if this region maps one or more keys to the specified value. More formally,
   * returns true if and only if this region contains at least one entry to a value v such that
   * (value==null ? v==null : value.equals(v)). This operation is not distributed and only the
   * current region will be checked for this value.
   *
   * @since GemFire 5.0
   * @see java.util.Map#containsValue(Object)
   */
  boolean containsValue(Object value);

  /**
   * Returns the <code>Set</code> of <code>Region.Entry</code> objects in this region.
   *
   * <p>
   * This <code>Set</code> is unmodifiable. It is backed by this region. Synchronization is not
   * necessary to access or iterate over this set. No <code>ConcurrentModificationException</code>s
   * will be thrown, but entries may be added or removed to this set while a thread is iterating.
   * Iterators are intended to be used by one thread at a time. If a stable "snapshot" view of the
   * set is required, then call one of the toArray methods on the set and iterate over the array. If
   * you need to lock down the region so this set is not modified while it is being accessed, use
   * global scope with a distributed lock.
   *
   * A remove called on an entry via the iterator will result in an UnsupportedOperationException
   *
   * The Region.Entry obtained via the iterator is backed by the region. If a setValue on that entry
   * is called, it will be similar in effect as calling a put on that key.
   *
   * @return a Set of all the Region.Entry instances in this region locally
   * @since GemFire 5.0
   * @see java.util.Map#entrySet()
   */
  Set<Map.Entry<K, V>> entrySet(); // @todo darrel: should be Region.Entry

  /**
   * Returns true if this region contains no entries.
   *
   * @since GemFire 5.0
   * @see java.util.Map#isEmpty()
   * @return true if this region contains no entries.
   */
  boolean isEmpty();

  /**
   * Copies all of the entries from the specified map to this region. The effect of this call is
   * equivalent to that of calling {@link #put(Object, Object)} on this region once for each entry
   * in the specified map. This operation will be distributed to other caches if the scope is not
   * <code>Scope.LOCAL</code>.
   *
   * @param map the key/value pairs to put in this region.
   * @since GemFire 5.0
   * @see java.util.Map#putAll(Map map)
   * @throws LowMemoryException if a low memory condition is detected.
   */
  void putAll(Map<? extends K, ? extends V> map);

  /**
   * Copies all of the entries from the specified map to this region. The effect of this call is
   * equivalent to that of calling {@link #put(Object, Object, Object)} on this region once for each
   * entry in the specified map. This operation will be distributed to other caches if the scope is
   * not <code>Scope.LOCAL</code>.
   *
   * @param map the key/value pairs to put in this region.
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. May be null. Must be serializable if this operation is distributed.
   * @since GemFire 8.1
   * @see java.util.Map#putAll(Map map)
   * @throws LowMemoryException if a low memory condition is detected.
   */
  void putAll(Map<? extends K, ? extends V> map, Object aCallbackArgument);

  /**
   * Removes all of the entries for the specified keys from this region. The effect of this call is
   * equivalent to that of calling {@link #destroy(Object)} on this region once for each key in the
   * specified collection. If an entry does not exist that key is skipped; EntryNotFoundException is
   * not thrown. This operation will be distributed to other caches if the scope is not
   * <code>Scope.LOCAL</code>.
   *
   * @param keys the keys to remove from this region.
   * @since GemFire 8.1
   * @see Region#destroy(Object)
   */
  void removeAll(Collection<? extends K> keys);

  /**
   * Removes all of the entries for the specified keys from this region. The effect of this call is
   * equivalent to that of calling {@link #destroy(Object, Object)} on this region once for each key
   * in the specified collection. If an entry does not exist that key is skipped;
   * EntryNotFoundException is not thrown. This operation will be distributed to other caches if the
   * scope is not <code>Scope.LOCAL</code>.
   *
   * @param keys the keys to remove from this region.
   * @param aCallbackArgument a user-defined parameter to pass to callback events triggered by this
   *        method. May be null. Must be serializable if this operation is distributed.
   * @since GemFire 8.1
   * @see Region#destroy(Object, Object)
   */
  void removeAll(Collection<? extends K> keys, Object aCallbackArgument);

  /**
   * Gets values for all the keys in the input Collection. If a given key does not exist in the
   * region then that key's value in the returned map will be <code>null</code>.
   * <p>
   * Note that the keys collection should extend K since a load may be done and the key in that case
   * will be stored in the region. The keys parameter was not changed to extend K for backwards
   * compatibility.
   *
   * @param keys A Collection of keys
   * @return A Map of values for the input keys
   *
   * @since GemFire 5.7
   */
  Map<K, V> getAll(Collection<?> keys);

  /**
   * Gets and returns values for all the keys in the input Collection. If a given key does not exist
   * in the region then that key's value in the returned map will be <code>null</code>.
   *
   * @param <T> the type of the keys passed to getAll
   * @param keys A Collection of keys
   * @param aCallbackArgument an argument passed into the CacheLoader if a loader is used. This same
   *        argument will also be subsequently passed to a CacheWriter if the loader returns a
   *        non-null value to be placed in the cache. Modifications to this argument made in the
   *        CacheLoader will be visible to the CacheWriter even if the loader and the writer are
   *        installed in different cache VMs. It will also be passed to any other callback events
   *        triggered by this method. May be null. Must be serializable if this operation is
   *        distributed.
   * @return A Map of values for the input keys
   *
   * @since GemFire 8.1
   */
  <T extends K> Map<T, V> getAll(Collection<T> keys, Object aCallbackArgument);


  /**
   * Removes the entry with the specified key. The operation removes not only the value but also the
   * key and entry from this region. Remove will be distributed to other caches if the scope is not
   * <code>Scope.LOCAL</code>.
   * <p>
   * Does not update any <code>CacheStatistics</code>.
   *
   * @param key the key of the entry
   * @return <code>null</code> is returned if an entry for key does not exist otherwise the value
   *         that was stored locally for the removed entry is returned. If the entry was "invalid"
   *         then <code>null</code> is returned. In some cases <code>null</code> may be returned
   *         even if a previous value exists. If the region is a client proxy then <code>null</code>
   *         is returned. If the region is off-heap and the old value was stored in off-heap memory
   *         then <code>null</code> is returned. If the region is partitioned and the remove is done
   *         on a non-primary then <code>null</code> is returned. If the value is not currently
   *         stored in memory but is on disk and if the region does not have cqs then
   *         <code>null</code> is returned.
   * @throws NullPointerException if key is null
   * @throws IllegalArgumentException if key does not meet serializability requirements
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @see Region#destroy(Object)
   * @see CacheListener#afterDestroy
   * @see CacheWriter#beforeDestroy
   * @see java.util.Map#remove(Object)
   *
   * @since GemFire 5.0
   */
  V remove(Object key);

  /**
   * Returns the number of entries present in this region.
   *
   * For {@link DataPolicy#PARTITION}, this is a distributed operation that returns the number of
   * entries present in entire region.
   *
   * For all other types of regions, it returns the number of entries present locally, and it is not
   * a distributed operation.
   *
   * @since GemFire 5.0
   * @see java.util.Map#size()
   * @return the number of entries present in this region
   */
  int size();

  /**
   *
   * Compares the specified object with this region for equality. Regions are only equal to
   * themselves (identity based) so {@link Object#equals} is used. Note that some other class that
   * implements <code>Map</code> may say that it is equal to an instance of Region (since Region
   * implements Map) even though Region will never say that it is equal to that instance.
   *
   * @param other Object object to be compared against the this
   * @return <tt>true</tt> if the specified object is equal to this region.
   * @see Object#equals
   */
  boolean equals(Object other);

  /**
   * Returns the hash code value for this region. The hash code of a region is based on identity and
   * uses {@link Object#hashCode}.
   *
   * @return the hash code value for this region.
   * @see Object#hashCode()
   */
  int hashCode();

  /**
   * Sends a request to the CacheServer to register interest in a key for this client. Updates to
   * this key by other clients will be pushed to this client by the CacheServer. This method is
   * currently supported only on clients in a client server topology. This key is first locally
   * cleared from the client and current value for this key is inserted into the local cache before
   * this call returns.
   *
   * @param key The key on which to register interest.
   *
   *        <p>
   *        ###Deprecated behavior###
   *        </p>
   *        <p>
   *        The following <code>List</code> and
   *        'ALL_KEYS' behavior is now deprecated. As an alternative, please use
   *        </p>
   *        <p>
   *        {@link #registerInterestForKeys(Iterable, InterestResultPolicy)}
   *        </p>
   *        <p>
   *        {@link #registerInterestForAllKeys(InterestResultPolicy)}
   *        </p>
   *
   *        <p>
   *        If the key is a <code>List</code>, then all the keys in the
   *        <code>List</code> will be registered. The key can also be the special token 'ALL_KEYS',
   *        which will register interest in all keys in the region. In effect, this will cause an
   *        update to any key in this region in the CacheServer to be pushed to the client.
   *        </p>
   *
   *        <p>
   *        <i>Using 'ALL_KEYS' is the same as calling {@link #registerInterestRegex(String)} with
   *        ".*" as the argument. This means that all keys of any type are pushed to the client and
   *        inserted into the local cache.</i>
   *        </p>
   *        ###End of deprecation###
   *
   *        <p>
   *        This method uses the default <code>InterestResultPolicy</code>.
   *        </p>
   *
   *        <p>
   *        If you locally-destroy a key and your region has concurrency-checks-enabled turned off
   *        you will not receive invalidation events from your interest subscription for that key.
   *        When concurrency-checks-enabled is turned on GemFire will accept invalidation and
   *        deliver these events to your client cache.
   *        </p>
   *
   * @see InterestResultPolicy
   *
   * @since GemFire 4.2
   *
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   */
  void registerInterest(K key);

  /**
   * Sends a request to the CacheServer to register interest in a key for this client. Updates to
   * this key by other clients will be pushed to this client by the CacheServer. This method is
   * currently supported only on clients in a client server topology. This key is first locally
   * cleared from the client and current value for this key is inserted into the local cache before
   * this call returns. (if requested).
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param key The key on which to register interest.
   *
   *        <p>
   *        ###Deprecated behavior###
   *        </p>
   *        <p>
   *        The following <code>List</code> and
   *        'ALL_KEYS' behavior is now deprecated. As an alternative, please use
   *        </p>
   *        <p>
   *        {@link #registerInterestForKeys(Iterable, InterestResultPolicy)}
   *        </p>
   *        <p>
   *        {@link #registerInterestForAllKeys(InterestResultPolicy)}
   *        </p>
   *
   *        <p>
   *        If the key is a <code>List</code>, then all the keys in the
   *        <code>List</code> will be registered. The key can also be the special token 'ALL_KEYS',
   *        which will register interest in all keys in the region. In effect, this will cause an
   *        update to any key in this region in the CacheServer to be pushed to the client.
   *        </p>
   *
   *        <p>
   *        <i>Using 'ALL_KEYS' is the same as calling {@link #registerInterestRegex(String)} with
   *        ".*" as the argument. This means that all keys of any type are pushed to the client and
   *        inserted into the local cache.</i>
   *        </p>
   *        ###End of deprecation###
   *
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subcriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   *
   * @since GemFire 4.2.3
   */
  void registerInterest(K key, InterestResultPolicy policy);


  /**
   * Sends a request to the CacheServer to register interest in all keys for this client. Updates to
   * any key by other clients will be pushed to this client by the CacheServer. This method is
   * currently supported only on clients in a client server topology. The keys are first locally
   * cleared from the client and current value for the keys are inserted into the local cache before
   * this call returns.
   *
   * @since Geode 1.5
   *
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   */
  default void registerInterestForAllKeys() {
    registerInterestRegex(".*");
  }


  /**
   * Sends a request to the CacheServer to register interest in for all keys for this client.
   * Updates to any key by other clients will be pushed to this client by the CacheServer. This
   * method is currently supported only on clients in a client server topology. The keys are first
   * locally cleared from the client and current value for the keys are inserted into the local
   * cache before this call returns. (if requested).
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subcriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   *
   * @since Geode 1.5
   */
  default void registerInterestForAllKeys(InterestResultPolicy policy) {
    registerInterestRegex(".*", policy);
  }

  /**
   * Sends a request to the CacheServer to register interest in all keys for this client. Updates to
   * any key by other clients will be pushed to this client by the CacheServer. This method is
   * currently supported only on clients in a client server topology. The keys are first locally
   * cleared from the client and the current value for the keys are inserted into the local cache
   * before this call returns (if requested).
   *
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @param isDurable true if the register interest is durable
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   *
   * @since Geode 1.5
   */
  default void registerInterestForAllKeys(InterestResultPolicy policy, boolean isDurable) {
    registerInterestRegex(".*", policy, isDurable);
  }


  /**
   * Sends a request to the CacheServer to register interest in all keys for this client. Updates to
   * any key by other clients will be pushed to this client by the CacheServer. This method is
   * currently supported only on clients in a client server topology. The keys are first locally
   * cleared from the client and the current value for the keys are inserted into the local cache
   * before this call returns (if requested).
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @param isDurable true if the register interest is durable
   * @param receiveValues defaults to true. set to false to receive create or update events as
   *        invalidates similar to notify-by-subscription false.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   *
   * @since Geode 1.5
   */
  default void registerInterestForAllKeys(InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) {
    registerInterestRegex(".*", policy, isDurable, receiveValues);
  }

  /**
   * Sends a request to the CacheServer to register interest for all key in the iterable for this
   * client. Updates to any of the keys in the iterable by other clients will be pushed to this
   * client by the CacheServer. This method is currently supported only on clients in a client
   * server topology. The keys are first locally cleared from the client and current value for the
   * keys are inserted into the local cache before this call returns. (if requested).
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param iterable The <code>Iterable</code> of keys on which to register interest.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subcriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   **
   * @since Geode 1.5
   */
  default void registerInterestForKeys(Iterable<K> iterable) {
    iterable.forEach(k -> registerInterest(k));
  }

  /**
   * Sends a request to the CacheServer to register interest for all key in the iterable for this
   * client. Updates to any of the keys in the iterable by other clients will be pushed to this
   * client by the CacheServer. This method is currently supported only on clients in a client
   * server topology. The keys are first locally cleared from the client and current value for the
   * keys are inserted into the local cache before this call returns. (if requested).
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param iterable The <code>Iterable</code> of keys on which to register interest.
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subcriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   *
   * @since Geode 1.5
   */
  default void registerInterestForKeys(Iterable<K> iterable, InterestResultPolicy policy) {
    iterable.forEach(k -> registerInterest(k, policy));
  }

  /**
   * Sends a request to the CacheServer to register interest for all key in the iterable for this
   * client. Updates to any of the keys in the iterable by other clients will be pushed to this
   * client by the CacheServer. This method is currently supported only on clients in a client
   * server topology. The keys are first locally cleared from the client and current value for the
   * keys are inserted into the local cache before this call returns. (if requested).
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param iterable The <code>Iterable</code> of keys on which to register interest.
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @param isDurable true if the register interest is durable
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subcriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   *
   * @since Geode 1.5
   */
  default void registerInterestForKeys(Iterable<K> iterable, InterestResultPolicy policy,
      boolean isDurable) {
    iterable.forEach(k -> registerInterest(k, policy, isDurable));
  }

  /**
   * Sends a request to the CacheServer to register interest for all key in the iterable for this
   * client. Updates to any of the keys in the iterable by other clients will be pushed to this
   * client by the CacheServer. This method is currently supported only on clients in a client
   * server topology. The keys are first locally cleared from the client and current value for the
   * keys are inserted into the local cache before this call returns. (if requested).
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param iterable The <code>Iterable</code> of keys on which to register interest.
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @param isDurable true if the register interest is durable
   * @param receiveValues defaults to true. set to false to receive create or update events as
   *        invalidates similar to notify-by-subscription false.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subcriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   *
   * @since Geode 1.5
   */
  default void registerInterestForKeys(Iterable<K> iterable, InterestResultPolicy policy,
      boolean isDurable, boolean receiveValues) {
    iterable.forEach(k -> registerInterest(k, policy, isDurable, receiveValues));
  }


  /**
   * Sends a request to the CacheServer to register interest in a regular expression pattern for
   * this client. Updates to any keys of type {@link String} satisfying this regular expression by
   * other clients will be pushed to this client by the CacheServer. This method is currently
   * supported only on clients in a client server topology. These keys are first locally cleared
   * from the client and the current values for keys of type {@link String} that satisfy the regular
   * expression are inserted into the local cache before this call returns.
   *
   * <p>
   * Note that if the <code>regex</code> is <code>".*"</code> then all keys of any type will be
   * pushed to the client.
   * <p>
   * This method uses the default <code>InterestResultPolicy</code>.
   * </p>
   *
   * <p>
   * The regular expression string is compiled using the {@link java.util.regex.Pattern} class.
   * </p>
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param regex The regular expression on which to register interest.
   *
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   * @see InterestResultPolicy
   * @see java.util.regex.Pattern
   *
   * @since GemFire 4.2.3
   */
  void registerInterestRegex(String regex);

  /**
   * Sends a request to the CacheServer to register interest in a regular expression pattern for
   * this client. Updates to any keys of type {@link String} satisfying this regular expression by
   * other clients will be pushed to this client by the CacheServer. This method is currently
   * supported only on clients in a client server topology. These keys are first locally cleared
   * from the client and the current values for keys of type {@link String} that satisfy the regular
   * expression are inserted into the local cache before this call returns.
   *
   * <p>
   * The regular expression string is compiled using the {@link java.util.regex.Pattern} class.
   * </p>
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param regex The regular expression on which to register interest.
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   * @see java.util.regex.Pattern
   *
   * @since GemFire 4.2.3
   */
  void registerInterestRegex(String regex, InterestResultPolicy policy);

  /**
   * Sends a request to the CacheServer to unregister interest in a key for this client. Updates to
   * this key by other clients will be no longer be pushed to this client by the CacheServer. This
   * method is currently supported only on clients in a client server topology.
   *
   * @param key The key on which to register interest. If the key is a <code>List</code>, then all
   *        the keys in the <code>List</code> will be unregistered.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   *
   * @since GemFire 4.2
   */
  void unregisterInterest(K key);

  /**
   * Sends a request to the CacheServer to unregister interest in a regular expression pattern for
   * this client. Updates to any keys satisfying this regular expression by other clients will no
   * longer be pushed to this client by the CacheServer. This method is currently supported only on
   * clients in a client server topology.
   *
   * @param regex The regular expression on which to unregister interest.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   *
   * @since GemFire 4.2.3
   */
  void unregisterInterestRegex(String regex);

  /**
   * Returns the list of keys on which this client is interested and will be notified of changes.
   * This method is currently supported only on clients in a client server topology.
   *
   * @return The list of keys on which this client is interested
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   *
   * @since GemFire 4.2
   */
  List<K> getInterestList();


  /**
   * Sends a request to the CacheServer to register interest in a key for this client. Updates to
   * this key by other clients will be pushed to this client by the CacheServer. This method is
   * currently supported only on clients in a client server topology. This key is first locally
   * cleared from the client and the current value for this key is inserted into the local cache
   * before this call returns.
   *
   * @param key The key on which to register interest.
   *
   *        <p>
   *        ###Deprecated behavior###
   *        </p>
   *        <p>
   *        The following <code>List</code> and
   *        'ALL_KEYS' behavior is now deprecated. As an alternative, please use
   *        </p>
   *        <p>
   *        {@link #registerInterestForKeys(Iterable, InterestResultPolicy)}
   *        </p>
   *        <p>
   *        {@link #registerInterestForAllKeys(InterestResultPolicy)}
   *        </p>
   *
   *        <p>
   *        If the key is a <code>List</code>, then all the keys in the
   *        <code>List</code> will be registered. The key can also be the special token 'ALL_KEYS',
   *        which will register interest in all keys in the region. In effect, this will cause an
   *        update to any key in this region in the CacheServer to be pushed to the client.
   *        </p>
   *
   *        <p>
   *        <i>Using 'ALL_KEYS' is the same as calling {@link #registerInterestRegex(String)} with
   *        ".*" as the argument. This means that all keys of any type are pushed to the client and
   *        inserted into the local cache.</i>
   *        </p>
   *        ###End of deprecation###
   *
   *        <p>
   *        This method uses the default <code>InterestResultPolicy</code>.
   *        </p>
   *
   *        <p>
   *        If you locally-destroy a key and your region has concurrency-checks-enabled turned off
   *        you will not receive invalidation events from your interest subscription for that key.
   *        When concurrency-checks-enabled is turned on GemFire will accept invalidation and
   *        deliver these events to your client cache.
   *        </p>
   *
   * @param isDurable true if the register interest is durable
   *
   * @see InterestResultPolicy
   *
   * @since GemFire 5.5
   *
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   */
  void registerInterest(K key, boolean isDurable);

  /**
   * Sends a request to the CacheServer to register interest in a key for this client. Updates to
   * this key by other clients will be pushed to this client by the CacheServer. This method is
   * currently supported only on clients in a client server topology. This key is first locally
   * cleared from the client and the current value for this key is inserted into the local cache
   * before this call returns.
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param key The key on which to register interest.
   *
   *        <p>
   *        ###Deprecated behavior###
   *        </p>
   *        <p>
   *        The following <code>List</code> and
   *        'ALL_KEYS' behavior is now deprecated. As an alternative, please use
   *        </p>
   *        <p>
   *        {@link #registerInterestForKeys(Iterable, InterestResultPolicy)}
   *        </p>
   *        <p>
   *        {@link #registerInterestForAllKeys(InterestResultPolicy)}
   *        </p>
   *
   *        <p>
   *        If the key is a <code>List</code>, then all the keys in the
   *        <code>List</code> will be registered. The key can also be the special token 'ALL_KEYS',
   *        which will register interest in all keys in the region. In effect, this will cause an
   *        update to any key in this region in the CacheServer to be pushed to the client.
   *        </p>
   *
   *        <p>
   *        <i>Using 'ALL_KEYS' is the same as calling {@link #registerInterestRegex(String)} with
   *        ".*" as the argument. This means that all keys of any type are pushed to the client and
   *        inserted into the local cache.</i>
   *        </p>
   *        ###End of deprecation###
   *
   *        <p>
   *        This method uses the default <code>InterestResultPolicy</code>.
   *        </p>
   * @param isDurable true if the register interest is durable
   *
   * @param receiveValues defaults to true. set to false to receive create or update events as
   *        invalidates similar to notify-by-subscription false.
   *
   * @see InterestResultPolicy
   *
   * @since GemFire 6.0.3
   *
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   */
  void registerInterest(K key, boolean isDurable, boolean receiveValues);

  /**
   * Sends a request to the CacheServer to register interest in a key for this client. Updates to
   * this key by other clients will be pushed to this client by the CacheServer. This method is
   * currently supported only on clients in a client server topology. This key is first locally
   * cleared from the client and the current value for this key is inserted into the local cache
   * before this call returns (if requested).
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param key The key on which to register interest.
   *
   *        <p>
   *        ###Deprecated behavior###
   *        </p>
   *        <p>
   *        The following <code>List</code> and
   *        'ALL_KEYS' behavior is now deprecated. As an alternative, please use
   *        </p>
   *        <p>
   *        {@link #registerInterestForKeys(Iterable, InterestResultPolicy)}
   *        </p>
   *        <p>
   *        {@link #registerInterestForAllKeys(InterestResultPolicy)}
   *        </p>
   *
   *        <p>
   *        If the key is a <code>List</code>, then all the keys in the
   *        <code>List</code> will be registered. The key can also be the special token 'ALL_KEYS',
   *        which will register interest in all keys in the region. In effect, this will cause an
   *        update to any key in this region in the CacheServer to be pushed to the client.
   *        </p>
   *
   *        <p>
   *        <i>Using 'ALL_KEYS' is the same as calling {@link #registerInterestRegex(String)} with
   *        ".*" as the argument. This means that all keys of any type are pushed to the client and
   *        inserted into the local cache.</i>
   *        </p>
   *        ###End of deprecation###
   *
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @param isDurable true if the register interest is durable
   * @param receiveValues defaults to true. set to false to receive create or update events as
   *        invalidates similar to notify-by-subscription false.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   *
   * @since GemFire 6.0.3
   */
  void registerInterest(K key, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues);

  /**
   * Sends a request to the CacheServer to register interest in a key for this client. Updates to
   * this key by other clients will be pushed to this client by the CacheServer. This method is
   * currently supported only on clients in a client server topology. This key is first locally
   * cleared from the client and the current value for this key is inserted into the local cache
   * before this call returns (if requested).
   *
   * @param key The key on which to register interest.
   *
   *        <p>
   *        ###Deprecated behavior###
   *        </p>
   *        <p>
   *        The following <code>List</code> and
   *        'ALL_KEYS' behavior is now deprecated. As an alternative, please use
   *        </p>
   *        <p>
   *        {@link #registerInterestForKeys(Iterable, InterestResultPolicy)}
   *        </p>
   *        <p>
   *        {@link #registerInterestForAllKeys(InterestResultPolicy)}
   *        </p>
   *
   *        <p>
   *        If the key is a <code>List</code>, then all the keys in the
   *        <code>List</code> will be registered. The key can also be the special token 'ALL_KEYS',
   *        which will register interest in all keys in the region. In effect, this will cause an
   *        update to any key in this region in the CacheServer to be pushed to the client.
   *        </p>
   *
   *        <p>
   *        <i>Using 'ALL_KEYS' is the same as calling {@link #registerInterestRegex(String)} with
   *        ".*" as the argument. This means that all keys of any type are pushed to the client and
   *        inserted into the local cache.</i>
   *        </p>
   *        ###End of deprecation###
   *
   *        <p>
   *        If you locally-destroy a key and your region has concurrency-checks-enabled turned off
   *        you will not receive invalidation events from your interest subscription for that key.
   *        When concurrency-checks-enabled is turned on GemFire will accept invalidation and
   *        deliver these events to your client cache.
   *        </p>
   *
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @param isDurable true if the register interest is durable
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   *
   * @since GemFire 5.5
   */
  void registerInterest(K key, InterestResultPolicy policy, boolean isDurable);

  /**
   * Sends a request to the CacheServer to register interest in a regular expression pattern for
   * this client. Updates to any keys of type {@link String} satisfying this regular expression by
   * other clients will be pushed to this client by the CacheServer. This method is currently
   * supported only on clients in a client server topology. These keys are first locally cleared
   * from the client and the current values for keys of type {@link String} that satisfy the regular
   * expression are inserted into the local cache before this call returns.
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * <p>
   * Note that if the <code>regex</code> is <code>".*"</code> then all keys of any type will be
   * pushed to the client.
   * <p>
   * This method uses the default <code>InterestResultPolicy</code>.
   * </p>
   *
   * <p>
   * The regular expression string is compiled using the {@link java.util.regex.Pattern} class.
   * </p>
   *
   * @param regex The regular expression on which to register interest.
   *
   * @param isDurable true if the register interest is durable
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   * @see InterestResultPolicy
   * @see java.util.regex.Pattern
   *
   * @since GemFire 5.5
   */
  void registerInterestRegex(String regex, boolean isDurable);

  /**
   * Sends a request to the CacheServer to register interest in a regular expression pattern for
   * this client. Updates to any keys of type {@link String} satisfying this regular expression by
   * other clients will be pushed to this client by the CacheServer. This method is currently
   * supported only on clients in a client server topology. These keys are first locally cleared
   * from the client and the current values for keys of type {@link String} that satisfy the regular
   * expression are inserted into the local cache before this call returns.
   *
   * <p>
   * Note that if the <code>regex</code> is <code>".*"</code> then all keys of any type will be
   * pushed to the client.
   * <p>
   * This method uses the default <code>InterestResultPolicy</code>.
   * </p>
   *
   * <p>
   * The regular expression string is compiled using the {@link java.util.regex.Pattern} class.
   * </p>
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param regex The regular expression on which to register interest.
   *
   * @param isDurable true if the register interest is durable
   * @param receiveValues defaults to true. set to false to receive create or update events as
   *        invalidates similar to notify-by-subscription false.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   * @see InterestResultPolicy
   * @see java.util.regex.Pattern
   *
   * @since GemFire 6.0.3
   */
  void registerInterestRegex(String regex, boolean isDurable, boolean receiveValues);

  /**
   * Sends a request to the CacheServer to register interest in a regular expression pattern for
   * this client. Updates to any keys of type {@link String} satisfying this regular expression by
   * other clients will be pushed to this client by the CacheServer. This method is currently
   * supported only on clients in a client server topology. These keys are first locally cleared
   * from the client and the current values for keys of type {@link String} that satisfy the regular
   * expression are inserted into the local cache before this call returns.
   *
   * <p>
   * The regular expression string is compiled using the {@link java.util.regex.Pattern} class.
   * </p>
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param regex The regular expression on which to register interest.
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @param isDurable true if the register interest is durable
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   * @see java.util.regex.Pattern
   *
   * @since GemFire 5.5
   */
  void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable);

  /**
   * Sends a request to the CacheServer to register interest in a regular expression pattern for
   * this client. Updates to any keys of type {@link String} satisfying this regular expression by
   * other clients will be pushed to this client by the CacheServer. This method is currently
   * supported only on clients in a client server topology. These keys are first locally cleared
   * from the client and the current values for keys of type {@link String} that satisfy the regular
   * expression are inserted into the local cache before this call returns.
   *
   * <p>
   * The regular expression string is compiled using the {@link java.util.regex.Pattern} class.
   * </p>
   *
   * <p>
   * If you locally-destroy a key and your region has concurrency-checks-enabled turned off you will
   * not receive invalidation events from your interest subscription for that key. When
   * concurrency-checks-enabled is turned on GemFire will accept invalidation and deliver these
   * events to your client cache.
   * </p>
   *
   * @param regex The regular expression on which to register interest.
   * @param policy The interest result policy. This can be one of:
   *        <ul>
   *        <li>InterestResultPolicy.NONE - does not initialize the local cache</li>
   *        <li>InterestResultPolicy.KEYS - initializes the local cache with the keys satisfying the
   *        request</li>
   *        <li>InterestResultPolicy.KEYS_VALUES - initializes the local cache with the keys and
   *        current values satisfying the request</li>
   *        </ul>
   * @param isDurable true if the register interest is durable
   * @param receiveValues defaults to true. set to false to receive create or update events as
   *        invalidates similar to notify-by-subscription false.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   * @throws SubscriptionNotEnabledException if the region's pool does not have subscriptions
   *         enabled.
   * @throws UnsupportedOperationException if the region is a replicate with distributed scope.
   *
   * @see InterestResultPolicy
   * @see java.util.regex.Pattern
   *
   * @since GemFire 6.0.3
   */
  void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues);

  /**
   * Returns the list of regular expresssions on which this client is interested and will be
   * notified of changes. This method is currently supported only on clients in a client server
   * topology.
   *
   * @return The list of regular expresssions on which this client is interested
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   *
   * @since GemFire 4.2.3
   */
  List<String> getInterestListRegex();

  /**
   * Returns a set of keys in the region on the server.
   *
   * @return a Set of all the keys in the region on the server
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   *
   * @since GemFire 5.0.2
   */
  Set<K> keySetOnServer();

  /**
   * Returns whether the specified key currently exists in this region on the server.
   *
   * @param key the key to check for an existing entry
   * @return true if there is an entry in this region for the specified key on the server
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   *
   * @since GemFire 5.0.2
   */
  boolean containsKeyOnServer(Object key);

  /**
   * Returns the number of entries present in this region on the server. Entries stored in this
   * client region are ignored.
   *
   * @return the number of entries present in this region on the server.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   *
   * @since Geode 1.2.0
   */
  int sizeOnServer();

  /**
   * Returns true if this region contains no entries on the server.
   *
   * @return true if this region contains no entries on the server.
   * @throws UnsupportedOperationException if the region is not configured with a pool name.
   *
   * @since Geode 1.2.0
   */
  boolean isEmptyOnServer();

  /**
   * If the specified key is not already associated with a value, associate it with the given value.
   * This is equivalent to
   *
   * <pre>
   * if (!map.containsKey(key))
   *   return map.put(key, value);
   * else
   *   return map.get(key);
   * </pre>
   *
   * except that the action is performed atomically.
   *
   * <p>
   * ConcurrentMap operations are supported on partitioned and replicated regions and in client
   * caches. They are also supported on non-empty local regions.
   * </p>
   * <p>
   * Please read the notes on ConcurrentMap operations in the javadoc for Region.
   * </p>
   * <p>
   * Region allows the value parameter to be null, which will create an invalid entry.
   * </p>
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the specified key, or <tt>null</tt> if there was no
   *         mapping for the key. (A <tt>null</tt> return can also indicate that the map previously
   *         associated <tt>null</tt> with the key, if the implementation supports null values.)
   * @throws UnsupportedOperationException if the <tt>put</tt> operation is not supported by this
   *         map
   * @throws ClassCastException if the class of the specified key or value prevents it from being
   *         stored in this map
   * @throws NullPointerException if the specified key or value is null, and this map does not
   *         permit null keys or values
   * @throws IllegalArgumentException if some property of the specified key or value prevents it
   *         from being stored in this map
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @throws PartitionedRegionStorageException if the operation could not be completed on a
   *         partitioned region.
   * @throws LowMemoryException if a low memory condition is detected.
   * @since GemFire 6.5
   *
   */
  V putIfAbsent(K key, V value);

  /**
   * Removes the entry for a key only if currently mapped to a given value. This is equivalent to
   *
   * <pre>
   * if (map.containsKey(key) &amp;&amp; map.get(key).equals(value)) {
   *   map.remove(key);
   *   return true;
   * } else
   *   return false;
   * </pre>
   *
   * except that the action is performed atomically.
   * <p>
   * As of 8.5, if value is an Array then Arrays.equals or Arrays.deepEquals is used instead of
   * Object.equals.
   * <p>
   * ConcurrentMap operations are supported on partitioned and replicated regions and in client
   * caches. They are also supported on non-empty local regions.
   * <p>
   * <p>
   * Please read the notes on ConcurrentMap operations in the javadoc for Region.
   * </p>
   * <p>
   * Region allows the value parameter to be null, which will match an invalid entry.
   * </p>
   *
   * @param key key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return <tt>true</tt> if the value was removed
   * @throws UnsupportedOperationException if the <tt>remove</tt> operation is not supported by this
   *         map
   * @throws ClassCastException if the key or value is of an inappropriate type for this map
   *         (optional)
   * @throws NullPointerException if the specified key or value is null, and this map does not
   *         permit null keys or values (optional)
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @throws PartitionedRegionStorageException if the operation could not be completed on a
   *         partitioned region.
   * @throws LowMemoryException if a low memory condition is detected.
   * @since GemFire 6.5
   */
  boolean remove(Object key, Object value);

  /**
   * Replaces the entry for a key only if currently mapped to a given value. This is equivalent to
   *
   * <pre>
   * if (map.containsKey(key) &amp;&amp; map.get(key).equals(oldValue)) {
   *   map.put(key, newValue);
   *   return true;
   * } else
   *   return false;
   * </pre>
   *
   * except that the action is performed atomically.
   * <p>
   * As of 8.5, if value is an Array then Arrays.equals or Arrays.deepEquals is used instead of
   * Object.equals.
   * <p>
   * ConcurrentMap operations are supported on partitioned and replicated regions and in client
   * caches. They are also supported on non-empty local regions.
   * </p>
   * <p>
   * Please read the notes on ConcurrentMap operations in the javadoc for Region.
   * </p>
   * <p>
   * Region allows the oldValue parameter to be null, which will match an invalid entry.
   * </p>
   *
   * @param key key with which the specified value is associated
   * @param oldValue value expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   * @return <tt>true</tt> if the value was replaced
   * @throws UnsupportedOperationException if the <tt>replace</tt> operation is not supported by
   *         this map
   * @throws ClassCastException if the class of a specified key or value prevents it from being
   *         stored in this map
   * @throws NullPointerException if a specified key is null, and this map does not permit null keys
   * @throws IllegalArgumentException if some property of a specified key or value prevents it from
   *         being stored in this map
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @throws PartitionedRegionStorageException if the operation could not be completed on a
   *         partitioned region.
   * @throws LowMemoryException if a low memory condition is detected.
   * @since GemFire 6.5
   */
  boolean replace(K key, V oldValue, V newValue);

  /**
   * Replaces the entry for a key only if currently mapped to some value. This is equivalent to
   *
   * <pre>
   * if (map.containsKey(key)) {
   *   return map.put(key, value);
   * } else
   *   return null;
   * </pre>
   *
   * except that the action is performed atomically.
   * <p>
   * ConcurrentMap operations are supported on partitioned and replicated regions and in client
   * caches. They are also supported on non-empty local regions.
   * </p>
   * <p>
   * Please read the notes on ConcurrentMap operations in the javadoc for Region.
   * </p>
   *
   * @param key key with which the specified value is associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the specified key, or <tt>null</tt> if there was no
   *         mapping for the key. (A <tt>null</tt> return can also indicate that the map previously
   *         associated <tt>null</tt> with the key, if the implementation supports null values.)
   * @throws UnsupportedOperationException if the <tt>put</tt> operation is not supported by this
   *         map
   * @throws ClassCastException if the class of the specified key or value prevents it from being
   *         stored in this map
   * @throws NullPointerException if the specified key or value is null, and this map does not
   *         permit null keys or values
   * @throws IllegalArgumentException if some property of the specified key or value prevents it
   *         from being stored in this map
   * @throws org.apache.geode.distributed.LeaseExpiredException if lease expired on distributed lock
   *         for Scope.GLOBAL
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws CacheWriterException if a CacheWriter aborts the operation
   * @throws PartitionedRegionStorageException if the operation could not be completed on a
   *         partitioned region.
   * @throws LowMemoryException if a low memory condition is detected.
   * @since GemFire 6.5
   */
  V replace(K key, V value);

  /**
   * A key-value pair containing the cached data in a region. This object's operations (except
   * for{Entry#setValue()}), are not distributed, do not acquire any locks, and do not affect
   * <code>CacheStatistics</code>.
   * <p>
   * Unless otherwise noted, all of these methods throw a <code>CacheClosedException</code> if the
   * Cache is closed at the time of invocation, or an <code>EntryDestroyedException</code> if the
   * entry has been destroyed.
   */
  interface Entry<K, V> extends Map.Entry<K, V> {

    /**
     * Returns the key for this entry.
     *
     * @return the key for this entry
     */
    K getKey();

    /**
     * Returns the value of this entry in the local cache. Does not invoke a
     * <code>CacheLoader</code>, does not do a netSearch, netLoad, etc.
     *
     * @return the value or <code>null</code> if this entry is invalid
     */
    V getValue();

    /**
     * Returns the region that contains this entry.
     *
     * @return the Region that contains this entry
     */
    Region<K, V> getRegion();

    /**
     * This method checks to see if the entry is in the in-process cache, or is in another process.
     * Only Regions with {@link DataPolicy#PARTITION} may return false in response to this query. A
     * non-local Entry will not reflect dynamic changes being made to the cache. For instance, the
     * result of getValue() will not change, even though the cache may have been updated for the
     * corresponding key. To see an updated snapshot of a non-local Entry, you must fetch the entry
     * from the Region again.
     */
    boolean isLocal();

    /**
     * Returns the statistics for this entry.
     *
     * @return the CacheStatistics for this entry
     * @throws StatisticsDisabledException if statistics have been disabled for this region
     */
    CacheStatistics getStatistics();

    /**
     * Returns the user attribute for this entry in the local cache.
     *
     * @return the user attribute for this entry
     */
    Object getUserAttribute();

    /**
     * Sets the user attribute for this entry. Does not distribute the user attribute to other
     * caches.
     *
     * @param userAttribute the user attribute for this entry
     * @return the previous user attribute or null no user attributes has been set for this entry
     */
    Object setUserAttribute(Object userAttribute);

    /**
     * Returns whether this entry has been destroyed.
     * <p>
     * Does not throw a <code>EntryDestroyedException</code> if this entry has been destroyed.
     *
     * @return true if this entry has been destroyed
     */
    boolean isDestroyed();

    /**
     * Sets the value of this entry. It has similar to calling a put on the key of this Entry
     *
     * @param value Object the value to be set
     * @return the previous value object stored locally for this entry. If the entry did not exist
     *         then <code>null</code> is returned. If the entry was "invalid" then <code>null</code>
     *         is returned. In some cases <code>null</code> may be returned even if a previous value
     *         exists. If the region is a client proxy then <code>null</code> is returned. If the
     *         region is off-heap and the old value was stored in off-heap memory then
     *         <code>null</code> is returned. If the region is partitioned and the setValue is done
     *         on a non-primary then <code>null</code> is returned. If the value is not currently
     *         stored in memory but is on disk and if the region does not have cqs then
     *         <code>null</code> is returned.
     * @since GemFire 5.0
     * @see Region#put(Object, Object)
     */
    V setValue(V value);
  }
}
