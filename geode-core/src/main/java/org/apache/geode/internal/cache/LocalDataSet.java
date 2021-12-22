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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.cache.query.internal.QueryExecutor;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.LocalRegion.IteratorType;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.internal.cache.snapshot.RegionSnapshotServiceImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class LocalDataSet implements Region, QueryExecutor {

  private static final Logger logger = LogService.getLogger();

  private final PartitionedRegion proxy;
  private final Set<Integer> buckets;
  private InternalRegionFunctionContext rfContext;

  public LocalDataSet(PartitionedRegion pr, int[] buckets) {
    proxy = pr;
    this.buckets = BucketSetHelper.toSet(buckets);
  }

  public LocalDataSet(PartitionedRegion pr, Set<Integer> buckets) {
    proxy = pr;
    this.buckets = buckets;
  }

  @Override
  public Set<Region.Entry> entrySet(boolean recursive) {
    return proxy.entrySet(getBucketSet());
  }

  @Override
  public Set<Region.Entry> entrySet() {
    return entrySet(false);
  }

  @Override
  public Collection values() {
    proxy.checkReadiness();
    return proxy.new ValuesSet(getBucketSet());
  }

  public Set keys() {
    return proxy.keySet(getBucketSet());
  }

  @Override
  public Set keySet() {
    return keys();
  }

  public Collection localValues() {
    return new LocalEntriesSet(IteratorType.VALUES);
  }

  public Set<Region.Entry> localEntrySet() {
    return new LocalEntriesSet(IteratorType.ENTRIES);
  }

  public Set<Region.Entry> localKeys() {
    return new LocalEntriesSet(IteratorType.KEYS);
  }

  /**
   * This instance method was added so that unit tests could mock it
   */
  int getHashKey(Operation op, Object key, Object value, Object callbackArg) {
    return PartitionedRegionHelper.getHashKey(proxy, op, key, value, callbackArg);
  }

  private boolean isInDataSet(Object key, Object callbackArgument) {
    int bucketId = getHashKey(Operation.CONTAINS_KEY, key, null, callbackArgument);
    Integer bucketIdInt = Integer.valueOf(bucketId);
    return buckets.contains(bucketIdInt);
  }

  public InternalRegionFunctionContext getFunctionContext() {
    return rfContext;
  }

  public void setFunctionContext(InternalRegionFunctionContext fContext) {
    rfContext = fContext;
  }

  @Override
  public SelectResults query(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    QueryService qs = getCache().getLocalQueryService();
    DefaultQuery query = (DefaultQuery) qs
        .newQuery("select * from " + getFullPath() + " this where " + queryPredicate);
    final ExecutionContext executionContext = new QueryExecutionContext(null, getCache(), query);
    Object[] params = null;
    return (SelectResults) executeQuery(query, executionContext, params, getBucketSet());
  }

  @Override
  public Object selectValue(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    SelectResults result = query(queryPredicate);
    if (result.isEmpty()) {
      return null;
    }
    if (result.size() > 1) {
      throw new FunctionDomainException(
          String.format("selectValue expects results of size 1, but found results of size %s",
              Integer.valueOf(result.size())));
    }
    return result.iterator().next();
  }

  /**
   * Asif: This method should not be used for multiple partitioned regions based join queries We do
   * not support equijoin queries on PartitionedRegions unless they are colocated and the the
   * colocated columns ACTUALLY EXIST IN WHERE CLAUSE , AND IN CASE OF MULTI COLUMN PARTITIONING ,
   * SHOULD HAVE AND CLAUSE.
   *
   * If not , this method will return wrong results. We DO NOT DETECT COLOCATION CRITERIA IN THE
   * MULTI REGION PR BASED QUERIES.
   */
  @Override
  public Object executeQuery(DefaultQuery query,
      final ExecutionContext executionContext,
      Object[] parameters, Set buckets)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    long startTime = 0L;
    Object result = null;
    boolean traceOn = DefaultQuery.QUERY_VERBOSE || query.isTraced();
    if (traceOn && proxy != null) {
      startTime = NanoTimer.getTime();
    }

    QueryObserver indexObserver = query.startTrace();

    try {
      result = proxy.executeQuery(query, executionContext, parameters, buckets);
    } finally {
      query.endTrace(indexObserver, startTime, result);
    }

    return result;
  }

  public PartitionedRegion getProxy() {
    return proxy;
  }

  public Set<Integer> getBucketSet() {
    return buckets;
  }

  // / Proxied calls
  @Override
  public void becomeLockGrantor() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    proxy.close();
  }

  @Override
  public boolean containsKeyOnServer(Object key) {
    return proxy.containsKeyOnServer(key);
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void create(Object key, Object value)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    create(key, value, null);
  }

  @Override
  public void create(Object key, Object value, Object callbackArgument)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    proxy.create(key, value, callbackArgument);
  }

  @Override
  public Region createSubregion(String subregionName, RegionAttributes regionAttributes)
      throws RegionExistsException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object destroy(Object key)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    return destroy(key, null);
  }

  @Override
  public Object destroy(Object key, Object callbackArgument)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    return proxy.destroy(key, callbackArgument);
  }

  @Override
  public void destroyRegion() throws CacheWriterException, TimeoutException {
    destroyRegion(null);
  }

  @Override
  public void destroyRegion(Object callbackArgument) throws CacheWriterException, TimeoutException {
    proxy.destroyRegion(callbackArgument);
  }

  @Override
  public boolean existsValue(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return proxy.existsValue(queryPredicate);
  }

  @Override
  public void forceRolling() {
    proxy.forceRolling();
  }

  @Override
  public InternalCache getCache() {
    return proxy.getCache();
  }

  @Override
  public String getFullPath() {
    return proxy.getFullPath();
  }

  @Override
  public List getInterestList() throws CacheWriterException {
    return proxy.getInterestList();
  }

  @Override
  public List getInterestListRegex() throws CacheWriterException {
    return proxy.getInterestListRegex();
  }

  @Override
  public String getName() {
    return proxy.getName();
  }

  @Override
  public Region getParentRegion() {
    return proxy.getParentRegion();
  }

  @Override
  public Lock getRegionDistributedLock() throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CacheStatistics getStatistics() throws StatisticsDisabledException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Region getSubregion(String path) {
    return proxy.getSubregion(path);
  }

  @Override
  public RegionAttributes getAttributes() {
    return proxy.getAttributes();
  }

  @Override
  public AttributesMutator getAttributesMutator() {
    return proxy.getAttributesMutator();
  }

  @Override
  public Lock getDistributedLock(Object key) throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void invalidate(Object key) throws TimeoutException, EntryNotFoundException {
    invalidate(key, null);
  }

  @Override
  public void invalidate(Object key, Object callbackArgument)
      throws TimeoutException, EntryNotFoundException {
    proxy.invalidate(key, callbackArgument);
  }

  @Override
  public void invalidateRegion() throws TimeoutException {
    invalidateRegion(null);
  }

  @Override
  public void invalidateRegion(Object callbackArgument) throws TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDestroyed() {
    return proxy.isDestroyed();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public Object getUserAttribute() {
    return proxy.getUserAttribute();
  }

  public int[] getDiskDirSizes() {
    return proxy.getDiskDirSizes();
  }

  @Override
  public Set subregions(boolean recursive) {
    return proxy.subregions(recursive);
  }

  @Override
  public void unregisterInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unregisterInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeToDisk() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setUserAttribute(Object value) {
    proxy.setUserAttribute(value);
  }

  @Override
  public Object remove(Object key) {
    return proxy.remove(key);
  }

  @Override
  public void registerInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterest(Object key, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy)
      throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable)
      throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy)
      throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable)
      throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set keySetOnServer() {
    return proxy.keySetOnServer();
  }

  @Override
  public int sizeOnServer() {
    return proxy.sizeOnServer();
  }

  @Override
  public boolean isEmptyOnServer() {
    return proxy.isEmptyOnServer();
  }

  @Override
  public void loadSnapshot(InputStream inputStream)
      throws IOException, ClassNotFoundException, CacheWriterException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map getAll(Collection keys) {
    return getAll(keys, null);
  }

  @Override
  public Map getAll(Collection keys, Object callback) {
    HashMap result = new HashMap();
    for (Object key : keys) {
      try {
        result.put(key, get(key, callback));
      } catch (Exception e) {
        logger.warn(String.format("The following exception occurred attempting to get key=%s",
            key),
            e);
      }
    }
    return result;
  }

  @Override
  public void localClear() {
    proxy.localClear();
  }

  @Override
  public void localDestroyRegion() {
    localDestroyRegion(null);
  }

  @Override
  public void localDestroyRegion(Object callbackArgument) {
    proxy.localDestroyRegion(callbackArgument);
  }

  @Override
  public void localInvalidateRegion() {
    localInvalidateRegion(null);
  }

  @Override
  public void localInvalidateRegion(Object callbackArgument) {
    proxy.localInvalidateRegion(callbackArgument);
  }

  @Override
  public void localDestroy(Object key) throws EntryNotFoundException {
    localDestroy(key, null);
  }

  // TODO, we could actually perform a local destroy
  @Override
  public void localDestroy(Object key, Object callbackArgument) throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void localInvalidate(Object key) throws EntryNotFoundException {
    localInvalidate(key, null);
  }

  // TODO, we could actually perform a local invalidate
  @Override
  public void localInvalidate(Object key, Object callbackArgument) throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object put(Object key, Object value) throws TimeoutException, CacheWriterException {
    return put(key, value, null);
  }

  @Override
  public Object put(Object key, Object value, Object callbackArgument)
      throws TimeoutException, CacheWriterException {
    return proxy.put(key, value, callbackArgument);
  }

  @Override
  public void putAll(Map map) {
    proxy.putAll(map);
  }

  @Override
  public void putAll(Map map, Object callbackArg) {
    proxy.putAll(map, callbackArg);
  }

  // /
  // / Read only calls
  // /

  @Override
  public boolean containsKey(Object key) {
    if (isInDataSet(key, null)) {
      return proxy.containsKey(key);
    } else {
      return false;
    }
  }

  @Override
  public boolean containsValueForKey(Object key) {
    if (isInDataSet(key, null)) {
      return proxy.containsValueForKey(key);
    } else {
      return false;
    }
  }

  @Override
  public Entry getEntry(Object key) {
    if (isInDataSet(key, null)) {
      return proxy.getEntry(key);
    } else {
      return null;
    }
  }

  @Override
  public int size() {
    return proxy.entryCount(getBucketSet());
  }

  @Override
  public Object get(Object key) throws CacheLoaderException, TimeoutException {
    return get(key, null);
  }

  @Override
  public Object get(Object key, Object aCallbackArgument)
      throws TimeoutException, CacheLoaderException {
    if (isInDataSet(key, aCallbackArgument)) {
      return proxy.get(key, aCallbackArgument);
    } else {
      return null;
    }
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getName());
    sb.append("[path='").append(getFullPath());
    sb.append("';scope=").append(proxy.getScope());
    sb.append("';dataPolicy=").append(proxy.getDataPolicy());
    sb.append(" ;bucketIds=").append(buckets);
    return sb.append(']').toString();
  }

  @Override
  public void saveSnapshot(OutputStream outputStream) throws IOException {}

  @Override
  public void registerInterest(Object key, boolean isDurable, boolean receiveValues)
      throws CacheWriterException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void registerInterestRegex(String regex, boolean isDurable, boolean receiveValues)
      throws CacheWriterException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException();

  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
   */
  @Override
  public Object putIfAbsent(Object key, Object value) {
    return proxy.putIfAbsent(key, value);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
   */
  @Override
  public boolean remove(Object key, Object value) {
    return proxy.remove(key, value);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
   */
  @Override
  public Object replace(Object key, Object value) {
    return proxy.replace(key, value);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object,
   * java.lang.Object)
   */
  @Override
  public boolean replace(Object key, Object oldValue, Object newValue) {
    return proxy.replace(key, oldValue, newValue);
  }

  @Override
  public RegionService getRegionService() {
    return getCache();
  }

  @Override
  public RegionSnapshotService<?, ?> getSnapshotService() {
    return new RegionSnapshotServiceImpl(this);
  }

  protected class LocalEntriesSet extends EntriesSet {

    public LocalEntriesSet(IteratorType type) {
      super(proxy, false, type, false);
    }

    public LocalEntriesSet() {
      this(IteratorType.ENTRIES);
    }

    @Override
    public Iterator iterator() {
      return new LocalEntriesSetIterator();
    }

    protected class LocalEntriesSetIterator implements Iterator<Object> {
      Iterator curBucketIter = null;
      Integer curBucketId;
      List<Integer> localBuckets = new ArrayList<>(buckets);
      int index = 0;
      int localBucketsSize = localBuckets.size();
      boolean hasNext = false;
      Object next = null;

      LocalEntriesSetIterator() {
        next = moveNext();
      }

      @Override
      public Object next() {
        Object result = next;
        if (result != null) {
          next = moveNext();
          return result;
        }
        throw new NoSuchElementException();
      }

      @Override
      public boolean hasNext() {
        return (next != null);
      }

      private Object moveNext() {
        // Check if PR is destroyed.
        proxy.checkReadiness();
        try {
          for (;;) { // Loop till we get valid value
            while (curBucketIter == null || !(hasNext = curBucketIter.hasNext())) { // Loop all the
                                                                                    // buckets.
              if (index >= localBucketsSize) {
                return null;
              }
              curBucketId = localBuckets.get(index++);
              BucketRegion br = proxy.getDataStore().getLocalBucketById(curBucketId);
              if (br == null) {
                throw new BucketMovedException(
                    "The Bucket region with id " + curBucketId + " is moved/destroyed.");
              }
              br.waitForData();
              curBucketIter = br.entrySet().iterator();
            }

            // Check if there is a valid value.
            if (hasNext) {
              Map.Entry e = (Map.Entry) curBucketIter.next();
              try {
                if (iterType == IteratorType.VALUES) {
                  if (isKeepSerialized()) {
                    next = ((NonTXEntry) e).getRawValue();
                  } else if (ignoreCopyOnReadForQuery) {
                    next = ((NonTXEntry) e).getValue(true);
                  } else {
                    next = e.getValue();
                  }
                  if (next == null || Token.isInvalidOrRemoved(next)) {
                    continue;
                  }
                } else if (iterType == IteratorType.KEYS) {
                  next = e.getKey();
                } else {
                  if (((NonTXEntry) e).isDestroyed()) {
                    throw new EntryDestroyedException();
                  }
                  next = e;
                }
              } catch (EntryDestroyedException ede) {
                // Entry is destroyed, continue to the next element.
                continue;
              }
            }
            return next;
          }
        } catch (RegionDestroyedException rde) {
          throw new BucketMovedException(
              "The Bucket region with id " + curBucketId + " is moved/destroyed.");
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "This iterator does not support modification");
      }

    }

    @Override
    public int size() {
      int size = 0;
      for (Integer bId : buckets) {
        BucketRegion br = proxy.getDataStore().getLocalBucketById(bId);
        size += br.size();
      }
      return size;
    }

  }

  @Override
  public void removeAll(Collection keys) {
    proxy.removeAll(keys);
  }

  @Override
  public void removeAll(Collection keys, Object aCallbackArgument) {
    proxy.removeAll(keys, aCallbackArgument);
  }

}
