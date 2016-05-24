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

import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheStatistics;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.StatisticsDisabledException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.QueryExecutor;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.snapshot.RegionSnapshotService;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.LocalRegion.IteratorType;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.internal.cache.execute.BucketMovedException;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.gemstone.gemfire.internal.cache.snapshot.RegionSnapshotServiceImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;


/**
 * 
 *
 */
public class LocalDataSet implements Region, QueryExecutor {

  private static final Logger logger = LogService.getLogger();
  
  final private PartitionedRegion proxy;
  final private Set<Integer> buckets;
  private InternalRegionFunctionContext rfContext;

  public LocalDataSet(PartitionedRegion pr, Set<Integer> buckets) {
    this.proxy = pr;
    this.buckets = buckets;
  }

  public Set<Region.Entry> entrySet(boolean recursive) {
    return entries(recursive);
  }

  public Set<Region.Entry> entrySet() {
    return entries(false);
  }

  public Set<Region.Entry> entries(boolean recursive) {
    return this.proxy.new PREntriesSet(getBucketSet());    
  }

  public Collection values() {
    this.proxy.checkReadiness();
    return this.proxy.new ValuesSet(getBucketSet());
  }

  public Set keys() {
    return this.proxy.new KeysSet(getBucketSet());
  }

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
    return PartitionedRegionHelper.getHashKey(this.proxy, op, key, value, callbackArg);
  }
  
  private boolean isInDataSet(Object key, Object callbackArgument) {
    int bucketId = getHashKey(Operation.CONTAINS_KEY, key, null, callbackArgument);
    Integer bucketIdInt = Integer.valueOf(bucketId);
    return buckets.contains(bucketIdInt);
  }

  public InternalRegionFunctionContext getFunctionContext() {
    return this.rfContext;
  }

  public void setFunctionContext(InternalRegionFunctionContext fContext) {
    this.rfContext = fContext;
  }

  public SelectResults query(String queryPredicate)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    QueryService qs = getCache().getLocalQueryService();
    DefaultQuery query = (DefaultQuery)qs.newQuery("select * from "
        + getFullPath() + " this where " + queryPredicate);
    Object[] params = null;
    return (SelectResults)this.executeQuery(query, params, getBucketSet());
  }

  public Object selectValue(String queryPredicate)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    SelectResults result = query(queryPredicate);
    if (result.isEmpty()) {
      return null;
    }
    if (result.size() > 1) {
      throw new FunctionDomainException(LocalizedStrings
          .AbstractRegion_SELECTVALUE_EXPECTS_RESULTS_OF_SIZE_1_BUT_FOUND_RESULTS_OF_SIZE_0
              .toLocalizedString(Integer.valueOf(result.size())));
    }
    return result.iterator().next();
  }

  /**
   * Asif: This method should not be used for multiple partitioned regions based join queries
   * We do not support equijoin queries on PartitionedRegions unless they are colocated
   * and the the colocated columns ACTUALLY EXIST IN WHERE CLAUSE  , AND  IN CASE OF
   * MULTI COLUMN PARTITIONING , SHOULD HAVE AND CLAUSE.
   * 
   *  If not , this method will return wrong results.   We DO NOT DETECT COLOCATION
   *  CRITERIA IN THE MULTI REGION PR BASED QUERIES. 
   */
  public Object executeQuery(DefaultQuery query, Object[] parameters,
      Set buckets) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    long startTime = 0L;
    Object result = null;
    boolean traceOn = DefaultQuery.QUERY_VERBOSE || query.isTraced();
    if (traceOn && this.proxy != null) {
      startTime = NanoTimer.getTime();
    }

    QueryObserver indexObserver = query.startTrace();

    try{
      result = this.proxy.executeQuery(query, parameters, buckets);
    }
    finally {
      query.endTrace(indexObserver, startTime, result);
    }
    
    return result;
  }

  public PartitionedRegion getProxy() {
    return this.proxy;
  }

  public Set<Integer> getBucketSet() {
    return this.buckets;
  }
  
  // / Proxied calls
  public void becomeLockGrantor() {
    throw new UnsupportedOperationException();
  }

  public void clear() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    this.proxy.close();
  }

  public boolean containsKeyOnServer(Object key) {
    return this.proxy.containsKeyOnServer(key);
  }

  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  public void create(Object key, Object value) throws TimeoutException,
      EntryExistsException, CacheWriterException {
    create(key, value, null);
  }

  public void create(Object key, Object value, Object callbackArgument)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    this.proxy.create(key, value, callbackArgument);
  }

  public Region createSubregion(String subregionName,
      RegionAttributes regionAttributes) throws RegionExistsException,
      TimeoutException {
    throw new UnsupportedOperationException();
  }

  public Object destroy(Object key) throws TimeoutException,
      EntryNotFoundException, CacheWriterException {
    return destroy(key, null);
  }

  public Object destroy(Object key, Object callbackArgument)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    return this.proxy.destroy(key, callbackArgument);
  }

  public void destroyRegion() throws CacheWriterException, TimeoutException {
    destroyRegion(null);
  }

  public void destroyRegion(Object callbackArgument)
      throws CacheWriterException, TimeoutException {
    this.proxy.destroyRegion(callbackArgument);
  }

  public boolean existsValue(String queryPredicate)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    return this.proxy.existsValue(queryPredicate);
  }

  public void forceRolling() {
    this.proxy.forceRolling();
  }

  public GemFireCacheImpl getCache() {
    return this.proxy.getCache();
  }

  public String getFullPath() {
    return this.proxy.getFullPath();
  }

  public List getInterestList() throws CacheWriterException {
    return this.proxy.getInterestList();
  }

  public List getInterestListRegex() throws CacheWriterException {
    return this.proxy.getInterestListRegex();
  }

  public String getName() {
    return this.proxy.getName();
  }

  public Region getParentRegion() {
    return this.proxy.getParentRegion();
  }

  public Lock getRegionDistributedLock() throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  public CacheStatistics getStatistics() throws StatisticsDisabledException {
    throw new UnsupportedOperationException();
  }

  public Region getSubregion(String path) {
    return this.proxy.getSubregion(path);
  }

  public RegionAttributes getAttributes() {
    return this.proxy.getAttributes();
  }

  public AttributesMutator getAttributesMutator() {
    return this.proxy.getAttributesMutator();
  }

  public Lock getDistributedLock(Object key) throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  public void invalidate(Object key) throws TimeoutException,
      EntryNotFoundException {
    invalidate(key, null);
  }

  public void invalidate(Object key, Object callbackArgument)
      throws TimeoutException, EntryNotFoundException {
    this.proxy.invalidate(key, callbackArgument);
  }

  public void invalidateRegion() throws TimeoutException {
    invalidateRegion(null);
  }

  public void invalidateRegion(Object callbackArgument) throws TimeoutException {
    throw new UnsupportedOperationException();
  }

  public boolean isDestroyed() {
    return this.proxy.isDestroyed();
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public Object getUserAttribute() {
    return this.proxy.getUserAttribute();
  }

  public int[] getDiskDirSizes() {
    return this.proxy.getDiskDirSizes();
  }

  public Set subregions(boolean recursive) {
    return this.proxy.subregions(recursive);
  }

  public void unregisterInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void unregisterInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void writeToDisk() {
    throw new UnsupportedOperationException();
  }

  public void setUserAttribute(Object value) {
    this.proxy.setUserAttribute(value);
  }

  public Object remove(Object key) {
    return this.proxy.remove(key);
  }

  public void registerInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void registerInterest(Object key, boolean isDurable)
      throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void registerInterest(Object key, InterestResultPolicy policy)
      throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void registerInterest(Object key, InterestResultPolicy policy,
      boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex, boolean isDurable)
      throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex, InterestResultPolicy policy)
      throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex, InterestResultPolicy policy,
      boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public Set keySetOnServer() {
    return this.proxy.keySetOnServer();
  }

  public void loadSnapshot(InputStream inputStream) throws IOException,
      ClassNotFoundException, CacheWriterException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  public Map getAll(Collection keys) {
    return getAll(keys, null);
  }

  public Map getAll(Collection keys, Object callback) {
    HashMap result = new HashMap();
    for (Iterator i = keys.iterator(); i.hasNext();) {
      Object key = i.next();
      try {
        result.put(key, get(key, callback));
      } catch (Exception e) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.LocalRegion_THE_FOLLOWING_EXCEPTION_OCCURRED_ATTEMPTING_TO_GET_KEY_0, key), e);
      }
    }
    return result;
  }

  public void localClear() {
    this.proxy.localClear();
  }

  public void localDestroyRegion() {
    localDestroyRegion(null);
  }

  public void localDestroyRegion(Object callbackArgument) {
    this.proxy.localDestroyRegion(callbackArgument);
  }

  public void localInvalidateRegion() {
    localInvalidateRegion(null);
  }

  public void localInvalidateRegion(Object callbackArgument) {
    this.proxy.localInvalidateRegion(callbackArgument);
  }

  public void localDestroy(Object key) throws EntryNotFoundException {
    localDestroy(key, null);
  }

  // TODO, we could actually perform a local destroy
  public void localDestroy(Object key, Object callbackArgument)
      throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  public void localInvalidate(Object key) throws EntryNotFoundException {
    localInvalidate(key, null);
  }

  // TODO, we could actually perform a local invalidate
  public void localInvalidate(Object key, Object callbackArgument)
      throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  public Object put(Object key, Object value) throws TimeoutException,
      CacheWriterException {
    return put(key, value, null);
  }

  public Object put(Object key, Object value, Object callbackArgument)
      throws TimeoutException, CacheWriterException {
    return this.proxy.put(key, value, callbackArgument);
  }

  public void putAll(Map map) {
    this.proxy.putAll(map);
  }
  
  @Override
  public void putAll(Map map, Object callbackArg) {
    this.proxy.putAll(map, callbackArg);
  }

  // /
  // / Read only calls
  // /

  public boolean containsKey(Object key) {
    if(isInDataSet(key, null)) {
      return this.proxy.containsKey(key);
    } else {
      return false;
    }
  }

  public boolean containsValueForKey(Object key) {
    if(isInDataSet(key, null)) {
      return this.proxy.containsValueForKey(key);
    } else {
      return false;
    }
  }

  public Entry getEntry(Object key) {
    if(isInDataSet(key, null)) {
      return this.proxy.getEntry(key);
    } else {
      return null;
    }
  }

  public int size() {
    return this.proxy.entryCount(getBucketSet());
  }

  public Object get(Object key) throws CacheLoaderException, TimeoutException {
    return get(key, null);
  }

  public Object get(Object key, Object aCallbackArgument)
      throws TimeoutException, CacheLoaderException {
    if (isInDataSet(key, aCallbackArgument)) {
      return this.proxy.get(key, aCallbackArgument);
    }
    else {
      return null;
    }
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getName());
    sb.append("[path='").append(getFullPath());
    sb.append("';scope=").append(this.proxy.getScope());
    sb.append("';dataPolicy=").append(this.proxy.getDataPolicy());
    sb.append(" ;bucketIds=").append(this.buckets);
    return sb.append(']').toString();
  }

  public void saveSnapshot(OutputStream outputStream) throws IOException {
  }

  public void registerInterest(Object key, boolean isDurable,
      boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException();

  }

  public void registerInterest(Object key, InterestResultPolicy policy,
      boolean isDurable, boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException();

  }

  public void registerInterestRegex(String regex, boolean isDurable,
      boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException();

  }

  public void registerInterestRegex(String regex, InterestResultPolicy policy,
      boolean isDurable, boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException();

  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
   */
  public Object putIfAbsent(Object key, Object value) {
    return this.proxy.putIfAbsent(key, value);
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
   */
  public boolean remove(Object key, Object value) {
    return this.proxy.remove(key, value);
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
   */
  public Object replace(Object key, Object value) {
    return this.proxy.replace(key, value);
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object, java.lang.Object)
   */
  public boolean replace(Object key, Object oldValue, Object newValue) {
    return this.proxy.replace(key, oldValue, newValue);
  } 

  public RegionService getRegionService() {
    return getCache();
  }
  
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
      List<Integer> localBuckets = new ArrayList<Integer>(buckets);
      int index = 0;
      int localBucketsSize = localBuckets.size();
      boolean hasNext = false;
      Object next = null;

      LocalEntriesSetIterator() {
        this.next = moveNext();
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
        return (this.next != null);
      }

      private Object moveNext() {
        // Check if PR is destroyed.
        proxy.checkReadiness();
        try {
          for (;;) { // Loop till we get valid value
            while (curBucketIter == null || !(hasNext = curBucketIter.hasNext())) { // Loop all the buckets.
              if (index >= localBucketsSize) {
                return null;
              }
              curBucketId = localBuckets.get(index++);
              BucketRegion br = proxy.getDataStore().getLocalBucketById(
                curBucketId);
              if (br == null) {
                throw new BucketMovedException("The Bucket region with id " +
                  curBucketId + " is moved/destroyed.");
              }
              br.waitForData();
              curBucketIter = br.entrySet().iterator();
            }
            
            // Check if there is a valid value.
            if (hasNext){
              Map.Entry e = (Map.Entry)curBucketIter.next();
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
                  if(((NonTXEntry)e).isDestroyed()){
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
          throw new BucketMovedException("The Bucket region with id " + 
            curBucketId + " is moved/destroyed.");
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException(LocalizedStrings
            .LocalRegion_THIS_ITERATOR_DOES_NOT_SUPPORT_MODIFICATION
            .toLocalizedString());
      }

    }

    @Override
    public int size() {
      int size = 0;
      for (Integer bId : buckets) {
        BucketRegion br = proxy.getDataStore().getLocalBucketById(bId);
        size+=br.size();
      }
      return size;
    }

  }

  @Override
  public void removeAll(Collection keys) {
    this.proxy.removeAll(keys);
  }

  @Override
  public void removeAll(Collection keys, Object aCallbackArgument) {
    this.proxy.removeAll(keys, aCallbackArgument);
  }
  
}
