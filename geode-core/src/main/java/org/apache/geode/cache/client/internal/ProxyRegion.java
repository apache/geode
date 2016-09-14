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
package org.apache.geode.cache.client.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.internal.cache.snapshot.RegionSnapshotServiceImpl;

/**
 * A wrapper class over an actual Region instance. This is used when the
 * multiuser-authentication attribute is set to true.
 * 
 * @see ProxyCache
 * @since GemFire 6.5
 */
public class ProxyRegion implements Region {
  
  private final ProxyCache proxyCache;
  private final Region realRegion;
  
  public ProxyRegion(ProxyCache proxyCache, Region realRegion) {
    this.proxyCache = proxyCache;
    this.realRegion = realRegion;
  }

  public void becomeLockGrantor() {
    throw new UnsupportedOperationException();
  }

  public void clear() {
    try {
      preOp();
      this.realRegion.clear();
    } finally {
      postOp();
    }
  }

  public void close() {
    try {
      preOp();
      this.realRegion.close();
    } finally {
      postOp();
    }
  }

  public boolean containsKey(Object key) {
    try {
      preOp();
      return this.realRegion.containsKey(key);
    } finally {
      postOp();
    }
  }

  public boolean containsKeyOnServer(Object key) {
    try {
      preOp();
      return this.realRegion.containsKeyOnServer(key);
    } finally {
      postOp();
    }
  }

  public boolean containsValue(Object value) {
    try {
      preOp();
      return this.realRegion.containsValue(value);
    } finally {
      postOp();
    }
  }

  public boolean containsValueForKey(Object key) {
    try {
      preOp();
      return this.realRegion.containsValueForKey(key);
    } finally {
      postOp();
    }
  }

  public void create(Object key, Object value) throws TimeoutException,
      EntryExistsException, CacheWriterException {
    try {
      preOp();
      this.realRegion.create(key, value);
    } finally {
      postOp();
    }
  }

  public void create(Object key, Object value, Object callbackArgument)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    try {
      preOp();
      this.realRegion.create(key, value, callbackArgument);
    } finally {
      postOp();
    }
  }

  public Region createSubregion(String subregionName,
      RegionAttributes regionAttributes) throws RegionExistsException,
      TimeoutException {
    throw new UnsupportedOperationException();
  }

  public Object destroy(Object key) throws TimeoutException,
      EntryNotFoundException, CacheWriterException {
    try {
      preOp();
      return this.realRegion.destroy(key);
    } finally {
      postOp();
    }
  }

  public Object destroy(Object key, Object callbackArgument)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    try {
      preOp();
      return this.realRegion.destroy(key, callbackArgument);
    } finally {
      postOp();
    }
  }

  public void destroyRegion() throws CacheWriterException, TimeoutException {
    try {
      preOp();
      this.realRegion.destroyRegion();
    } finally {
      postOp();
    }
  }

  public void destroyRegion(Object callbackArgument)
      throws CacheWriterException, TimeoutException {
    try {
      preOp();
      this.realRegion.destroyRegion(callbackArgument);
    } finally {
      postOp();
    }
  }

  public Set entries(boolean recursive) {
    try {
      preOp();
      return this.realRegion.entries(recursive);
    } finally {
      postOp();
    }
  }

  public Set entrySet(boolean recursive) {
    try {
      preOp();
      return this.realRegion.entrySet(recursive);
    } finally {
      postOp();
    }
  }

  public Set entrySet() {
    try {
      preOp();
      return this.realRegion.entrySet();
    } finally {
      postOp();
    }
  }

  public boolean existsValue(String queryPredicate)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    try {
      preOp();
      return this.realRegion.existsValue(queryPredicate);
    } finally {
      postOp();
    }
  }

  public void forceRolling() {
    throw new UnsupportedOperationException();
  }

  public Object get(Object key) throws CacheLoaderException, TimeoutException {
    try {
      preOp();
      return this.realRegion.get(key);
    } finally {
      postOp();
    }
  }

  public Object get(Object key, Object callbackArgument)
      throws TimeoutException, CacheLoaderException {
    try {
      preOp();
      return this.realRegion.get(key, callbackArgument);
    } finally {
      postOp();
    }
  }

  public Map getAll(Collection keys) {
    return getAll(keys, null);
  }

  public Map getAll(Collection keys, Object callback) {
    try {
      preOp();
      return this.realRegion.getAll(keys, callback);
    } finally {
      postOp();
    }
  }

  public RegionAttributes getAttributes() {
    return realRegion.getAttributes();
  }

  public AttributesMutator getAttributesMutator() {
    throw new UnsupportedOperationException();
  }

  public Cache getCache() {
    throw new UnsupportedOperationException();
  }

  public RegionService getRegionService() {
    return this.proxyCache;
  }
  
  public ProxyCache getAuthenticatedCache() {
    return this.proxyCache;    
  }

  public Lock getDistributedLock(Object key) throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  public Entry getEntry(Object key) {
    try {
      preOp();
      return this.realRegion.getEntry(key);
    } finally {
      postOp();
    }
  }

  public String getFullPath() {
    return this.realRegion.getFullPath();
  }

  public List getInterestList() {
    throw new UnsupportedOperationException();
  }

  public List getInterestListRegex() {
    throw new UnsupportedOperationException();
  }

  public String getName() {
    return this.realRegion.getName();
  }

  public Region getParentRegion() {
    return this.realRegion.getParentRegion();
  }

  public Lock getRegionDistributedLock() throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  public CacheStatistics getStatistics() throws StatisticsDisabledException {
    return this.realRegion.getStatistics();
  }

  public Region getSubregion(String path) {
    Region region = this.realRegion.getSubregion(path);
    return region != null ? new ProxyRegion(this.proxyCache, region) : null;
  }

  public Object getUserAttribute() {
    return this.realRegion.getUserAttribute();
  }

  public void invalidate(Object key) throws TimeoutException,
      EntryNotFoundException {
    try {
      preOp();
      this.realRegion.invalidate(key);
    } finally {
      postOp();
    }
  }

  public void invalidate(Object key, Object callbackArgument)
      throws TimeoutException, EntryNotFoundException {
    try {
      preOp();
      this.realRegion.invalidate(key, callbackArgument);
    } finally {
      postOp();
    }
  }

  public void invalidateRegion() throws TimeoutException {
    try {
      preOp();
      this.realRegion.invalidateRegion();
    } finally {
      postOp();
    }
  }

  public void invalidateRegion(Object callbackArgument) throws TimeoutException {
    try {
      preOp();
      this.realRegion.invalidateRegion(callbackArgument);
    } finally {
      postOp();
    }
  }

  public boolean isDestroyed() {
    return this.realRegion.isDestroyed();
  }

  public boolean isEmpty() {
    return this.realRegion.isEmpty();
  }

  public Set keySet() {
    return this.realRegion.keySet();
  }

  public Set keySetOnServer() {
    try {
      preOp();
      return this.realRegion.keySetOnServer();
    } finally {
      postOp();
    }
  }

  public Set keys() {
    try {
      preOp();
      return this.realRegion.keys();
    } finally {
      postOp();
    }
  }

  public void loadSnapshot(InputStream inputStream) throws IOException,
      ClassNotFoundException, CacheWriterException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  public void localClear() {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  public void localDestroy(Object key) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  public void localDestroy(Object key, Object callbackArgument)
      throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  public void localDestroyRegion() {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  public void localDestroyRegion(Object callbackArgument) {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  public void localInvalidate(Object key) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  public void localInvalidate(Object key, Object callbackArgument)
      throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  public void localInvalidateRegion() {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  public void localInvalidateRegion(Object callbackArgument) {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  public Object put(Object key, Object value) throws TimeoutException,
      CacheWriterException {
    try {
      preOp();
      return this.realRegion.put(key, value);
    } finally {
      postOp();
    }
  }

  public Object put(Object key, Object value, Object callbackArgument)
      throws TimeoutException, CacheWriterException {
    try {
      preOp();
      return this.realRegion.put(key, value, callbackArgument);
    } finally {
      postOp();
    }
  }

  public void putAll(Map map) {
    putAll(map, null);
  }
  
  @Override
  public void putAll(Map map, Object callbackArg) {
    try {
      preOp();
      this.realRegion.putAll(map, callbackArg);
    } finally {
      postOp();
    }
  }

  public SelectResults query(String queryPredicate)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    try {
      preOp();
      return this.realRegion.query(queryPredicate);
    } finally {
      postOp();
    }
  }

  public void registerInterest(Object key) {
    throw new UnsupportedOperationException();
  }

  public void registerInterest(Object key, InterestResultPolicy policy) {
    throw new UnsupportedOperationException();
  }

  public void registerInterest(Object key, boolean isDurable) {
    throw new UnsupportedOperationException();
  }

  public void registerInterest(Object key, boolean isDurable,
      boolean receiveValues) {
    throw new UnsupportedOperationException();
  }

  public void registerInterest(Object key, InterestResultPolicy policy,
      boolean isDurable, boolean receiveValues) {
    throw new UnsupportedOperationException();
  }

  public void registerInterest(Object key, InterestResultPolicy policy,
      boolean isDurable) {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex) {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex, InterestResultPolicy policy) {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex, boolean isDurable) {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex, boolean isDurable,
      boolean receiveValues) {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex, InterestResultPolicy policy,
      boolean isDurable) {
    throw new UnsupportedOperationException();
  }

  public void registerInterestRegex(String regex, InterestResultPolicy policy,
      boolean isDurable, boolean receiveValues) {
    throw new UnsupportedOperationException();
  }

  public Object remove(Object key) {
    try {
      preOp();
      return this.realRegion.remove(key);
    } finally {
      postOp();
    }
  }

  public void saveSnapshot(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException();
  }

  public Object selectValue(String queryPredicate)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    try {
      preOp();
      return this.realRegion.selectValue(queryPredicate);
    } finally {
      postOp();
    }
  }

  public void setUserAttribute(Object value) {
    throw new UnsupportedOperationException();
  }

  public int size() {
    try {
      preOp();
      return this.realRegion.size();
    } finally {
      postOp();
    }
  }

  public Set subregions(boolean recursive) {
    return this.realRegion.subregions(recursive);
  }

  public void unregisterInterest(Object key) {
    throw new UnsupportedOperationException();
  }

  public void unregisterInterestRegex(String regex) {
    throw new UnsupportedOperationException();
  }

  public Collection values() {
    try {
      preOp();
      return this.realRegion.values();
    } finally {
      postOp();
    }
  }

  public void writeToDisk() {
    throw new UnsupportedOperationException();
  }

  private void preOp() {
    if (this.proxyCache.isClosed()) {
      throw new CacheClosedException("Cache is closed for this user.");
    }
    UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
  }

  private void postOp() {
    this.proxyCache.setUserAttributes(UserAttributes.userAttributes.get());
    UserAttributes.userAttributes.set(null);
  }

  public Region getRealRegion() {
    return realRegion;
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
   */
  public Object putIfAbsent(Object key, Object value) {
    try {
      preOp();
      return this.realRegion.putIfAbsent(key, value);
    } finally {
      postOp();
}
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
   */
  public boolean remove(Object key, Object value) {
    try {
      preOp();
      return this.realRegion.remove(key, value);
    } finally {
      postOp();
    }
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
   */
  public Object replace(Object key, Object value) {
    try {
      preOp();
      return this.realRegion.replace(key, value);
    } finally {
      postOp();
    }
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object, java.lang.Object)
   */
  public boolean replace(Object key, Object oldValue, Object newValue) {
    try {
      preOp();
      return this.realRegion.replace(key, oldValue, newValue);
    } finally {
      postOp();
    }
  }
  
  public RegionSnapshotService<?, ?> getSnapshotService() {
    return new RegionSnapshotServiceImpl(this);
  }

  @Override
  public void removeAll(Collection keys) {
    removeAll(keys, null);
  }

  @Override
  public void removeAll(Collection keys, Object aCallbackArgument) {
    try {
      preOp();
      this.realRegion.removeAll(keys, aCallbackArgument);
    } finally {
      postOp();
    }
  }
}
