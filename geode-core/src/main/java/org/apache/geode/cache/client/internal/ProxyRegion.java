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
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.internal.cache.snapshot.RegionSnapshotServiceImpl;
import org.apache.geode.internal.statistics.StatisticsClock;

/**
 * A wrapper class over an actual Region instance. This is used when the multiuser-authentication
 * attribute is set to true.
 *
 * @see ProxyCache
 * @since GemFire 6.5
 */
public class ProxyRegion implements Region {

  private final ProxyCache proxyCache;
  private final Region realRegion;
  private final StatisticsClock statisticsClock;

  public ProxyRegion(ProxyCache proxyCache, Region realRegion, StatisticsClock statisticsClock) {
    this.proxyCache = proxyCache;
    this.realRegion = realRegion;
    this.statisticsClock = statisticsClock;
  }

  @Override
  public void becomeLockGrantor() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    try {
      preOp();
      realRegion.clear();
    } finally {
      postOp();
    }
  }

  @Override
  public void close() {
    try {
      preOp();
      realRegion.close();
    } finally {
      postOp();
    }
  }

  @Override
  public boolean containsKey(Object key) {
    try {
      preOp();
      return realRegion.containsKey(key);
    } finally {
      postOp();
    }
  }

  @Override
  public boolean containsKeyOnServer(Object key) {
    try {
      preOp();
      return realRegion.containsKeyOnServer(key);
    } finally {
      postOp();
    }
  }

  @Override
  public int sizeOnServer() {
    try {
      preOp();
      return realRegion.sizeOnServer();
    } finally {
      postOp();
    }
  }

  @Override
  public boolean isEmptyOnServer() {
    try {
      preOp();
      return realRegion.isEmptyOnServer();
    } finally {
      postOp();
    }
  }

  @Override
  public boolean containsValue(Object value) {
    try {
      preOp();
      return realRegion.containsValue(value);
    } finally {
      postOp();
    }
  }

  @Override
  public boolean containsValueForKey(Object key) {
    try {
      preOp();
      return realRegion.containsValueForKey(key);
    } finally {
      postOp();
    }
  }

  @Override
  public void create(Object key, Object value)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    try {
      preOp();
      realRegion.create(key, value);
    } finally {
      postOp();
    }
  }

  @Override
  public void create(Object key, Object value, Object callbackArgument)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    try {
      preOp();
      realRegion.create(key, value, callbackArgument);
    } finally {
      postOp();
    }
  }

  @Override
  public Region createSubregion(String subregionName, RegionAttributes regionAttributes)
      throws RegionExistsException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object destroy(Object key)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    try {
      preOp();
      return realRegion.destroy(key);
    } finally {
      postOp();
    }
  }

  @Override
  public Object destroy(Object key, Object callbackArgument)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    try {
      preOp();
      return realRegion.destroy(key, callbackArgument);
    } finally {
      postOp();
    }
  }

  @Override
  public void destroyRegion() throws CacheWriterException, TimeoutException {
    try {
      preOp();
      realRegion.destroyRegion();
    } finally {
      postOp();
    }
  }

  @Override
  public void destroyRegion(Object callbackArgument) throws CacheWriterException, TimeoutException {
    try {
      preOp();
      realRegion.destroyRegion(callbackArgument);
    } finally {
      postOp();
    }
  }

  @Override
  public Set entrySet(boolean recursive) {
    try {
      preOp();
      return realRegion.entrySet(recursive);
    } finally {
      postOp();
    }
  }

  @Override
  public Set entrySet() {
    try {
      preOp();
      return realRegion.entrySet();
    } finally {
      postOp();
    }
  }

  @Override
  public boolean existsValue(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    try {
      preOp();
      return realRegion.existsValue(queryPredicate);
    } finally {
      postOp();
    }
  }

  @Override
  public void forceRolling() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(Object key) throws CacheLoaderException, TimeoutException {
    try {
      preOp();
      return realRegion.get(key);
    } finally {
      postOp();
    }
  }

  @Override
  public Object get(Object key, Object callbackArgument)
      throws TimeoutException, CacheLoaderException {
    try {
      preOp();
      return realRegion.get(key, callbackArgument);
    } finally {
      postOp();
    }
  }

  @Override
  public Map getAll(Collection keys) {
    return getAll(keys, null);
  }

  @Override
  public Map getAll(Collection keys, Object callback) {
    try {
      preOp();
      return realRegion.getAll(keys, callback);
    } finally {
      postOp();
    }
  }

  @Override
  public RegionAttributes getAttributes() {
    return realRegion.getAttributes();
  }

  @Override
  public AttributesMutator getAttributesMutator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Cache getCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RegionService getRegionService() {
    return proxyCache;
  }

  public ProxyCache getAuthenticatedCache() {
    return proxyCache;
  }

  @Override
  public Lock getDistributedLock(Object key) throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Entry getEntry(Object key) {
    try {
      preOp();
      return realRegion.getEntry(key);
    } finally {
      postOp();
    }
  }

  @Override
  public String getFullPath() {
    return realRegion.getFullPath();
  }

  @Override
  public List getInterestList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List getInterestListRegex() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return realRegion.getName();
  }

  @Override
  public Region getParentRegion() {
    return realRegion.getParentRegion();
  }

  @Override
  public Lock getRegionDistributedLock() throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CacheStatistics getStatistics() throws StatisticsDisabledException {
    return realRegion.getStatistics();
  }

  @Override
  public Region getSubregion(String path) {
    Region region = realRegion.getSubregion(path);
    return region != null ? new ProxyRegion(proxyCache, region, statisticsClock) : null;
  }

  @Override
  public Object getUserAttribute() {
    return realRegion.getUserAttribute();
  }

  @Override
  public void invalidate(Object key) throws TimeoutException, EntryNotFoundException {
    try {
      preOp();
      realRegion.invalidate(key);
    } finally {
      postOp();
    }
  }

  @Override
  public void invalidate(Object key, Object callbackArgument)
      throws TimeoutException, EntryNotFoundException {
    try {
      preOp();
      realRegion.invalidate(key, callbackArgument);
    } finally {
      postOp();
    }
  }

  @Override
  public void invalidateRegion() throws TimeoutException {
    try {
      preOp();
      realRegion.invalidateRegion();
    } finally {
      postOp();
    }
  }

  @Override
  public void invalidateRegion(Object callbackArgument) throws TimeoutException {
    try {
      preOp();
      realRegion.invalidateRegion(callbackArgument);
    } finally {
      postOp();
    }
  }

  @Override
  public boolean isDestroyed() {
    return realRegion.isDestroyed();
  }

  @Override
  public boolean isEmpty() {
    return realRegion.isEmpty();
  }

  @Override
  public Set keySetOnServer() {
    try {
      preOp();
      return realRegion.keySetOnServer();
    } finally {
      postOp();
    }
  }

  @Override
  public Set keySet() {
    try {
      preOp();
      return realRegion.keySet();
    } finally {
      postOp();
    }
  }

  @Override
  public void loadSnapshot(InputStream inputStream)
      throws IOException, ClassNotFoundException, CacheWriterException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void localClear() {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  @Override
  public void localDestroy(Object key) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  @Override
  public void localDestroy(Object key, Object callbackArgument) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  @Override
  public void localDestroyRegion() {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  @Override
  public void localDestroyRegion(Object callbackArgument) {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  @Override
  public void localInvalidate(Object key) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  @Override
  public void localInvalidate(Object key, Object callbackArgument) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  @Override
  public void localInvalidateRegion() {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  @Override
  public void localInvalidateRegion(Object callbackArgument) {
    throw new UnsupportedOperationException(
        "Local operations are not supported when multiuser-authentication is true.");
  }

  @Override
  public Object put(Object key, Object value) throws TimeoutException, CacheWriterException {
    try {
      preOp();
      return realRegion.put(key, value);
    } finally {
      postOp();
    }
  }

  @Override
  public Object put(Object key, Object value, Object callbackArgument)
      throws TimeoutException, CacheWriterException {
    try {
      preOp();
      return realRegion.put(key, value, callbackArgument);
    } finally {
      postOp();
    }
  }

  @Override
  public void putAll(Map map) {
    putAll(map, null);
  }

  @Override
  public void putAll(Map map, Object callbackArg) {
    try {
      preOp();
      realRegion.putAll(map, callbackArg);
    } finally {
      postOp();
    }
  }

  @Override
  public SelectResults query(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    try {
      preOp();
      return realRegion.query(queryPredicate);
    } finally {
      postOp();
    }
  }

  @Override
  public void registerInterest(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterest(Object key, boolean isDurable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterest(Object key, boolean isDurable, boolean receiveValues) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex, boolean isDurable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex, boolean isDurable, boolean receiveValues) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object remove(Object key) {
    try {
      preOp();
      return realRegion.remove(key);
    } finally {
      postOp();
    }
  }

  @Override
  public void saveSnapshot(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object selectValue(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    try {
      preOp();
      return realRegion.selectValue(queryPredicate);
    } finally {
      postOp();
    }
  }

  @Override
  public void setUserAttribute(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    try {
      preOp();
      return realRegion.size();
    } finally {
      postOp();
    }
  }

  @Override
  public Set subregions(boolean recursive) {
    return realRegion.subregions(recursive);
  }

  @Override
  public void unregisterInterest(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unregisterInterestRegex(String regex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection values() {
    try {
      preOp();
      return realRegion.values();
    } finally {
      postOp();
    }
  }

  @Override
  public void writeToDisk() {
    throw new UnsupportedOperationException();
  }

  private void preOp() {
    if (proxyCache.isClosed()) {
      throw proxyCache.getCacheClosedException("Cache is closed for this user.");
    }
    UserAttributes.userAttributes.set(proxyCache.getUserAttributes());
  }

  private void postOp() {
    proxyCache.setUserAttributes(UserAttributes.userAttributes.get());
    UserAttributes.userAttributes.set(null);
  }

  public Region getRealRegion() {
    return realRegion;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
   */
  @Override
  public Object putIfAbsent(Object key, Object value) {
    try {
      preOp();
      return realRegion.putIfAbsent(key, value);
    } finally {
      postOp();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
   */
  @Override
  public boolean remove(Object key, Object value) {
    try {
      preOp();
      return realRegion.remove(key, value);
    } finally {
      postOp();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
   */
  @Override
  public Object replace(Object key, Object value) {
    try {
      preOp();
      return realRegion.replace(key, value);
    } finally {
      postOp();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object,
   * java.lang.Object)
   */
  @Override
  public boolean replace(Object key, Object oldValue, Object newValue) {
    try {
      preOp();
      return realRegion.replace(key, oldValue, newValue);
    } finally {
      postOp();
    }
  }

  @Override
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
      realRegion.removeAll(keys, aCallbackArgument);
    } finally {
      postOp();
    }
  }
}
