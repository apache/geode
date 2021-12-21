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


package org.apache.geode.internal.admin.remote;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.geode.admin.RuntimeAdminException;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.snapshot.RegionSnapshotService;

/**
 * Represents a snapshot of a {@link Region}.
 */
public class AdminRegion implements Region {
  // instance variables
  private final String globalName;
  private final String localName;
  private final String userAttributeDesc;
  private final RemoteGemFireVM vm;

  private static final char nameSep = Region.SEPARATOR_CHAR;

  /**
   * Creates a root region
   */
  public AdminRegion(String localName, RemoteGemFireVM vm, String userAttributeDesc) {
    String gn = localName;
    int idx = localName.lastIndexOf(nameSep);
    if (idx != -1) {
      localName = localName.substring(idx + 1);
    } else {
      gn = nameSep + gn;
    }
    globalName = gn;
    this.localName = localName;
    this.userAttributeDesc = userAttributeDesc;
    this.vm = vm;
  }

  public AdminRegion(String localName, AdminRegion parent, String userAttributeDesc) {
    this.localName = localName;
    this.userAttributeDesc = userAttributeDesc;
    String gn = parent.getFullPath() + nameSep + localName;
    globalName = gn;
    vm = parent.vm;
  }

  @Override
  public String getName() {
    return localName;
  }

  @Override
  public String getFullPath() {
    return globalName;
  }

  @Override
  public Region getParentRegion() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RegionAttributes getAttributes() {
    try {
      RegionAttributesResponse resp =
          (RegionAttributesResponse) sendAndWait(RegionAttributesRequest.create());
      return resp.getRegionAttributes();
    } catch (CacheException c) {
      throw new RuntimeAdminException(c);
    }
  }

  @Override
  public AttributesMutator getAttributesMutator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CacheStatistics getStatistics() {
    try {
      RegionStatisticsResponse resp =
          (RegionStatisticsResponse) sendAndWait(RegionStatisticsRequest.create());
      return resp.getRegionStatistics();
    } catch (CacheException c) {
      throw new RuntimeAdminException(c);
    }
  }

  @Override
  public void invalidateRegion() throws TimeoutException {
    sendAsync(DestroyRegionMessage.create(ExpirationAction.INVALIDATE));
  }

  @Override
  public void invalidateRegion(Object aCallbackArgument) throws TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void localInvalidateRegion() {
    sendAsync(DestroyRegionMessage.create(ExpirationAction.LOCAL_INVALIDATE));
  }

  @Override
  public void localInvalidateRegion(Object aCallbackArgument) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void destroyRegion() throws CacheWriterException, TimeoutException {
    sendAsync(DestroyRegionMessage.create(ExpirationAction.DESTROY));
  }

  @Override
  public void destroyRegion(Object aCallbackArgument)
      throws CacheWriterException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void localDestroyRegion() {
    sendAsync(DestroyRegionMessage.create(ExpirationAction.LOCAL_DESTROY));
  }

  @Override
  public void localDestroyRegion(Object aCallbackArgument) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Region getSubregion(String regionName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Region createSubregion(String subregionName, RegionAttributes aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set subregions(boolean recursive) {
    if (recursive) {
      throw new UnsupportedOperationException();
    }
    try {
      SubRegionResponse resp = (SubRegionResponse) sendAndWait(SubRegionRequest.create());
      return resp.getRegionSet(this);
    } catch (CacheException c) {
      throw new RuntimeAdminException(c);
    }
  }

  @Override
  public Entry getEntry(Object key) {
    try {
      ObjectDetailsResponse resp = (ObjectDetailsResponse) sendAndWait(
          ObjectDetailsRequest.create(key, vm.getCacheInspectionMode()));

      return new DummyEntry(this, key, resp.getObjectValue(), resp.getUserAttribute(),
          resp.getStatistics());
    } catch (CacheException c) {
      throw new RuntimeAdminException(c);
    }
  }

  @Override
  public Object get(Object key) throws CacheLoaderException, TimeoutException {
    throw new UnsupportedOperationException();
    // try {
    // ObjectValueResponse resp = (ObjectValueResponse)sendAndWait(ObjectValueRequest.create(key));
    // return resp.getObjectValue();
    // } catch (CacheException c) {
    // if (c instanceof CacheLoaderException) {
    // throw (CacheLoaderException)c;
    // } else if (c instanceof TimeoutException) {
    // throw (TimeoutException)c;
    // } else {
    // throw new RuntimeAdminException(c);
    // }
    // }
  }

  @Override
  public Object get(Object key, Object aCallbackArgument)
      throws TimeoutException, CacheLoaderException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object put(Object key, Object value) throws TimeoutException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object put(Object key, Object value, Object aCallbackArgument)
      throws TimeoutException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void create(Object key, Object value)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void create(Object key, Object value, Object aCacheWriterParam)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void invalidate(Object key) throws TimeoutException, EntryNotFoundException {
    sendAsync(DestroyEntryMessage.create(key, ExpirationAction.INVALIDATE));
  }

  @Override
  public void invalidate(Object key, Object aCallbackArgument)
      throws TimeoutException, EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void localInvalidate(Object key) throws EntryNotFoundException {
    sendAsync(DestroyEntryMessage.create(key, ExpirationAction.LOCAL_INVALIDATE));
  }

  @Override
  public void localInvalidate(Object key, Object aCallbackArgument) throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object destroy(Object key)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    sendAsync(DestroyEntryMessage.create(key, ExpirationAction.DESTROY));
    return null;
  }

  @Override
  public Object destroy(Object key, Object aCacheWriterParam)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void localDestroy(Object key) throws EntryNotFoundException {
    sendAsync(DestroyEntryMessage.create(key, ExpirationAction.LOCAL_DESTROY));
  }

  @Override
  public void localDestroy(Object key, Object aCallbackArgument) throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set keySet() {
    try {
      ObjectNamesResponse resp = (ObjectNamesResponse) sendAndWait(ObjectNamesRequest.create());
      return resp.getNameSet();
    } catch (CacheException ce) {
      throw new RuntimeAdminException(ce);
    }
  }

  @Override
  public Collection values() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Cache getCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RegionService getRegionService() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getUserAttribute() {
    return userAttributeDesc;
  }

  @Override
  public void setUserAttribute(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDestroyed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void becomeLockGrantor() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsValueForKey(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsKey(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Lock getRegionDistributedLock() throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Lock getDistributedLock(Object key) throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeToDisk() {
    throw new UnsupportedOperationException(
        "Should not be called");
  }

  @Override
  public void registerInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy)
      throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy)
      throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void unregisterInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void unregisterInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterest(Object key, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterest(Object key, boolean isDurable, boolean receiveValues)
      throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterestRegex(String regex, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterestRegex(String regex, boolean isDurable, boolean receiveValues)
      throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable)
      throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable)
      throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public void unregisterInterest(Object key, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public void unregisterInterestRegex(String regex, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public List getInterestList() throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public List getInterestListRegex() throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public Set keySetOnServer() throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public boolean containsKeyOnServer(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public int sizeOnServer() throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override
  public boolean isEmptyOnServer() throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  @Override // GemStoneAddition
  public String toString() {
    return "<AdminRegion " + getFullPath() + ">";
  }

  //// AdminRegion methods

  /**
   * Returns a two element array, the first of which is the entry count, the second is the subregion
   * count
   */
  public int[] sizes() throws CacheException {
    RegionSizeResponse resp = (RegionSizeResponse) sendAndWait(RegionSizeRequest.create());
    return new int[] {resp.getEntryCount(), resp.getSubregionCount()};
  }

  // Additional instance methods
  /**
   * Sends an AdminRequest to this application's vm and waits for the AdminReponse
   */
  AdminResponse sendAndWait(RegionAdminRequest msg) throws CacheException {
    msg.setRegionName(getFullPath());
    try {
      return vm.sendAndWait(msg);
    } catch (RuntimeAdminException ex) {
      Throwable cause = ex.getRootCause();
      if (cause instanceof CacheException) {
        throw (CacheException) cause;
      } else if (cause instanceof CacheRuntimeException) {
        throw (CacheRuntimeException) cause;
      } else {
        throw ex;
      }
    }
  }

  void sendAsync(RegionAdminMessage msg) {
    msg.setRegionName(getFullPath());
    vm.sendAsync(msg);
  }

  @Override
  public boolean existsValue(String predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectResults query(String predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object selectValue(String predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadSnapshot(InputStream inputStream)
      throws IOException, ClassNotFoundException, CacheWriterException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void saveSnapshot(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsValue(Object arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map arg0, Object callbackArg) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map getAll(Collection keys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map getAll(Collection keys, Object callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set entrySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object remove(Object arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set entrySet(boolean recursive) {
    throw new UnsupportedOperationException("Not implemented yet");
    // return entries(recursive);
  }

  @Override
  public void localClear() {
    throw new UnsupportedOperationException();

  }

  @Override
  public void forceRolling() {
    throw new UnsupportedOperationException();
  }

  public int[] forceCompaction() {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
   */
  @Override
  public Object putIfAbsent(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
   */
  @Override
  public boolean remove(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
   */
  @Override
  public Object replace(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object,
   * java.lang.Object)
   */
  @Override
  public boolean replace(Object key, Object oldValue, Object newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RegionSnapshotService<?, ?> getSnapshotService() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeAll(Collection keys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeAll(Collection keys, Object aCallbackArgument) {
    throw new UnsupportedOperationException();
  }

}
