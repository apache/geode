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


package com.gemstone.gemfire.internal.admin.remote;

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.RuntimeAdminException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.snapshot.RegionSnapshotService;
import com.gemstone.gemfire.internal.cache.snapshot.RegionSnapshotServiceImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
//import com.gemstone.gemfire.distributed.internal.*;
//import com.gemstone.gemfire.util.*;


import java.io.*;
//import java.net.*;
import java.util.*;
import java.util.concurrent.locks.Lock;

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
  public AdminRegion(String localName, RemoteGemFireVM vm,
                     String userAttributeDesc) {
    String gn = localName;
    int idx = localName.lastIndexOf(nameSep);
    if (idx != -1) {
      localName = localName.substring(idx+1);
    } else {
      gn = nameSep + gn;
    }
    this.globalName = gn;
    this.localName = localName;
    this.userAttributeDesc = userAttributeDesc;
    this.vm = vm;
  }

  public AdminRegion(String localName, AdminRegion parent, String userAttributeDesc) {
    this.localName = localName;
    this.userAttributeDesc = userAttributeDesc;
    String gn = parent.getFullPath() + nameSep + localName;
    this.globalName = gn;
    this.vm = parent.vm;
  }

  public String getName() {
    return localName;
  }

  public String getFullPath() {
    return globalName;
  }

  public Region getParentRegion() {
    throw new UnsupportedOperationException();
  }

  public RegionAttributes getAttributes() {
    try {
      RegionAttributesResponse resp = (RegionAttributesResponse)sendAndWait(RegionAttributesRequest.create());
      return resp.getRegionAttributes();
    } catch (CacheException c) {
      throw new RuntimeAdminException(c);
    }
  }

  public AttributesMutator getAttributesMutator() {
    throw new UnsupportedOperationException();
  }

  public CacheStatistics getStatistics() {
    try {
      RegionStatisticsResponse resp = (RegionStatisticsResponse)sendAndWait(RegionStatisticsRequest.create());
      return resp.getRegionStatistics();
    } catch (CacheException c) {
      throw new RuntimeAdminException(c);
    }
  }

  public void invalidateRegion() throws TimeoutException {
    sendAsync(DestroyRegionMessage.create(ExpirationAction.INVALIDATE));
  }

  public void invalidateRegion(Object aCallbackArgument) throws TimeoutException {
    throw new UnsupportedOperationException();
  }

  public void localInvalidateRegion() {
    sendAsync(DestroyRegionMessage.create(ExpirationAction.LOCAL_INVALIDATE));
  }

  public void localInvalidateRegion(Object aCallbackArgument) {
    throw new UnsupportedOperationException();
  }

  public void destroyRegion() throws CacheWriterException, TimeoutException {
    sendAsync(DestroyRegionMessage.create(ExpirationAction.DESTROY));
  }

  public void destroyRegion(Object aCallbackArgument) throws CacheWriterException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  public void localDestroyRegion() {
    sendAsync(DestroyRegionMessage.create(ExpirationAction.LOCAL_DESTROY));
  }

  public void localDestroyRegion(Object aCallbackArgument) {
    throw new UnsupportedOperationException();
  }

  public void close() {
    throw new UnsupportedOperationException();
  }

  public Region getSubregion(String regionName) {
    throw new UnsupportedOperationException();
  }

  public Region createSubregion(String subregionName, RegionAttributes aRegionAttributes)
    throws RegionExistsException, TimeoutException {
    throw new UnsupportedOperationException();
  }

   public Set subregions(boolean recursive) {
    if (recursive) {
      throw new UnsupportedOperationException();
    }
    try {
      SubRegionResponse resp = (SubRegionResponse)sendAndWait(SubRegionRequest.create());
      return resp.getRegionSet(this);
    } catch (CacheException c) {
      throw new RuntimeAdminException(c);
    }
  }

   public Entry getEntry(Object key) {
    try {
      ObjectDetailsResponse resp = (ObjectDetailsResponse)
        sendAndWait(ObjectDetailsRequest.create(key, vm.getCacheInspectionMode()));

      return new DummyEntry(this, key, resp.getObjectValue(),
                            resp.getUserAttribute(), resp.getStatistics());
    } catch (CacheException c) {
      throw new RuntimeAdminException(c);
    }
  }

  public Object get(Object key) throws CacheLoaderException, TimeoutException {
    throw new UnsupportedOperationException();
//     try {
//       ObjectValueResponse resp = (ObjectValueResponse)sendAndWait(ObjectValueRequest.create(key));
//       return resp.getObjectValue();
//     } catch (CacheException c) {
//       if (c instanceof CacheLoaderException) {
//         throw (CacheLoaderException)c;
//       } else if (c instanceof TimeoutException) {
//         throw (TimeoutException)c;
//       } else {
//         throw new RuntimeAdminException(c);
//       }
//     }
  }

  public Object get(Object key, Object aCallbackArgument)
    throws TimeoutException, CacheLoaderException {
     throw new UnsupportedOperationException();
  }

  public Object put(Object key, Object value)
    throws TimeoutException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public Object put(Object key, Object value, Object aCallbackArgument)
    throws TimeoutException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void create(Object key, Object value)
    throws TimeoutException, EntryExistsException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void create(Object key, Object value, Object aCacheWriterParam)
    throws TimeoutException, EntryExistsException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void invalidate(Object key)
    throws TimeoutException, EntryNotFoundException {
    sendAsync(DestroyEntryMessage.create(key, ExpirationAction.INVALIDATE));
  }

  public void invalidate(Object key, Object aCallbackArgument)
    throws TimeoutException, EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  public void localInvalidate(Object key) throws EntryNotFoundException {
    sendAsync(DestroyEntryMessage.create(key, ExpirationAction.LOCAL_INVALIDATE));
  }

  public void localInvalidate(Object key,Object aCallbackArgument)
    throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  public Object destroy(Object key)
    throws TimeoutException, EntryNotFoundException, CacheWriterException {
    sendAsync(DestroyEntryMessage.create(key, ExpirationAction.DESTROY));
    return null;
  }

  public Object destroy(Object key, Object aCacheWriterParam)
    throws TimeoutException, EntryNotFoundException, CacheWriterException {
    throw new UnsupportedOperationException();
  }

  public void localDestroy(Object key) throws EntryNotFoundException {
    sendAsync(DestroyEntryMessage.create(key, ExpirationAction.LOCAL_DESTROY));
  }

  public void localDestroy(Object key,Object aCallbackArgument) throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  public Set keys() {
    try {
      ObjectNamesResponse resp = (ObjectNamesResponse)sendAndWait(ObjectNamesRequest.create());
      return resp.getNameSet();
    } catch (CacheException ce) {
      throw new RuntimeAdminException(ce);
    }
  }

  public Collection values() {
    throw new UnsupportedOperationException();
  }

  public Set entries(boolean recursive) {
    throw new UnsupportedOperationException();
  }

  public Cache getCache() {
    throw new UnsupportedOperationException();
  }

  public RegionService getRegionService() {
    throw new UnsupportedOperationException();
  }

  public Object getUserAttribute() {
    return  userAttributeDesc;
  }

  public void setUserAttribute(Object value) {
    throw new UnsupportedOperationException();
  }

  public boolean isDestroyed() {
    throw new UnsupportedOperationException();
  }

  public void becomeLockGrantor() {
    throw new UnsupportedOperationException();
  }

  public boolean containsValueForKey(Object key) {
    throw new UnsupportedOperationException();
  }

  public boolean containsKey(Object key) {
    throw new UnsupportedOperationException();
  }

  public Lock getRegionDistributedLock() throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  public Lock getDistributedLock(Object key) throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  public void writeToDisk() {
    throw new UnsupportedOperationException(LocalizedStrings.AdminRegion_SHOULD_NOT_BE_CALLED.toLocalizedString());
  }

  public void registerInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void registerInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void registerInterest(Object key, InterestResultPolicy policy) throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void registerInterestRegex(String regex, InterestResultPolicy policy) throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void unregisterInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void unregisterInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void registerInterest(Object key, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }
  
  public void registerInterest(Object key, boolean isDurable,
      boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void registerInterestRegex(String regex, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }
  
  public void registerInterestRegex(String regex, boolean isDurable,
      boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }
  
  public void registerInterest(Object key, InterestResultPolicy policy,
      boolean isDurable, boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }
  
  public void registerInterestRegex(String regex, InterestResultPolicy policy,
      boolean isDurable, boolean receiveValues) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void unregisterInterest(Object key, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public void unregisterInterestRegex(String regex, boolean isDurable) throws CacheWriterException {
    throw new UnsupportedOperationException(
      LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }
  public List getInterestList() throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public List getInterestListRegex() throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public Set keySetOnServer() throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  public boolean containsKeyOnServer(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException(LocalizedStrings.UNSUPPORTED_AT_THIS_TIME.toLocalizedString());
  }

  @Override // GemStoneAddition
  public String toString() {
    return "<AdminRegion " + getFullPath() + ">";
  }

  //// AdminRegion methods

  /**
   * Returns a two element array, the first of which is the entry
   * count, the second is the subregion count
   */
  public int[] sizes() throws CacheException {
    RegionSizeResponse resp = (RegionSizeResponse)sendAndWait(RegionSizeRequest.create());
    return new int[]{resp.getEntryCount(), resp.getSubregionCount()};
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
        throw (CacheException)cause;
      } else if (cause instanceof CacheRuntimeException) {
        throw (CacheRuntimeException)cause;
      } else {
        throw ex;
      }
    }
  }

  void sendAsync(RegionAdminMessage msg) {
    msg.setRegionName(getFullPath());
    vm.sendAsync(msg);
  }

  public boolean existsValue(String predicate) {
    throw new UnsupportedOperationException();
  }

  public SelectResults query(String predicate) {
    throw new UnsupportedOperationException();
  }

  public Object selectValue(String predicate) {
    throw new UnsupportedOperationException();
  }

  public void loadSnapshot(InputStream inputStream)
  throws IOException, ClassNotFoundException, CacheWriterException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  public void saveSnapshot(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException();
  }

  public int size() {
    throw new UnsupportedOperationException();
  }

  public void clear() {
    throw new UnsupportedOperationException();
    
  }

  public boolean isEmpty() {
    throw new UnsupportedOperationException();
  }

  public boolean containsValue(Object arg0) {
    throw new UnsupportedOperationException();
  }

  public void putAll(Map arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map arg0, Object callbackArg) {
    throw new UnsupportedOperationException();
  }

  public Map getAll(Collection keys) {
    throw new UnsupportedOperationException(); 
  }

  public Map getAll(Collection keys, Object callback) {
    throw new UnsupportedOperationException(); 
  }

  public Set entrySet() {
    throw new UnsupportedOperationException();
  }

  public Set keySet() {
    throw new UnsupportedOperationException();
  }

   public Object remove(Object arg0) {
     throw new UnsupportedOperationException();
  }

  public Set entrySet(boolean recursive) {
    throw new UnsupportedOperationException("Not implemented yet");
//    return entries(recursive);
  }

  public void localClear() {
    throw new UnsupportedOperationException();
    
  }

  public void forceRolling(){
    throw new UnsupportedOperationException();
  }

  public int[] forceCompaction() {   
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
   */
  public Object putIfAbsent(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
   */
  public boolean remove(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
   */
  public Object replace(Object key, Object value) {
    throw new UnsupportedOperationException();
  }
  
  /* (non-Javadoc)
   * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object, java.lang.Object)
   */
  public boolean replace(Object key, Object oldValue, Object newValue) {
    throw new UnsupportedOperationException();
  }

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
