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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/** TXRegionLockRequest represents all the locks that need to be made
 * for a single region.
 *
 * 
 * @since GemFire 4.0
 * 
 */
public class TXRegionLockRequestImpl
  implements com.gemstone.gemfire.internal.cache.locks.TXRegionLockRequest
{
  private static final long serialVersionUID = 5840033961584078082L;
  private static final Logger logger = LogService.getLogger();
  
  private transient LocalRegion r;
  private String regionPath;
  private Set<Object> entryKeys;

  public TXRegionLockRequestImpl() {
    // for DataSerializer
  }
  
  public TXRegionLockRequestImpl(LocalRegion r)
  {
    this.r = r;
    this.regionPath = null;
    this.entryKeys = null;
  }
  /**
   * Used by unit tests
   */
  public TXRegionLockRequestImpl(String regionPath, Set<Object> entryKeys) {
    this.regionPath = regionPath;
    this.entryKeys = entryKeys;
  }

  public boolean isEmpty() {
    return this.entryKeys == null || this.entryKeys.isEmpty();
  }

  public void addEntryKeys(Set<Object> s) {
    if (s == null || s.isEmpty()) {
      return;
    }
    if (this.entryKeys == null) {
      //Create new temporary HashSet. Fix for defect # 44472.
      final HashSet<Object> tmp = new HashSet<Object>(s.size());
      tmp.addAll(s);
      this.entryKeys = tmp;
    	
    } else {
      // Need to make a copy so we can do a union
      final HashSet<Object> tmp = new HashSet<Object>(this.entryKeys.size()
          + s.size());
      tmp.addAll(s);
      tmp.addAll(this.entryKeys);
      this.entryKeys = tmp;
    }
  }

  public void addEntryKey(Object key) {
    if (this.entryKeys == null) {
      this.entryKeys = new HashSet<Object>();
    }
    this.entryKeys.add(key);
  }
  
  public final void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    this.regionPath = DataSerializer.readString(in);

    final GemFireCacheImpl cache = getCache(false);
    try {
      final int size = InternalDataSerializer.readArrayLength(in);
      if (cache != null && size > 0) {
        this.r = (LocalRegion)cache.getRegion(this.regionPath);
      }
      this.entryKeys = readEntryKeySet(size, in);
    } catch (CacheClosedException cce) {
      // don't throw in deserialization
      this.entryKeys = null;
    }
  }
  
  private final Set<Object> readEntryKeySet(
      final int size, final DataInput in) throws IOException,
      ClassNotFoundException {

    if (logger.isDebugEnabled()) {
      logger.trace(LogMarker.SERIALIZER, "Reading HashSet with size {}", size);
    }

    final HashSet<Object> set = new HashSet<Object>(size);
    Object key;
    for (int i = 0; i < size; i++) {
      key = DataSerializer.readObject(in);
      set.add(key);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Read HashSet with {} elements: {}", size, set);
    }

    return set;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(getRegionFullPath(), out);
    InternalDataSerializer.writeSet(this.entryKeys, out);
  }

  public static final TXRegionLockRequestImpl createFromData(DataInput in) 
    throws IOException, ClassNotFoundException
  {
    TXRegionLockRequestImpl result = new TXRegionLockRequestImpl();
    InternalDataSerializer.invokeFromData(result, in);
    return result;
  }

  public final String getRegionFullPath() {
    if (this.regionPath == null) {
      this.regionPath = this.r.getFullPath();
    }
    return this.regionPath;
  }

  public final Set<Object> getKeys() {
    if (this.entryKeys == null) {
      // check for cache closed/closing
      getCache(true);
    }
    return this.entryKeys;
  }

  private final GemFireCacheImpl getCache(boolean throwIfClosing) {
    final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && !cache.isClosed()) {
      if (throwIfClosing) {
        cache.getCancelCriterion().checkCancelInProgress(null);
      }
      return cache;
    }
    if (throwIfClosing) {
      throw cache.getCacheClosedException("The cache is closed.", null);
    }
    return null;
  }

  /**
   * Only safe to call in the vm that creates this request.
   * Once it is serialized this method will return null.
   */
  public final LocalRegion getLocalRegion() {
    return this.r;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder(256);
    result.append("regionPath=")
      .append(getRegionFullPath())
      .append(" keys=")
      .append(this.entryKeys);
    return result.toString();
  }
}
