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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.locks.TXRegionLockRequest;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * TXRegionLockRequest represents all the locks that need to be made for a single region.
 *
 * @since GemFire 4.0
 */
public class TXRegionLockRequestImpl implements TXRegionLockRequest {
  private static final long serialVersionUID = 5840033961584078082L;
  private static final Logger logger = LogService.getLogger();

  private transient InternalCache cache;

  private transient InternalRegion r;

  private String regionPath;

  private Map<Object, Boolean> entryKeys;

  public TXRegionLockRequestImpl() {
    // for DataSerializer
    cache = null;
  }

  public TXRegionLockRequestImpl(InternalCache cache, InternalRegion r) {
    this.cache = cache;
    this.r = r;
    regionPath = null;
    entryKeys = null;
  }

  /**
   * Used by unit tests
   */
  @VisibleForTesting
  public TXRegionLockRequestImpl(String regionPath, Map<Object, Boolean> entryKeys) {
    cache = null;
    this.regionPath = regionPath;
    this.entryKeys = entryKeys;
  }

  public boolean isEmpty() {
    return entryKeys == null || entryKeys.isEmpty();
  }

  @Override
  public void addEntryKeys(Map<Object, Boolean> map) {
    if (map == null || map.isEmpty()) {
      return;
    }
    if (entryKeys == null) {
      // Create new temporary HashMap. Fix for defect # 44472.
      final HashMap<Object, Boolean> tmp = new HashMap<>(map.size());
      tmp.putAll(map);
      entryKeys = tmp;

    } else {
      // Need to make a copy so we can do a union
      final HashMap<Object, Boolean> tmp =
          new HashMap<>(entryKeys.size() + map.size());
      tmp.putAll(entryKeys);
      entryKeys = tmp;
      for (Map.Entry<Object, Boolean> entry : map.entrySet()) {
        addEntryKey(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public void addEntryKey(Object key, Boolean isEvent) {
    if (entryKeys == null) {
      entryKeys = new HashMap<>();
    }
    if (!entryKeys.getOrDefault(key, Boolean.FALSE)) {
      entryKeys.put(key, isEvent);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    regionPath = DataSerializer.readString(in);

    cache = GemFireCacheImpl.getInstance();
    try {
      final int size = InternalDataSerializer.readArrayLength(in);
      if (cache != null && size > 0) {
        r = (LocalRegion) cache.getRegion(regionPath);
      }
      if (StaticSerialization.getVersionForDataStream(in)
          .isNotOlderThan(KnownVersion.GEODE_1_10_0)) {
        entryKeys = readEntryKeyMap(size, in);
      } else {
        entryKeys = readEntryKeySet(size, in);
      }

    } catch (CacheClosedException ignore) {
      // don't throw in deserialization
      entryKeys = null;
    }
  }

  private Map<Object, Boolean> readEntryKeyMap(final int size, final DataInput in)
      throws IOException, ClassNotFoundException {

    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading HashMap with size {}", size);
    }
    if (size == -1) {
      return null;
    }

    final HashMap<Object, Boolean> map = new HashMap<>(size);
    Object key;
    Boolean value;
    for (int i = 0; i < size; i++) {
      key = DataSerializer.readObject(in);
      value = DataSerializer.readObject(in);
      map.put(key, value);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Read HashMap with {} elements: {}", size, map);
    }

    return map;
  }

  private Map<Object, Boolean> readEntryKeySet(final int size, final DataInput in)
      throws IOException, ClassNotFoundException {

    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading HashSet with size {}", size);
    }

    final HashMap<Object, Boolean> map = new HashMap<>(size);
    Object key;
    Boolean value;
    for (int i = 0; i < size; i++) {
      key = DataSerializer.readObject(in);
      value = true;
      map.put(key, value);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Read HashSet with {} elements: {}", size, map);
    }

    return map;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(getRegionFullPath(), out);
    if (StaticSerialization.getVersionForDataStream(out)
        .isNotOlderThan(KnownVersion.GEODE_1_10_0)) {
      InternalDataSerializer.writeHashMap(entryKeys, out);
    } else {
      HashSet hashset = new HashSet(entryKeys.keySet());
      InternalDataSerializer.writeHashSet(hashset, out);
    }
  }

  public static TXRegionLockRequestImpl createFromData(DataInput in)
      throws IOException, ClassNotFoundException {
    TXRegionLockRequestImpl result = new TXRegionLockRequestImpl();
    InternalDataSerializer.invokeFromData(result, in);
    return result;
  }

  @Override
  public String getRegionFullPath() {
    if (regionPath == null) {
      regionPath = r.getFullPath();
    }
    return regionPath;
  }

  @Override
  public Map<Object, Boolean> getKeys() {
    if (entryKeys == null) {
      // check for cache closed/closing
      cache.getCancelCriterion().checkCancelInProgress(null);
    }
    return entryKeys;
  }

  /**
   * Only safe to call in the vm that creates this request. Once it is serialized this method will
   * return null.
   */
  public InternalRegion getLocalRegion() {
    return r;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder(256);
    result.append("regionPath=").append(getRegionFullPath()).append(" keys=")
        .append(entryKeys);
    return result.toString();
  }
}
