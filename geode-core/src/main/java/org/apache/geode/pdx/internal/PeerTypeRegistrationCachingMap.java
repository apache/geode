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
package org.apache.geode.pdx.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;

class PeerTypeRegistrationCachingMap extends TypeRegistrationCachingMap {
  private final Map<Integer, PdxType> pendingIdToType =
      Collections.synchronizedMap(new HashMap<>());
  private final Map<PdxType, Integer> pendingTypeToId =
      Collections.synchronizedMap(new HashMap<>());
  private final Map<EnumId, EnumInfo> pendingIdToEnum =
      Collections.synchronizedMap(new HashMap<>());
  private final Map<EnumInfo, EnumId> pendingEnumToId =
      Collections.synchronizedMap(new HashMap<>());

  void saveToPending(Object key, Object value) {
    if (value instanceof PdxType) {
      PdxType type = (PdxType) value;
      pendingIdToType.put((Integer) key, type);
      pendingTypeToId.put(type, (Integer) key);
    } else if (value instanceof EnumInfo) {
      EnumInfo info = (EnumInfo) value;
      pendingIdToEnum.put((EnumId) key, info);
      pendingEnumToId.put(info, (EnumId) key);
    }
  }

  // The maps should only be loaded from the region if there is a mismatch in size between
  // the region and either of the local maps, which should only occur when initializing a new
  // PeerTypeRegistration in a system with an existing and not-empty PdxTypes region
  boolean shouldReloadFromRegion(Region<?, ?> pdxRegion) {
    if (pdxRegion == null) {
      return false;
    }
    int regionSize = pdxRegion.size();
    // These sizes should always be the same, but it doesn't hurt to check both
    int localMapsSize =
        idToType.size() + pendingIdToType.size() + idToEnum.size() + pendingIdToEnum.size();
    int localReverseMapSize =
        typeToId.size() + pendingTypeToId.size() + enumToId.size() + pendingEnumToId.size();
    return (regionSize != localMapsSize || regionSize != localReverseMapSize);
  }

  void flushPendingLocalMaps() {
    if (!pendingIdToType.isEmpty()) {
      idToType.putAll(pendingIdToType);
      pendingIdToType.clear();
    }
    if (!pendingTypeToId.isEmpty()) {
      typeToId.putAll(pendingTypeToId);
      pendingTypeToId.clear();
    }
    if (!pendingIdToEnum.isEmpty()) {
      idToEnum.putAll(pendingIdToEnum);
      pendingIdToEnum.clear();
    }
    if (!pendingEnumToId.isEmpty()) {
      enumToId.putAll(pendingEnumToId);
      pendingEnumToId.clear();
    }
  }

  @Override
  void flushEnumCache() {
    super.flushEnumCache();
    synchronized (pendingIdToEnum) {
      pendingIdToEnum.values().forEach(EnumInfo::flushCache);
    }
    synchronized (pendingEnumToId) {
      pendingEnumToId.keySet().forEach(EnumInfo::flushCache);
    }
  }

  // This should only be called prior to reloading the maps from the region
  @Override
  void clear() {
    super.clear();
    pendingIdToType.clear();
    pendingTypeToId.clear();
    pendingIdToEnum.clear();
    pendingEnumToId.clear();
  }

  @VisibleForTesting
  int pendingIdToTypeSize() {
    return pendingIdToType.size();
  }

  @VisibleForTesting
  int pendingTypeToIdSize() {
    return pendingTypeToId.size();
  }

  @VisibleForTesting
  int pendingIdToEnumSize() {
    return pendingIdToEnum.size();
  }

  @VisibleForTesting
  int pendingEnumToIdSize() {
    return pendingEnumToId.size();
  }

}
