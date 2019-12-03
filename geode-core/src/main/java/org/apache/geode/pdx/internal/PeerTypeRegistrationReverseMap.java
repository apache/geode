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

/**
 * This class serves two purposes. It lets us look up an id based on a type/enum, if we previously
 * found that type/enum in the region. And, if a type/enum is present in this map, that means we
 * read the type/enum while holding the dlock, which means the type/enum was distributed to all
 * members.
 */
class PeerTypeRegistrationReverseMap extends TypeRegistrationReverseMap {
  /**
   * When a new pdxType or a new enumInfo is added to idToType region, its
   * listener will add the new type to the pendingTypeToId first, to make sure
   * the distribution finished.
   * Then any member who wants to use this new pdxType has to get the dlock to
   * flush the pendingTypeToId map into typeToId. This design to guarantee that
   * when using the new pdxType, it should have been distributed to all members.
   */
  private final Map<PdxType, Integer> pendingTypeToId =
      Collections.synchronizedMap(new HashMap<>());
  private final Map<EnumInfo, EnumId> pendingEnumToId =
      Collections.synchronizedMap(new HashMap<>());

  void saveToPending(Object key, Object value) {
    if (value instanceof PdxType) {
      PdxType type = (PdxType) value;
      pendingTypeToId.put(type, (Integer) key);
    } else if (value instanceof EnumInfo) {
      EnumInfo info = (EnumInfo) value;
      pendingEnumToId.put(info, (EnumId) key);
    }
  }

  // The reverse maps should only be loaded from the region if there is a mismatch in size between
  // the region and all reverse maps, which should only occur when initializing a new
  // PeerTypeRegistration in a system with an existing and not-empty PdxTypes region
  boolean shouldReloadFromRegion(Region pdxRegion) {
    if (pdxRegion == null) {
      return false;
    }
    return ((typeToId.size() + pendingTypeToId.size() + enumToId.size()
        + pendingEnumToId.size()) != pdxRegion.size());
  }

  void flushPendingReverseMap() {
    if (!pendingTypeToId.isEmpty()) {
      typeToId.putAll(pendingTypeToId);
      pendingTypeToId.clear();
    }
    if (!pendingEnumToId.isEmpty()) {
      enumToId.putAll(pendingEnumToId);
      pendingEnumToId.clear();
    }
  }

  @Override
  void flushEnumCache() {
    super.flushEnumCache();
    synchronized (pendingEnumToId) {
      pendingEnumToId.keySet().forEach(EnumInfo::flushCache);
    }
  }

  // This should only be called prior to reloading the maps from the region
  @Override
  void clear() {
    super.clear();
    pendingTypeToId.clear();
    pendingEnumToId.clear();
  }

  @VisibleForTesting
  int pendingTypeToIdSize() {
    return pendingTypeToId.size();
  }

  @VisibleForTesting
  int pendingEnumToIdSize() {
    return pendingEnumToId.size();
  }
}
