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

class PeerTypeRegistrationLocalMap extends TypeRegistrationLocalMap {
  private final Map<Integer, PdxType> pendingIdToType =
      Collections.synchronizedMap(new HashMap<>());
  private final Map<EnumId, EnumInfo> pendingIdToEnum =
      Collections.synchronizedMap(new HashMap<>());

  void saveToPending(Object key, Object value) {
    if (value instanceof PdxType) {
      PdxType type = (PdxType) value;
      pendingIdToType.put((Integer) key, type);
    } else if (value instanceof EnumInfo) {
      EnumInfo info = (EnumInfo) value;
      pendingIdToEnum.put((EnumId) key, info);
    }
  }

  void flushPendingLocalMap() {
    if (!pendingIdToType.isEmpty()) {
      idToType.putAll(pendingIdToType);
      pendingIdToType.clear();
    }
    if (!pendingIdToEnum.isEmpty()) {
      idToEnum.putAll(pendingIdToEnum);
      pendingIdToEnum.clear();
    }
  }

  @Override
  void flushEnumCache() {
    super.flushEnumCache();
    synchronized (pendingIdToEnum) {
      pendingIdToEnum.values().forEach(EnumInfo::flushCache);
    }
  }

  // This should only be called prior to reloading the maps from the region
  @Override
  void clear() {
    super.clear();
    pendingIdToType.clear();
    pendingIdToEnum.clear();
  }

  @VisibleForTesting
  int pendingTypeToIdSize() {
    return pendingIdToType.size();
  }

  @VisibleForTesting
  int pendingEnumToIdSize() {
    return pendingIdToEnum.size();
  }

}
