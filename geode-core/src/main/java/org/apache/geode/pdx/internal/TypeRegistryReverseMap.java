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

class TypeRegistryReverseMap {
  /**
   * These maps allow revere look-ups of the idToType and idToEnum maps (or the idToType region
   * if the TypeRegistration containing them is a PeerTypeRegistration) without having to iterate
   * through the entry set
   */
  final Map<PdxType, Integer> typeToId = Collections.synchronizedMap(new HashMap<>());
  final Map<EnumInfo, EnumId> enumToId = Collections.synchronizedMap(new HashMap<>());

  void save(Object key, Object value) {
    if (value instanceof PdxType) {
      PdxType type = (PdxType) value;
      typeToId.put(type, (Integer) key);
    } else if (value instanceof EnumInfo) {
      EnumInfo info = (EnumInfo) value;
      enumToId.put(info, (EnumId) key);
    }
  }

  void clear() {
    typeToId.clear();
    enumToId.clear();
  }

  int typeToIdSize() {
    return typeToId.size();
  }

  int enumToIdSize() {
    return enumToId.size();
  }

  Integer getIdFromReverseMap(PdxType newType) {
    return typeToId.get(newType);
  }

  EnumId getIdFromReverseMap(EnumInfo newInfo) {
    return enumToId.get(newInfo);
  }
}
