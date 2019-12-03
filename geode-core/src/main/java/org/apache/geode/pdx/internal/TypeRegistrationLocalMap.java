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

class TypeRegistrationLocalMap {
  final Map<Integer, PdxType> idToType = Collections.synchronizedMap(new HashMap<>());
  final Map<EnumId, EnumInfo> idToEnum = Collections.synchronizedMap(new HashMap<>());

  void save(Object key, Object value) {
    if (value instanceof PdxType) {
      PdxType type = (PdxType) value;
      idToType.put((Integer) key, type);
    } else if (value instanceof EnumInfo) {
      EnumInfo info = (EnumInfo) value;
      idToEnum.put((EnumId) key, info);
    }
  }

  void flushEnumCache() {
    synchronized (idToEnum) {
      idToEnum.values().forEach(EnumInfo::flushCache);
    }
  }

  void clear() {
    idToType.clear();
    idToEnum.clear();
  }

  int idToTypeSize() {
    return idToType.size();
  }

  int idToEnumSize() {
    return idToEnum.size();
  }

  PdxType getType(Integer id) {
    return idToType.get(id);
  }

  EnumInfo getEnum(EnumId id) {
    return idToEnum.get(id);
  }
}
