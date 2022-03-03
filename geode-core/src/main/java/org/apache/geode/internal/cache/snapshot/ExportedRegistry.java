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
package org.apache.geode.internal.cache.snapshot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.geode.DataSerializer;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * Captures the pdx types and enums to be exported.
 *
 */
public class ExportedRegistry {
  /** the types */
  private final Map<Integer, PdxType> types;

  /** the enums */
  private final Map<Integer, EnumInfo> enums;

  public ExportedRegistry() {
    types = new HashMap<>();
    enums = new HashMap<>();
  }

  public ExportedRegistry(TypeRegistry tr) {
    types = new HashMap<>(tr.typeMap());
    enums = new HashMap<>(tr.enumMap());
  }

  public Map<Integer, PdxType> types() {
    return types;
  }

  public Map<Integer, EnumInfo> enums() {
    return enums;
  }

  public void addType(int id, PdxType type) {
    types.put(id, type);
  }

  public void addEnum(int id, EnumInfo info) {
    enums.put(id, info);
  }

  public PdxType getType(int id) {
    return types.get(id);
  }

  public EnumInfo getEnum(int id) {
    return enums.get(id);
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(types.size());
    for (Entry<Integer, PdxType> entry : types.entrySet()) {
      DataSerializer.writeObject(entry.getValue(), out);
    }

    out.writeInt(enums.size());
    for (Entry<Integer, EnumInfo> entry : enums.entrySet()) {
      out.writeInt(entry.getKey());
      DataSerializer.writeObject(entry.getValue(), out);
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int typeCount = in.readInt();
    for (int i = 0; i < typeCount; i++) {
      PdxType type = DataSerializer.readObject(in);
      types.put(type.getTypeId(), type);
    }

    int enumCount = in.readInt();
    for (int i = 0; i < enumCount; i++) {
      int id = in.readInt();
      EnumInfo ei = DataSerializer.readObject(in);
      enums.put(id, ei);
    }
  }
}
