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

package org.apache.geode.cache.query.internal.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Implementation of ObjectType
 *
 * @since GemFire 4.0
 */
public class ObjectTypeImpl implements ObjectType, DataSerializableFixedID {
  private static final long serialVersionUID = 3327357282163564784L;
  private Class clazz;

  /**
   * Empty constructor to satisfy <code>DataSerializer</code> requirements
   */
  public ObjectTypeImpl() {}

  /** Creates a new instance of ObjectTypeImpl */
  public ObjectTypeImpl(Class clazz) {
    this.clazz = clazz;
  }

  public ObjectTypeImpl(String className) throws ClassNotFoundException {
    clazz = InternalDataSerializer.getCachedClass(className);
  }

  @Override
  public Class resolveClass() {
    return clazz;
  }

  @Override
  public String getSimpleClassName() {
    String cn = clazz.getName();
    int i = cn.lastIndexOf('.');
    return i < 0 ? cn : cn.substring(i + 1);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ObjectTypeImpl)) {
      return false;
    }
    return clazz == ((ObjectTypeImpl) obj).clazz;
  }

  @Override
  public int hashCode() {
    return clazz.hashCode();
  }

  @Override
  public String toString() {
    return clazz.getName();
  }

  @Override
  public boolean isCollectionType() {
    return false;
  }

  @Override
  public boolean isMapType() {
    return false;
  }

  @Override
  public boolean isStructType() {
    return false;
  }

  @Override
  public int getDSFID() {
    return OBJECT_TYPE_IMPL;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    toData(out, InternalDataSerializer.createSerializationContext(out));
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in, InternalDataSerializer.createDeserializationContext(in));
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    clazz = DataSerializer.readClass(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeClass(clazz, out);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
