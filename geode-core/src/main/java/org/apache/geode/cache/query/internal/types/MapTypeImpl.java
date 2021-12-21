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
import org.apache.geode.cache.query.types.MapType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;


/**
 * Implementation of CollectionType
 *
 * @since GemFire 4.0
 */
public class MapTypeImpl extends CollectionTypeImpl implements MapType {
  private static final long serialVersionUID = -705688605389537058L;
  private ObjectType keyType;

  /**
   * Empty constructor to satisfy <code>DataSerializer</code> requirements
   */
  public MapTypeImpl() {}

  /** Creates a new instance of ObjectTypeImpl */
  public MapTypeImpl(Class clazz, ObjectType keyType, ObjectType valueType) {
    super(clazz, valueType);
    this.keyType = keyType;
  }

  public MapTypeImpl(String className, ObjectType keyType, ObjectType valueType)
      throws ClassNotFoundException {
    super(className, valueType);
    this.keyType = keyType;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj) && (obj instanceof MapTypeImpl)
        && keyType.equals(((MapTypeImpl) obj).keyType);
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ keyType.hashCode();
  }

  @Override
  public String toString() {
    return resolveClass().getName() + "<key:" + keyType.resolveClass().getName() + ",value:"
        + getElementType().resolveClass().getName() + ">";
  }

  @Override
  public boolean isMapType() {
    return true;
  }

  @Override
  public ObjectType getKeyType() {
    return keyType;
  }

  @Override
  public StructType getEntryType() {
    ObjectType[] fieldTypes = new ObjectType[] {keyType, getElementType()};
    return new StructTypeImpl(new String[] {"key", "value"}, fieldTypes);
  }

  @Override
  public int getDSFID() {
    return MAP_TYPE_IMPL;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    keyType = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(keyType, out);
  }
}
