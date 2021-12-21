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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.internal.Ordered;
import org.apache.geode.cache.query.internal.ResultsSet;
import org.apache.geode.cache.query.internal.SortedResultSet;
import org.apache.geode.cache.query.internal.SortedStructSet;
import org.apache.geode.cache.query.internal.StructSet;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Implementation of CollectionType
 *
 * @since GemFire 4.0
 */
public class CollectionTypeImpl extends ObjectTypeImpl implements CollectionType {
  private static final long serialVersionUID = 892402666471396897L;
  private ObjectType elementType;

  /**
   * Empty constructor to satisfy <code>DataSerializer</code> requirements
   */
  public CollectionTypeImpl() {}

  /** Creates a new instance of ObjectTypeImpl */
  public CollectionTypeImpl(Class clazz, ObjectType elementType) {
    super(clazz);
    this.elementType = elementType;
  }

  public CollectionTypeImpl(String className, ObjectType elementType)
      throws ClassNotFoundException {
    super(className);
    this.elementType = elementType;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj) && (obj instanceof CollectionTypeImpl)
        && elementType.equals(((CollectionTypeImpl) obj).elementType);
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ elementType.hashCode();
  }

  @Override
  public String toString() {
    return resolveClass().getName() + "<" + elementType.resolveClass().getName() + ">";
  }

  @Override
  public boolean allowsDuplicates() {
    Class cls = resolveClass();
    return !Set.class.isAssignableFrom(cls) && !Map.class.isAssignableFrom(cls)
        && !Region.class.isAssignableFrom(cls) && !StructSet.class.isAssignableFrom(cls)
        && !SortedStructSet.class.isAssignableFrom(cls)
        && !SortedResultSet.class.isAssignableFrom(cls) && !ResultsSet.class.isAssignableFrom(cls);
  }

  @Override
  public ObjectType getElementType() {
    return elementType;
  }

  @Override
  public boolean isOrdered() {
    Class cls = resolveClass();
    return List.class.isAssignableFrom(cls) || cls.isArray() || Ordered.class.isAssignableFrom(cls)
        || TreeSet.class.isAssignableFrom(cls) || TreeMap.class.isAssignableFrom(cls)
        || LinkedHashSet.class.isAssignableFrom(cls) || LinkedHashMap.class.isAssignableFrom(cls);
  }

  @Override
  public boolean isCollectionType() {
    return true;
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
    return COLLECTION_TYPE_IMPL;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    elementType = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(elementType, out);
  }
}
