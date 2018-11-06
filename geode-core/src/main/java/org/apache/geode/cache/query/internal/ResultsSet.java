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

package org.apache.geode.cache.query.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;

// @todo probably should assert element type when elements added
/**
 * Implementation of SelectResults that extends HashSet If the elements are Structs, then use a
 * StructSet instead.
 *
 * @since GemFire 4.0
 */
public class ResultsSet extends HashSet implements SelectResults, DataSerializableFixedID {
  private static final long serialVersionUID = -5423281031630216824L;
  private ObjectType elementType;

  /**
   * Empty constructor to satisfy <code>DataSerializer</code> requirements
   */
  public ResultsSet() {}

  ResultsSet(Collection c) {
    super(c);
  }

  public ResultsSet(SelectResults sr) {
    super(sr);
    // grab type info
    setElementType(sr.getCollectionType().getElementType());
  }

  public ResultsSet(ObjectType elementType) {
    super();
    setElementType(elementType);
  }

  ResultsSet(ObjectType elementType, int initialCapacity) {
    super(initialCapacity);
    setElementType(elementType);
  }

  ResultsSet(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  ResultsSet(int initialCapacity) {
    super(initialCapacity);
  }

  public void setElementType(ObjectType elementType) {
    if (elementType instanceof StructType)
      throw new IllegalArgumentException(
          "This collection does not support struct elements");
    this.elementType = elementType;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ResultsSet)) {
      return false;
    }
    if (!this.elementType.equals(((ResultsSet) other).elementType)) {
      return false;
    }
    return super.equals(other);
  }


  public List asList() {
    return new ArrayList(this);
  }

  public Set asSet() {
    return this;
  }

  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(Set.class, this.elementType);
  }

  public boolean isModifiable() {
    return true;
  }

  public int occurrences(Object element) {
    return contains(element) ? 1 : 0;
  }


  public int getDSFID() {
    return RESULTS_SET;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    ObjectTypeImpl clt = new ObjectTypeImpl();
    InternalDataSerializer.invokeFromData(clt, in);
    setElementType(clt);
    for (int k = size; k > 0; k--) {
      this.add(DataSerializer.readObject(in));
    }
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(size());
    ObjectTypeImpl ctImpl = (ObjectTypeImpl) this.getCollectionType().getElementType();
    Assert.assertTrue(ctImpl != null, "ctImpl can not be null");
    InternalDataSerializer.invokeToData(ctImpl, out);
    for (Iterator itr = this.iterator(); itr.hasNext();) {
      DataSerializer.writeObject(itr.next(), out);
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
