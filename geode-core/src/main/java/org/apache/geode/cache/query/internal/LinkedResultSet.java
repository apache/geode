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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

public class LinkedResultSet extends java.util.LinkedHashSet
    implements Ordered, SelectResults, DataSerializableFixedID {

  private static final long serialVersionUID = 5184711453750319225L;

  private ObjectType elementType;

  public LinkedResultSet() {}

  LinkedResultSet(Collection c) {
    super(c);
  }

  LinkedResultSet(SelectResults sr) {
    super(sr);
    setElementType(sr.getCollectionType().getElementType());
  }

  LinkedResultSet(ObjectType elementType) {
    super();
    setElementType(elementType);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof LinkedResultSet)) {
      return false;
    }
    if (!this.elementType.equals(((LinkedResultSet) other).elementType)) {
      return false;
    }
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    return this.elementType.hashCode();
  }

  @Override
  public void setElementType(ObjectType elementType) {
    if (elementType instanceof StructType) {
      throw new IllegalArgumentException(
          "This collection does not support struct elements");
    }
    this.elementType = elementType;
  }

  @Override
  public List asList() {
    return new ArrayList(this);
  }

  @Override
  public Set asSet() {
    return this;
  }

  @Override
  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(Ordered.class, this.elementType);
  }

  @Override
  public boolean isModifiable() {
    return true;
  }

  @Override
  public int occurrences(Object element) {
    return contains(element) ? 1 : 0;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    this.elementType = (ObjectType) context.getDeserializer().readObject(in);
    for (int j = size; j > 0; j--) {
      this.add(context.getDeserializer().readObject(in));
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    // how do we serialize the comparator?
    out.writeInt(this.size());
    context.getSerializer().writeObject(this.elementType, out);
    for (Iterator i = this.iterator(); i.hasNext();) {
      context.getSerializer().writeObject(i.next(), out);
    }
  }

  @Override
  public int getDSFID() {

    return LINKED_RESULTSET;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public Comparator comparator() {
    return null;
  }

  @Override
  public boolean dataPreordered() {
    return true;
  }

}
