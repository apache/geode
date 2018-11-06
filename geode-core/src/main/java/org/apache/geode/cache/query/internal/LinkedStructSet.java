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
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

public class LinkedStructSet extends LinkedHashSet<Struct>
    implements SelectResults<Struct>, Ordered, DataSerializableFixedID {

  private static final long serialVersionUID = -1687142950781718156L;

  protected StructTypeImpl structType;

  /**
   * Holds value of property modifiable.
   */
  private boolean modifiable = true;

  /** Creates a new instance of StructSet */
  public LinkedStructSet() {}

  /** Creates a new instance of StructSet */
  public LinkedStructSet(StructTypeImpl structType) {
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.structType = structType;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof SortedStructSet
        && this.structType.equals(((SortedStructSet) other).structType) && super.equals(other);
  }

  /** Add a Struct */
  @Override
  public boolean add(Struct obj) {
    if (!(obj instanceof StructImpl)) {
      throw new IllegalArgumentException(
          "This set only accepts StructImpl");
    }
    StructImpl s = (StructImpl) obj;
    if (!s.getStructType().equals(this.structType)) {
      throw new IllegalArgumentException(
          "obj does not have the same StructType");
    }
    return super.add(s);
  }

  /** Does this set contain specified struct? */
  @Override
  public boolean contains(Object obj) {
    if (!(obj instanceof Struct)) {
      return false;
    }
    Struct s = (Struct) obj;
    return this.structType.equals(StructTypeImpl.typeFromStruct(s)) && contains(s);
  }

  /** Remove the specified Struct */
  @Override
  public boolean remove(Object o) {
    if (!(o instanceof Struct)) {
      return false;
    }
    Struct s = (Struct) o;
    return this.structType.equals(StructTypeImpl.typeFromStruct(s)) && remove(s);
  }

  @Override
  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(Ordered.class, this.structType);
  }

  // note: this method is dangerous in that it could result in undefined
  // behavior if the new struct type is not compatible with the data.
  // For now just trust that the application knows what it is doing if it
  // is overriding the element type in a set of structs
  @Override
  public void setElementType(ObjectType elementType) {
    if (!(elementType instanceof StructTypeImpl)) {
      throw new IllegalArgumentException(
          "element type must be struct");
    }
    this.structType = (StructTypeImpl) elementType;
  }

  @Override
  public List asList() {
    return new ArrayList(this);
  }

  @Override
  public Set asSet() {
    return this;
  }

  /**
   * Getter for property modifiable.
   *
   * @return Value of property modifiable.
   */
  @Override
  public boolean isModifiable() {
    return this.modifiable;
  }

  @Override
  public int occurrences(Struct element) {
    return contains(element) ? 1 : 0;
  }

  /**
   * Setter for property modifiable.
   *
   * @param modifiable New value of property modifiable.
   */
  public void setModifiable(boolean modifiable) {
    this.modifiable = modifiable;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    Iterator i = iterator();
    boolean hasNext = i.hasNext();
    while (hasNext) {
      Object o = i.next();
      buf.append(o == this ? "(this Collection)" : String.valueOf(o));
      hasNext = i.hasNext();
      if (hasNext)
        buf.append(",\n ");
    }
    buf.append("]");
    return buf.toString();
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.modifiable = in.readBoolean();
    int size = in.readInt();
    this.structType = (StructTypeImpl) DataSerializer.readObject(in);
    for (int j = size; j > 0; j--) {
      Object[] fieldValues = DataSerializer.readObject(in);
      this.add(new StructImpl(this.structType, fieldValues));
    }
  }

  @Override
  public int getDSFID() {
    return LINKED_STRUCTSET;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    // how do we serialize the comparator?
    out.writeBoolean(this.modifiable);
    out.writeInt(this.size());
    DataSerializer.writeObject(this.structType, out);
    for (Struct struct : this) {
      DataSerializer.writeObject(struct.getFieldValues(), out);
    }
  }

  @Override
  public Version[] getSerializationVersions() {
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
