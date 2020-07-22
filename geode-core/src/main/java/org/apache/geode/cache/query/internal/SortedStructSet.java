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
import java.util.TreeSet;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * A TreeSet constrained to contain Structs of all the same type. To conserve on objects, we store
 * the StructType once and reuse it to generate Struct instances on demand.
 *
 * The values in this set are stored as Object[] and get wrapped in Structs as necessary.
 *
 * @since GemFire 4.0
 */
public class SortedStructSet extends TreeSet
    implements SelectResults, DataSerializableFixedID, Ordered, StructFields {
  private static final long serialVersionUID = -1687142950781718159L;

  protected StructTypeImpl structType;
  /**
   * Holds value of property modifiable.
   */
  private boolean modifiable = true;

  /** Creates a new instance of StructSet */
  public SortedStructSet() {};

  /** Creates a new instance of StructSet */
  private SortedStructSet(Comparator c) {
    super(c);
  }

  /** Creates a new instance of StructSet */
  public SortedStructSet(Comparator c, StructTypeImpl structType) {
    this(c);
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.structType = structType;
  }

  /** Creates a new instance of StructSet */
  public SortedStructSet(StructTypeImpl structType) {
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.structType = structType;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SortedStructSet)) {
      return false;
    }
    if (!this.structType.equals(((SortedStructSet) other).structType)) {
      return false;
    }
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /** Add a Struct */
  @Override
  public boolean add(Object obj) {
    if (!(obj instanceof StructImpl)) {
      throw new IllegalArgumentException(
          "This set only accepts StructImpl");
    }
    StructImpl s = (StructImpl) obj;
    if (!s.getStructType().equals(this.structType)) {
      throw new IllegalArgumentException(
          "obj does not have the same StructType");
    }
    // return addFieldValues(s.getFieldValues());
    return this.addFieldValues(s.getFieldValues());
  }

  /**
   * For internal use. Just add the Object[] values for a struct with same type
   */
  @Override
  public boolean addFieldValues(Object[] fieldValues) {
    return super.add(fieldValues);
  }

  /*
   * For internal use. Just add the Object[] values for a struct with same type
   *
   * public boolean addFieldValues(Object[] fieldValues) { //return super.add(fieldValues);
   * StructImpl s = new StructImpl(this.structType, fieldValues); return super.add(s); }
   */

  /** Does this set contain specified struct? */
  @Override
  public boolean contains(Object obj) {
    if (!(obj instanceof Struct)) {
      return false;
    }
    Struct s = (Struct) obj;
    if (!this.structType.equals(StructTypeImpl.typeFromStruct(s))) {
      return false;
    }
    return containsFieldValues(s.getFieldValues());
  }

  /**
   * Does this set contain a Struct of the correct type with the specified values?
   */
  @Override
  public boolean containsFieldValues(Object[] fieldValues) {
    return super.contains(fieldValues);
  }

  /*
   * Does this set contain a Struct of the correct type with the specified values?
   *
   * public boolean containsFieldValues(Object[] fieldValues) { return super.contains(fieldValues);
   * }
   */

  /** Remove the specified Struct */
  @Override
  public boolean remove(Object o) {
    if (!(o instanceof Struct)) {
      return false;
    }
    Struct s = (Struct) o;
    if (!this.structType.equals(StructTypeImpl.typeFromStruct(s))) {
      return false;
    }
    return removeFieldValues(s.getFieldValues());
  }

  /** Remove the field values from a struct of the correct type */
  @Override
  public boolean removeFieldValues(Object[] fieldValues) {
    return super.remove(fieldValues);

  }

  // downcast StructSets to call more efficient methods
  @Override
  public boolean addAll(Collection c) {
    if (c instanceof StructSet) {
      return addAll((StructSet) c);
    }
    return super.addAll(c);
  }

  @Override
  public boolean removeAll(Collection c) {
    if (c instanceof StructSet) {
      return removeAll((StructSet) c);
    }
    return super.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection c) {
    if (c instanceof StructSet) {
      return retainAll((StructSet) c);
    }
    return super.retainAll(c);
  }

  public boolean addAll(StructSet ss) {
    boolean modified = false;
    if (!this.structType.equals(ss.structType)) {
      throw new IllegalArgumentException(
          "types do not match");
    }
    for (Iterator itr = ss.fieldValuesIterator(); itr.hasNext();) {
      if (this.addFieldValues((Object[]) itr.next())) {
        modified = true;
      }
    }
    return modified;
  }

  public boolean removeAll(StructSet ss) {
    boolean modified = false;
    if (!this.structType.equals(ss.structType)) {
      return false; // nothing
                    // modified
    }
    for (Iterator itr = ss.fieldValuesIterator(); itr.hasNext();) {
      Object[] fieldValues = (Object[]) itr.next();
      if (this.removeFieldValues(fieldValues)) {
        modified = true;
      }
    }
    return modified;
  }

  public boolean retainAll(StructSet ss) {
    if (!this.structType.equals(ss.structType)) {
      if (isEmpty()) {
        return false; // nothing modified
      } else {
        clear();
        return true; // nothing retained in receiver collection
      }
    }
    boolean changed = false;
    int size = size();
    Iterator it;
    it = fieldValuesIterator();
    while (size-- > 0) {
      Object[] val = (Object[]) it.next();
      // if (!ss.containsFieldValues(vals)) {
      if (!ss.containsFieldValues(val)) {
        it.remove();
        changed = true;
      }
    }
    return changed;
  }

  /** Returns an Iterator over the Structs in this set */
  @Override
  public Iterator iterator() {
    return new StructIterator(fieldValuesIterator());
  }

  /** Returns an iterator over the fieldValues Object[] instances */
  @Override
  public Iterator fieldValuesIterator() {
    return super.iterator();
  }

  @Override
  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(SortedStructSet.class, this.structType);
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
  public int occurrences(Object element) {
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
    StringBuffer buf = new StringBuffer();
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

  /**
   * Iterator wrapper to construct Structs on demand.
   */
  private class StructIterator implements Iterator {

    private final Iterator itr;

    StructIterator(Iterator itr) {
      this.itr = itr;
    }

    @Override
    public boolean hasNext() {
      return this.itr.hasNext();
    }

    @Override
    public Object next() {
      return new StructImpl((StructTypeImpl) SortedStructSet.this.structType,
          (Object[]) this.itr.next());
    }

    @Override
    public void remove() {
      this.itr.remove();
    }
  }

  @Override
  public int getDSFID() {
    return SORTED_STRUCT_SET;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.modifiable = in.readBoolean();
    int size = in.readInt();
    this.structType = (StructTypeImpl) context.getDeserializer().readObject(in);
    for (int j = size; j > 0; j--) {
      Object[] fieldValues = context.getDeserializer().readObject(in);
      this.addFieldValues(fieldValues);
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    // how do we serialize the comparator?
    out.writeBoolean(this.modifiable);
    out.writeInt(this.size());
    context.getSerializer().writeObject(this.structType, out);
    for (Iterator i = this.fieldValuesIterator(); i.hasNext();) {
      Object[] fieldValues = (Object[]) i.next();
      DataSerializer.writeObjectArray(fieldValues, out);
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public boolean dataPreordered() {
    return false;
  }
}
