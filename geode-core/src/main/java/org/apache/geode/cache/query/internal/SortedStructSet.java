/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import java.io.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.types.*;
import com.gemstone.gemfire.cache.query.internal.types.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * A TreeSet constrained to contain Structs of all the same type. To conserve on
 * objects, we store the StructType once and reuse it to generate Struct
 * instances on demand.
 * 
 * The values in this set are stored as Object[] and get wrapped in Structs as
 * necessary.
 * 
 * @since GemFire 4.0
 */
public final class SortedStructSet extends TreeSet implements SelectResults,
    DataSerializableFixedID, Ordered, StructFields {
  private static final long serialVersionUID = -1687142950781718159L;

  protected StructTypeImpl structType;
  /**
   * Holds value of property modifiable.
   */
  private boolean modifiable = true;

  /** Creates a new instance of StructSet */
  public SortedStructSet() {
  };

  /** Creates a new instance of StructSet */
  private SortedStructSet(Comparator c) {
    super(c);
  }

  /** Creates a new instance of StructSet */
  public SortedStructSet(Comparator c, StructTypeImpl structType) {
    this(c);
    if (structType == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.SortedStructSet_STRUCTTYPE_MUST_NOT_BE_NULL
              .toLocalizedString());
    }
    this.structType = structType;
  }

  /** Creates a new instance of StructSet */
  public SortedStructSet(StructTypeImpl structType) {
    if (structType == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.SortedStructSet_STRUCTTYPE_MUST_NOT_BE_NULL
              .toLocalizedString());
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

  /** Add a Struct */
  @Override
  public boolean add(Object obj) {
    if (!(obj instanceof StructImpl)) {
      throw new IllegalArgumentException(
          LocalizedStrings.SortedStructSet_THIS_SET_ONLY_ACCEPTS_STRUCTIMPL
              .toLocalizedString());
    }
    StructImpl s = (StructImpl) obj;
    if (!s.getStructType().equals(this.structType)) {
      throw new IllegalArgumentException(
          LocalizedStrings.SortedStructSet_OBJ_DOES_NOT_HAVE_THE_SAME_STRUCTTYPE
              .toLocalizedString());
    }
    // return addFieldValues(s.getFieldValues());
    return this.addFieldValues(s.getFieldValues());
  }

  /**
   * For internal use. Just add the Object[] values for a struct with same type
   */
  public boolean addFieldValues(Object[] fieldValues) {
    return super.add(fieldValues);
  }

  /**
   * For internal use. Just add the Object[] values for a struct with same type
   * 
   * public boolean addFieldValues(Object[] fieldValues) { //return
   * super.add(fieldValues); StructImpl s = new StructImpl(this.structType,
   * fieldValues); return super.add(s); }
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
   * Does this set contain a Struct of the correct type with the specified
   * values?
   */
  public boolean containsFieldValues(Object[] fieldValues) {
    return super.contains(fieldValues);
  }

  /**
   * Does this set contain a Struct of the correct type with the specified
   * values?
   * 
   * public boolean containsFieldValues(Object[] fieldValues) { return
   * super.contains(fieldValues); }
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
          LocalizedStrings.SortedStructSet_TYPES_DONT_MATCH.toLocalizedString());
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
  public Iterator fieldValuesIterator() {
    return super.iterator();
  }

  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(SortedStructSet.class, this.structType);
  }

  // note: this method is dangerous in that it could result in undefined
  // behavior if the new struct type is not compatible with the data.
  // For now just trust that the application knows what it is doing if it
  // is overriding the element type in a set of structs
  public void setElementType(ObjectType elementType) {
    if (!(elementType instanceof StructTypeImpl)) {
      throw new IllegalArgumentException(
          LocalizedStrings.SortedStructSet_ELEMENT_TYPE_MUST_BE_STRUCT
              .toLocalizedString());
    }
    this.structType = (StructTypeImpl) elementType;
  }

  public List asList() {
    return new ArrayList(this);
  }

  public Set asSet() {
    return this;
  }

  /**
   * Getter for property modifiable.
   * 
   * @return Value of property modifiable.
   */
  public boolean isModifiable() {
    return this.modifiable;
  }

  public int occurrences(Object element) {
    return contains(element) ? 1 : 0;
  }

  /**
   * Setter for property modifiable.
   * 
   * @param modifiable
   *          New value of property modifiable.
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

    public boolean hasNext() {
      return this.itr.hasNext();
    }

    public Object next() {
      return new StructImpl((StructTypeImpl) SortedStructSet.this.structType,
          (Object[]) this.itr.next());
    }

    public void remove() {
      this.itr.remove();
    }
  }

  public int getDSFID() {
    return SORTED_STRUCT_SET;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.modifiable = in.readBoolean();
    int size = in.readInt();
    this.structType = (StructTypeImpl) DataSerializer.readObject(in);
    for (int j = size; j > 0; j--) {
      Object[] fieldValues = DataSerializer.readObject(in);
      this.addFieldValues(fieldValues);
    }
  }

  public void toData(DataOutput out) throws IOException {
    // how do we serialize the comparator?
    out.writeBoolean(this.modifiable);
    out.writeInt(this.size());
    DataSerializer.writeObject(this.structType, out);
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
