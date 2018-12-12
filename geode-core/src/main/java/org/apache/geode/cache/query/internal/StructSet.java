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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

/**
 * A Set constrained to contain Structs of all the same type. To conserve on objects, we store the
 * StructType once and reuse it to generate Struct instances on demand.
 *
 * The values in this set are stored as Object[] and get wrapped in Structs as necessary.
 *
 * @since GemFire 4.0
 */
public class StructSet /* extends ObjectOpenCustomHashSet */ implements Set, SelectResults,
    DataSerializableFixedID, StructFields {
  private static final long serialVersionUID = -1228835506930611510L;

  protected StructType structType;
  /**
   * Holds value of property modifiable.
   */
  private boolean modifiable = true;
  /**
   * Holds the actual contents of the StructSet
   */
  private ObjectOpenCustomHashSet contents;

  /**
   * Empty constructor to satisfy <code>DataSerializer</code> requirements
   */
  public StructSet() {}

  /**
   * This implementation uses Arrays.equals(Object[]) as it hashing strategy.
   */
  protected static class ObjectArrayHashingStrategy implements ObjectOpenCustomHashSet.Strategy {
    private static final long serialVersionUID = -6407549977968716071L;

    public int hashCode(Object o) {
      // throws ClassCastException if not Object[]
      // compute hash code based on all elements
      Object[] oa = (Object[]) o;
      int h = 0;
      for (int i = 0; i < oa.length; i++) {
        Object obj = oa[i];
        if (obj != null)
          h += obj.hashCode();
      }
      return h;
    }

    public boolean equals(Object o1, Object o2) {
      // throws ClassCastException if not Object[]
      if (o1 == null)
        return o2 == null;
      if (!(o1 instanceof Object[]) || !(o2 instanceof Object[])) {
        return o1.equals(o2);
      }
      return Arrays.equals((Object[]) o1, (Object[]) o2);
    }
  }

  /** Creates a new instance of StructSet */
  public StructSet(StructType structType) {
    this.contents = new ObjectOpenCustomHashSet(new ObjectArrayHashingStrategy());
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.structType = structType;
  }

  /** takes collection of Object[] fieldValues *or* another StructSet */
  public StructSet(Collection c, StructType structType) {
    this.contents = new ObjectOpenCustomHashSet(c, new ObjectArrayHashingStrategy());
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.structType = structType;
  }

  /**
   * Creates a StructSet directly from a StructBag; (internal use)
   *
   * @since GemFire 5.1
   */
  StructSet(StructBag bag) {
    this.contents = new ObjectOpenCustomHashSet(new ObjectArrayHashingStrategy());
    this.structType = (StructType) bag.elementType;
    if (bag.hasLimitIterator) {
      // Asif: Since the number of unique keys which
      // will be returned by Bag with limit in place
      // cannot be more than the size of the bag ( i.e
      // the limit) , we can safely assume HashMap size
      // to equal to bag's size
      Iterator itr = bag.fieldValuesIterator();
      while (itr.hasNext()) {
        addFieldValues((Object[]) itr.next());
      }
    } else {
      Set keys = bag.map.keySet();
      for (Object key : keys) {
        addFieldValues((Object[]) key);
      }
    }
  }


  public StructSet(int initialCapacity, StructType structType) {
    this.contents = new ObjectOpenCustomHashSet(initialCapacity, new ObjectArrayHashingStrategy());
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.structType = structType;
  }

  public StructSet(int initialCapacity, float loadFactor, StructType structType) {
    this.contents =
        new ObjectOpenCustomHashSet(initialCapacity, loadFactor, new ObjectArrayHashingStrategy());
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.structType = structType;
  }


  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StructSet)) {
      return false;
    }
    if (!this.structType.equals(((StructSet) other).structType)) {
      return false;
    }
    if (other.getClass() == StructSet.class) {
      return this.contents.equals(((StructSet) other).contents);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return this.structType.hashCode();
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
          String.format("obj does not have the same StructType: required: %s , actual: %s",
              new Object[] {this.structType, s.getStructType()}));
    }
    return addFieldValues(s.getFieldValues());
  }

  /**
   * For internal use. Just add the Object[] values for a struct with same type
   */
  public boolean addFieldValues(Object[] fieldValues) {
    return this.contents.add(fieldValues);
  }

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
  public boolean containsFieldValues(Object[] fieldValues) {
    return this.contents.contains(fieldValues);
  }

  /** Remove the specified Struct */
  public boolean removeEntry(Object o) {
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
    return this.contents.remove(fieldValues);
  }

  // downcast StructSets to call more efficient methods
  public boolean addAll(Collection c) {
    if (c instanceof StructSet) {
      return addAll((StructSet) c);
    } else {
      boolean modified = false;
      for (Object o : c) {
        modified |= add(o);
      }
      return modified;
    }
  }

  public boolean removeAll(Collection c) {
    if (c instanceof StructSet) {
      return removeAll((StructSet) c);
    } else {
      boolean modified = false;
      for (Object o : c) {
        modified |= remove(o);
      }
      return modified;
    }
  }

  public boolean retainAll(Collection c) {
    if (c instanceof StructSet) {
      return retainAll((StructSet) c);
    }
    return this.contents.retainAll(c);
  }

  public boolean addAll(StructSet ss) {
    boolean modified = false;
    if (!this.structType.equals(ss.structType)) {
      throw new IllegalArgumentException(
          "types do not match");
    }
    for (Iterator itr = ss.fieldValuesIterator(); itr.hasNext();) {
      Object[] vals = (Object[]) itr.next();
      if (this.contents.add(vals)) {
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
      Object[] vals = (Object[]) itr.next();
      if (this.contents.remove(vals)) {
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
      Object[] vals = (Object[]) it.next();
      if (!ss.containsFieldValues(vals)) {
        it.remove();
        changed = true;
      }
    }
    return changed;
  }

  /** Returns an Iterator over the Structs in this set */
  @Override
  public ObjectIterator iterator() {
    return new StructIterator(fieldValuesIterator());
  }

  /** Returns an iterator over the fieldValues Object[] instances */
  public Iterator fieldValuesIterator() {
    return this.contents.iterator();
  }

  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(StructSet.class, this.structType);
  }

  // note: this method is dangerous in that it could result in undefined
  // behavior if the new struct type is not compatible with the data.
  // For now just trust that the application knows what it is doing if it
  // is overriding the element type in a set of structs
  public void setElementType(ObjectType elementType) {
    if (!(elementType instanceof StructTypeImpl)) {
      throw new IllegalArgumentException(
          "element type must be struct");
    }
    this.structType = (StructType) elementType;
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
        buf.append(", ");
    }
    buf.append("]");
    return buf.toString();
  }

  /**
   * Iterator wrapper to construct Structs on demand.
   */
  private class StructIterator extends AbstractObjectIterator {

    private final Iterator itr;

    /**
     * @param itr iterator over the Object[] instances of fieldValues
     */
    StructIterator(Iterator itr) {
      this.itr = itr;
    }

    public boolean hasNext() {
      return this.itr.hasNext();
    }

    public Object next() {
      return new StructImpl((StructTypeImpl) StructSet.this.structType, (Object[]) this.itr.next());
    }

    public void remove() {
      this.itr.remove();
    }

  }

  public int getDSFID() {
    return STRUCT_SET;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.contents = new ObjectOpenCustomHashSet(new ObjectArrayHashingStrategy());
    int size = in.readInt();
    this.structType = (StructTypeImpl) DataSerializer.readObject(in);
    for (int j = size; j > 0; j--) {
      this.add(DataSerializer.readObject(in));
    }
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.size());
    DataSerializer.writeObject(this.structType, out);
    for (Iterator i = this.iterator(); i.hasNext();) {
      DataSerializer.writeObject(i.next(), out);
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int size() {
    return this.contents.size();
  }

  @Override
  public boolean isEmpty() {
    return this.contents.isEmpty();
  }

  @Override
  public Object[] toArray() {
    Struct[] structs = new Struct[this.contents.size()];
    int i = 0;
    for (Iterator iter = this.iterator(); iter.hasNext();) {
      structs[i++] = (Struct) iter.next();
    }
    return structs;
  }

  @Override
  public Object[] toArray(Object[] a) {
    Object[] array = this.contents.toArray(a);
    int i = 0;
    for (Object o : array) {
      array[i++] = new StructImpl((StructTypeImpl) this.structType, (Object[]) o);
    }
    return array;
  }

  @Override
  public boolean remove(Object o) {
    if (o instanceof Struct) {
      o = ((Struct) o).getFieldValues();
    }
    return this.contents.remove(o);
  }

  @Override
  public boolean containsAll(Collection c) {
    // TODO: Asif : This is wrong ,we need to fix this.
    return this.contents.containsAll(c);
  }

  @Override
  public void clear() {
    this.contents.clear();
  }

}
