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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import it.unimi.dsi.fastutil.Hash;

import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.cache.CachePerfStats;

/**
 * A Bag constrained to contain Structs of all the same type. To conserve on objects, we store the
 * StructType once and reuse it to generate Struct instances on demand.
 *
 * The values in this set are stored as Object[] and get wrapped in Structs as necessary.
 *
 * @since GemFire 5.1
 */
public class StructBag extends ResultsBag implements StructFields {
  /**
   * Holds value of property modifiable.
   */
  private boolean modifiable = true;

  /**
   * This implementation uses Arrays.equals(Object[]) as it hashing strategy.
   */
  protected static class ObjectArrayHashingStrategy implements HashingStrategy {

    public int hashCode(Object o) {
      Object[] oa = (Object[]) o;
      return Arrays.deepHashCode(oa);
    }

    public boolean equals(Object o1, Object o2) {
      if (o1 == null)
        return o2 == null;
      if (!(o1 instanceof Object[]) || !(o2 instanceof Object[])) {
        return o1.equals(o2);
      }
      return Arrays.deepEquals((Object[]) o1, (Object[]) o2);
    }
  }

  /**
   * This implementation uses Arrays.equals(Object[]) as it hashing strategy.
   */
  protected static class ObjectArrayFUHashingStrategy implements Hash.Strategy<Object> {
    private static final long serialVersionUID = 8975047264555337042L;

    public int hashCode(Object o) {
      // throws ClassCastException if not Object[]
      // compute hash code based on all elements
      if (!(o instanceof Object[])) {
        throw new ClassCastException(String.format("Expected an Object[], but actual is %s",
            o.getClass().getName()));
      }
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

  /**
   * This constructor should only be used by DataSerializer
   */
  public StructBag() {

  }


  /**
   * Creates a new instance of StructBag
   *
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  public StructBag(StructType structType, CachePerfStats stats) {
    super(new ObjectArrayHashingStrategy(), stats);
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.elementType = structType;
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  public StructBag(Collection c, StructType structType, CachePerfStats stats) {
    super(c, new ObjectArrayHashingStrategy(), stats);
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.elementType = structType;
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  public StructBag(int initialCapacity, StructType structType, CachePerfStats stats) {
    super(initialCapacity, new ObjectArrayHashingStrategy(), stats);
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.elementType = structType;
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  public StructBag(int initialCapacity, float loadFactor, StructType structType,
      CachePerfStats stats) {
    super(initialCapacity, loadFactor, new ObjectArrayHashingStrategy(), stats);
    if (structType == null) {
      throw new IllegalArgumentException(
          "structType must not be null");
    }
    this.elementType = structType;
  }

  @Override
  public boolean equals(Object o) { // for findbugs
    return super.equals(o);
  }

  @Override
  public int hashCode() { // for findbugs
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
    if (!this.elementType.equals(s.getStructType())) {
      throw new IllegalArgumentException(
          String.format(
              "obj does not have the same StructType.; collection structype,%s; added obj type=%s",
              this.elementType, s.getStructType()));
    }
    return addFieldValues(s.getFieldValues());
  }

  /**
   * For internal use. Just add the Object[] values for a struct with same type
   */
  public boolean addFieldValues(Object[] fieldValues) {
    return super.add(fieldValues);
  }

  /** Does this set contain specified struct? */
  @Override
  public boolean contains(Object obj) {
    if (!(obj instanceof Struct)) {
      return false;
    }
    Struct s = (Struct) obj;
    if (!this.elementType.equals(StructTypeImpl.typeFromStruct(s))) {
      return false;
    }
    return containsFieldValues(s.getFieldValues());
  }

  /**
   * Does this set contain a Struct of the correct type with the specified values?
   */
  public boolean containsFieldValues(Object[] fieldValues) {
    // Asif: The fieldValues can never be null . If the Struc contained
    // null , then the the getFieldValues would have returned
    // a zero size Object array. So we need not bother about null here
    if (this.hasLimitIterator) {
      Iterator fieldItr = this.fieldValuesIterator();
      while (fieldItr.hasNext()) {
        if (Arrays.equals((Object[]) fieldItr.next(), fieldValues)) {
          return true;
        }
      }
      return false;
    } else {
      return super.contains(fieldValues);
    }
  }

  @Override
  public int occurrences(Object element) {
    if (!(element instanceof Struct)) {
      return 0;
    }
    Struct s = (Struct) element;
    if (!this.elementType.equals(StructTypeImpl.typeFromStruct(s))) {
      return 0;
    }
    if (this.hasLimitIterator) {
      int count = 0;
      boolean encounteredObject = false;
      Object[] fields = s.getFieldValues();
      for (Iterator itr = this.fieldValuesIterator(); itr.hasNext();) {
        Object[] structFields = (Object[]) itr.next();
        if (Arrays.equals(fields, structFields)) {
          count++;
          encounteredObject = true;
        } else if (encounteredObject) {
          // Asif: No possibility of its occurrence again
          break;
        }
      }
      return count;
    } else {
      return this.map.get(s.getFieldValues()); // returns 0 if not found
    }
  }

  public int occurrences(Object[] element) {
    return this.map.get(element); // returns 0 if not found
  }


  /** Remove the specified Struct */
  @Override
  public boolean remove(Object o) {
    if (!(o instanceof Struct)) {
      return false;
    }
    Struct s = (Struct) o;
    if (!this.elementType.equals(StructTypeImpl.typeFromStruct(s))) {
      return false;
    }
    return removeFieldValues(s.getFieldValues());
  }

  /** Remove the field values from a struct of the correct type */
  public boolean removeFieldValues(Object[] fieldValues) {
    if (this.hasLimitIterator) {
      // Asif : Get the field value Iterator
      Iterator fieldItr = this.fieldValuesIterator();
      while (fieldItr.hasNext()) {
        if (Arrays.equals((Object[]) fieldItr.next(), fieldValues)) {
          fieldItr.remove();
          return true;
        }
      }
      return false;
    } else {
      return super.remove(fieldValues);
    }
  }

  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(StructBag.class, this.elementType);
  }

  // downcast StructBags to call more efficient methods
  @Override
  public boolean addAll(Collection c) {
    if (c instanceof StructFields) {
      return addAll((StructFields) c);
    }
    return super.addAll(c);
  }

  @Override
  public boolean removeAll(Collection c) {
    if (c instanceof StructFields) {
      return removeAll((StructFields) c);
    }
    return super.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection c) {
    if (c instanceof StructFields) {
      return retainAll((StructFields) c);
    }
    return super.retainAll(c);
  }

  public boolean addAll(StructFields sb) {
    boolean modified = false;
    if (!this.elementType.equals(sb.getCollectionType().getElementType())) {
      throw new IllegalArgumentException(
          "types do not match");
    }

    for (Iterator itr = sb.fieldValuesIterator(); itr.hasNext();) {
      // Check if query execution on this thread is canceled.
      QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCanceled();

      Object[] vals = (Object[]) itr.next();
      if (super.add(vals)) {
        modified = true;
      }
    }
    return modified;
  }

  public boolean removeAll(StructFields ss) {
    boolean modified = false;
    if (!this.elementType.equals(ss.getCollectionType().getElementType())) {
      return false; // nothing // modified
    }
    for (Iterator itr = ss.fieldValuesIterator(); itr.hasNext();) {
      Object[] vals = (Object[]) itr.next();
      if (this.removeFieldValues(vals)) {
        modified = true;
      }
    }
    return modified;
  }

  public boolean retainAll(StructFields ss) {
    if (!this.elementType.equals(ss.getCollectionType().getElementType())) {
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

  /**
   * Return an iterator over the elements in this collection. Duplicates will show up the number of
   * times it has occurrences.
   */
  @Override
  public Iterator iterator() {
    return new StructBagIterator(fieldValuesIterator());
  }

  /** Returns an iterator over the fieldValues Object[] instances */
  public Iterator fieldValuesIterator() {
    return super.iterator();
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
    this.elementType = elementType;
  }

  @Override
  public Set asSet() {
    return new StructSet(this);
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

  /**
   * Setter for property modifiable.
   *
   * @param modifiable New value of property modifiable.
   */
  public void setModifiable(boolean modifiable) {
    this.modifiable = modifiable;
  }

  @Override
  public int getDSFID() {
    return STRUCT_BAG;
  }

  @Override
  protected ObjectIntHashMap createMapForFromData() {
    return new ObjectIntHashMap(this.size, new ObjectArrayHashingStrategy());
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.modifiable = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeBoolean(this.modifiable);
  }

  void writeNumNulls(DataOutput out) {}

  void readNumNulls(DataInput in) {}

  void createTObjectIntHashMap() {
    this.map = new ObjectIntHashMap(this.size, new ObjectArrayHashingStrategy());
  }

  /**
   * Iterator wrapper to construct Structs on demand.
   */
  private class StructBagIterator extends BagIterator {

    private final Iterator itr;

    /**
     * @param itr iterator over the Object[] instances of fieldValues
     */
    StructBagIterator(Iterator itr) {
      this.itr = itr;
    }

    @Override
    public boolean hasNext() {
      return this.itr.hasNext();
    }

    @Override
    public Object next() {
      return new StructImpl((StructTypeImpl) StructBag.this.elementType,
          (Object[]) this.itr.next());
    }

    @Override
    public void remove() {
      this.itr.remove();
    }
  }
}
