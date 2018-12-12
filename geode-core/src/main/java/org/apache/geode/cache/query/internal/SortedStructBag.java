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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.cache.CachePerfStats;

public class SortedStructBag extends SortedResultsBag<Object[]> implements StructFields {


  /**
   * Constructor for unordered input
   *
   */
  public SortedStructBag(Comparator<Object[]> comparator, boolean nullAtStart) {
    super(comparator, nullAtStart);

  }

  /**
   * Constructor for unordered input
   *
   */
  public SortedStructBag(Comparator<Object[]> comparator, StructType elementType,
      boolean nullAtStart) {
    super(comparator, elementType, nullAtStart);
  }

  /**
   * Constructor for unordered input
   *
   */
  public SortedStructBag(Comparator<Object[]> comparator, ObjectType elementType,
      CachePerfStats stats, boolean nullAtStart) {
    super(comparator, elementType, stats, nullAtStart);

  }

  /**
   * Constructor for unordered input
   *
   */
  public SortedStructBag(Comparator<Object[]> comparator, CachePerfStats stats,
      boolean nullAtStart) {
    super(comparator, stats, nullAtStart);

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

  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(SortedStructBag.class, this.elementType);
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
      return this.mapGet(s.getFieldValues()); // returns 0 if not found
    }
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
    return new HashSet(this);
  }

  /**
   * Getter for property modifiable.
   *
   * @return Value of property modifiable.
   */
  @Override
  public boolean isModifiable() {
    return false;
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
      return new StructImpl((StructTypeImpl) SortedStructBag.this.elementType,
          (Object[]) this.itr.next());
    }

    @Override
    public void remove() {
      this.itr.remove();
    }
  }

}
