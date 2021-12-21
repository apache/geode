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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.CopyHelper;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;

/**
 * Class that wraps any SelectResults and performs copy on read when iterating through the results
 * This should only be used when the cache itself has set copy-on-read to true This will save space
 * in that we do not need to copy all results until they are actually accessed through
 * iterator.next(). This wrapper will only be used for queries executed locally PartitionedRegion
 * queries that are executed on the local node will already have been copied and will be treated as
 * remote queries, so will not use this class
 *
 * The downside is that we may actually make two copies of the data if the cached data is actually a
 * serialized byte array. toSet, toList, toArray and toArray(Object[] a) will create new structures
 * that will iterate through the results using a SelectResultsCopyOnReadIterator. The new structures
 * will then have a copy of the values.
 *
 * @since GemFire 8.0
 */
public class ResultsCollectionCopyOnReadWrapper implements SelectResults {
  SelectResults results;

  public ResultsCollectionCopyOnReadWrapper(SelectResults results) {
    this.results = results;
  }

  @Override
  public Iterator iterator() {
    return new SelectResultsCopyOnReadIterator(results.iterator());
  }

  private class SelectResultsCopyOnReadIterator implements Iterator {

    Iterator iterator;

    SelectResultsCopyOnReadIterator(Iterator iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Object next() {
      Object object = iterator.next();
      if (object instanceof Struct) {
        Struct struct = (Struct) object;
        Object[] values = struct.getFieldValues();
        Object[] newValues = new Object[values.length];
        int length = values.length;
        for (int i = 0; i < length; i++) {
          newValues[i] = CopyHelper.copy(values[i]);
        }
        return new StructImpl((StructTypeImpl) struct.getStructType(), newValues);
      } else {
        return CopyHelper.copy(object);
      }
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }


  // All other methods are pass throughs and won't actually be used.
  @Override
  public boolean add(Object e) {
    return results.add(e);
  }



  @Override
  public boolean addAll(Collection c) {
    return results.addAll(c);
  }



  @Override
  public void clear() {
    results.clear();
  }



  @Override
  public boolean contains(Object o) {
    return results.contains(o);
  }



  @Override
  public boolean containsAll(Collection c) {
    return results.containsAll(c);
  }



  @Override
  public boolean isEmpty() {
    return results.isEmpty();
  }

  @Override
  public boolean remove(Object o) {
    return results.remove(o);
  }



  @Override
  public boolean removeAll(Collection c) {
    return results.removeAll(c);
  }



  @Override
  public boolean retainAll(Collection c) {
    return results.retainAll(c);
  }



  @Override
  public int size() {
    return results.size();
  }

  @Override
  public Object[] toArray() {
    ArrayList arrayList = new ArrayList();
    Iterator iter = iterator();
    while (iter.hasNext()) {
      arrayList.add(iter.next());
    }
    return arrayList.toArray();
  }

  @Override
  public Object[] toArray(Object[] a) {
    Iterator iter = iterator();
    int i = 0;
    while (iter.hasNext()) {
      a[i++] = iter.next();
    }
    return a;
  }


  @Override
  public boolean isModifiable() {
    return results.isModifiable();
  }



  @Override
  public int occurrences(Object element) {
    return results.occurrences(element);
  }



  @Override
  public Set asSet() {
    return new HashSet(this);
  }



  @Override
  public List asList() {
    return new ArrayList(this);
  }



  @Override
  public CollectionType getCollectionType() {
    return results.getCollectionType();
  }



  @Override
  public void setElementType(ObjectType elementType) {
    results.setElementType(elementType);
  }

}
