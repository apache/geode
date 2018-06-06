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
import java.util.Collections;
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
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxString;

public class ResultsCollectionPdxDeserializerWrapper implements SelectResults {
  SelectResults results;
  private boolean copyOnRead = false;

  public ResultsCollectionPdxDeserializerWrapper() {

  }

  public ResultsCollectionPdxDeserializerWrapper(SelectResults results, boolean copyOnRead) {
    this.results = results;
    this.copyOnRead = copyOnRead;
  }

  @Override
  public Iterator iterator() {
    if (results == null) {
      return new SelectResultsPdxInstanceIterator(Collections.emptyIterator());
    }
    return new SelectResultsPdxInstanceIterator(results.iterator());
  }

  private class SelectResultsPdxInstanceIterator implements Iterator {

    Iterator iterator;

    SelectResultsPdxInstanceIterator(Iterator iterator) {
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
        for (int i = 0; i < values.length; i++) {
          if (values[i] instanceof PdxInstance) {
            newValues[i] = ((PdxInstance) values[i]).getObject();
          } else if (values[i] instanceof PdxString) {
            newValues[i] = ((PdxString) values[i]).toString();
          } else if (copyOnRead) {
            // due to bug #50650 When query results are fed back through the query engine
            // we could end up copying a java object but due to serialization
            // end up getting a pdx value. So extract the actual value if needed
            newValues[i] = extractPdxIfNeeded(CopyHelper.copy(values[i]));
          } else {
            newValues[i] = values[i];
          }
        }
        return new StructImpl((StructTypeImpl) struct.getStructType(), newValues);
      } else {
        if (object instanceof PdxInstance) {
          object = ((PdxInstance) object).getObject();
        } else if (object instanceof PdxString) {
          object = ((PdxString) object).toString();
        } else if (copyOnRead) {
          // due to bug #50650 When query results are fed back through the query engine
          // we could end up copying a java object but due to serialization
          // end up getting a pdx value. So extract the actual value if needed
          object = extractPdxIfNeeded(CopyHelper.copy(object));
        }
        return object;
      }
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }

  // Extracts the java object from the pdx instance
  private Object extractPdxIfNeeded(Object object) {
    if (object instanceof PdxInstance) {
      object = ((PdxInstance) object).getObject();
    } else if (object instanceof PdxString) {
      object = ((PdxString) object).toString();
    }
    return object;
  }

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
    Iterator iter = this.iterator();
    while (iter.hasNext()) {
      arrayList.add(iter.next());
    }
    return arrayList.toArray();
  }

  @Override
  public Object[] toArray(Object[] a) {
    Iterator iter = this.iterator();
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
