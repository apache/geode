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
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.internal.utils.LimitIterator;
import org.apache.geode.cache.query.internal.utils.PDXUtils;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.BufferDataOutputStream.LongUpdater;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * This is used as a wrapper over all the results of PR which are of non distinct type
 *
 *
 */
public class CumulativeNonDistinctResults<E> implements SelectResults<E>, DataSerializableFixedID {

  private CollectionType collectionType;
  private Collection<E> data;

  public CumulativeNonDistinctResults() {}

  public CumulativeNonDistinctResults(Collection<? extends Collection<E>> results, int limit,
      ObjectType elementType, List<Metadata> collectionsMetadata) {

    collectionType = new CollectionTypeImpl(CumulativeNonDistinctResults.class, elementType);
    data = new CumulativeNonDistinctResultsCollection(results, limit, collectionsMetadata);

  }

  @Override
  public int size() {
    return data.size();
  }

  @Override
  public boolean isEmpty() {
    return data.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return data.contains(o);
  }

  @Override
  public Iterator<E> iterator() {
    return data.iterator();
  }

  @Override
  public Object[] toArray() {
    return data.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return data.toArray(a);
  }

  @Override
  public boolean add(E e) {
    throw new UnsupportedOperationException("Addition to collection not supported");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("Removal from collection not supported");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return data.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    throw new UnsupportedOperationException("Addition to collection not supported");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Removal from collection not supported");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Removal from collection not supported");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Removal from collection not supported");

  }

  @Override
  public boolean isModifiable() {
    return false;
  }

  @Override
  public int occurrences(E element) {

    // expensive!!
    int count = 0;
    /* this.base.iterator() */
    for (E v : this) {
      if (element == null ? v == null : element.equals(v)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public Set<E> asSet() {
    return new HashSet<>(this);
  }

  @Override
  public List<E> asList() {
    return new ArrayList<>(this);
  }

  @Override
  public CollectionType getCollectionType() {
    return collectionType;
  }

  @Override
  public void setElementType(ObjectType elementType) {
    throw new UnsupportedOperationException(" not supported");
  }

  private class CumulativeNonDistinctResultsCollection extends AbstractCollection<E> {

    private final Collection<? extends Collection<E>> results;
    private final List<Metadata> collectionsMetdata;
    private final int limit;

    public CumulativeNonDistinctResultsCollection(Collection<? extends Collection<E>> results,
        int limit, List<Metadata> collectionsMetadata) {
      this.results = results;
      this.limit = limit;
      collectionsMetdata = collectionsMetadata;
    }

    @Override
    public int size() {

      int totalSize = 0;
      for (Collection<E> result : results) {
        totalSize += result.size();
      }
      if (limit >= 0) {
        return totalSize > limit ? limit : totalSize;
      } else {
        return totalSize;
      }
    }

    @Override
    public Iterator<E> iterator() {
      Iterator<E> iter = new CumulativeCollectionIterator();

      if (limit > -1) {
        iter = new LimitIterator<>(iter, limit);
      }
      return iter;
    }

    private class CumulativeCollectionIterator implements Iterator<E> {

      protected final Iterator<E>[] iterators;
      protected int currentIterator = 0;
      private Boolean cachedHasNext = null;
      private final boolean isStruct;
      private final boolean[] objectChangedMarker = new boolean[1];

      protected CumulativeCollectionIterator() {
        iterators = new Iterator[results.size()];
        Iterator<? extends Collection<E>> listIter = results.iterator();
        int index = 0;
        while (listIter.hasNext()) {
          Iterator<E> temp = listIter.next().iterator();
          iterators[index++] = temp;
        }
        isStruct = collectionType.getElementType().isStructType();
      }

      @Override
      public boolean hasNext() {
        if (cachedHasNext != null) {
          return cachedHasNext;
        }
        boolean hasNext = false;

        for (int i = currentIterator; i < iterators.length; ++i) {
          if (iterators[i].hasNext()) {
            hasNext = true;
            currentIterator = i;
            break;
          }
        }
        cachedHasNext = hasNext;
        return hasNext;
      }

      @SuppressWarnings("unchecked")
      @Override
      public E next() {
        if (cachedHasNext == null) {
          hasNext();
        }
        cachedHasNext = null;
        Metadata metadata = collectionsMetdata.get(currentIterator);
        E original = iterators[currentIterator].next();
        Object e = PDXUtils.convertPDX(original, isStruct, metadata.getDomainObjectForPdx,
            metadata.getDeserializedObject, metadata.localResults, objectChangedMarker, false);
        if (isStruct) {
          if (objectChangedMarker[0]) {
            return (E) new StructImpl((StructTypeImpl) collectionType.getElementType(),
                (Object[]) e);
          } else {
            return original;
          }
        } else {
          return (E) e;
        }

      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove not supported");

      }
    }
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    ObjectType elementType = context.getDeserializer().readObject(in);
    collectionType = new CollectionTypeImpl(CumulativeNonDistinctResults.class, elementType);
    boolean isStruct = elementType.isStructType();

    long size = in.readLong();
    data = new ArrayList<>((int) size);
    long numLeft = size;
    while (numLeft > 0) {
      if (isStruct) {
        Object[] fields = DataSerializer.readObjectArray(in);
        data.add((E) new StructImpl((StructTypeImpl) elementType, fields));
      } else {
        E element = context.getDeserializer().readObject(in);
        data.add(element);
      }
      --numLeft;
    }
  }

  @Override
  public int getDSFID() {
    return CUMULATIVE_RESULTS;
  }

  // TODO : optimize for struct elements , by directly writing the fields
  // instead
  // of struct
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    boolean isStruct = collectionType.getElementType().isStructType();
    context.getSerializer().writeObject(collectionType.getElementType(), out);

    HeapDataOutputStream hdos = new HeapDataOutputStream(1024, null);
    LongUpdater lu = hdos.reserveLong();
    Iterator<E> iter = iterator();
    int numElements = 0;
    while (iter.hasNext()) {
      E data = iter.next();
      if (isStruct) {
        Object[] fields = ((Struct) data).getFieldValues();
        DataSerializer.writeObjectArray(fields, hdos);
      } else {
        context.getSerializer().writeObject(data, hdos);
      }
      ++numElements;
    }
    lu.update(numElements);
    hdos.sendTo(out);

  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("CumulativeNonDistinctResults::");
    builder.append('[');
    for (final E e : this) {
      builder.append(e).append(',');
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append(']');
    return builder.toString();
  }

  public static class Metadata {
    final boolean getDomainObjectForPdx;
    final boolean getDeserializedObject;
    final boolean localResults;

    private Metadata(boolean getDomainObjectForPdx, boolean getDeserializedObject,
        boolean localResults) {
      this.getDomainObjectForPdx = getDomainObjectForPdx;
      this.getDeserializedObject = getDeserializedObject;
      this.localResults = localResults;

    }
  }

  public static Metadata getCollectionMetadata(boolean getDomainObjectForPdx,
      boolean getDeserializedObject, boolean localResults) {
    return new Metadata(getDomainObjectForPdx, getDeserializedObject, localResults);
  }
}
