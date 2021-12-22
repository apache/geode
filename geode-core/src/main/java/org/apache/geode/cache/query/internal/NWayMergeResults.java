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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.internal.utils.LimitIterator;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.BufferDataOutputStream.LongUpdater;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * The n - way merge results returns a sorted results on the cumulative sorted results for
 * partitioned region based query
 *
 *
 */
public class NWayMergeResults<E> implements SelectResults<E>, Ordered, DataSerializableFixedID {
  private CollectionType collectionType;
  private Collection<E> data;
  private boolean isDistinct;

  public NWayMergeResults() {}

  public NWayMergeResults(Collection<? extends Collection<E>> sortedResults, boolean isDistinct,
      int limit, List<CompiledSortCriterion> orderByAttribs, ExecutionContext context,
      ObjectType elementType) {

    this.isDistinct = isDistinct;
    collectionType = new CollectionTypeImpl(Ordered.class, elementType);
    data = new NWayMergeResultsCollection(sortedResults, limit, orderByAttribs, context);

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
    if (isDistinct) {
      return data.contains(element) ? 1 : 0;
    }
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

  private class NWayMergeResultsCollection extends AbstractCollection<E> {

    private final Collection<? extends Collection<E>> sortedResults;
    private final OrderByComparator comparator;
    private final int limit;

    public NWayMergeResultsCollection(Collection<? extends Collection<E>> sortedResults, int limit,
        List<CompiledSortCriterion> orderByAttribs, ExecutionContext context) {
      this.sortedResults = sortedResults;
      this.limit = limit;
      comparator =
          new OrderByComparator(orderByAttribs, collectionType.getElementType(), context);

    }

    @Override
    public int size() {
      if (isDistinct) {
        Iterator<E> iter = iterator();
        int count = 0;
        while (iter.hasNext()) {
          ++count;
          iter.next();
        }
        return count;

      } else {
        int totalSize = 0;
        for (Collection<E> result : sortedResults) {
          totalSize += result.size();
        }
        if (limit >= 0) {
          return totalSize > limit ? limit : totalSize;
        } else {
          return totalSize;
        }
      }

    }


    @Override
    public Iterator<E> iterator() {
      Iterator<E> iter;
      if (isDistinct) {
        iter = new NWayMergeDistinctIterator();
      } else {
        iter = new NWayMergeIterator();
      }
      if (limit > -1) {
        iter = new LimitIterator<>(iter, limit);
      }
      return iter;
    }

    private class NWayMergeIterator implements Iterator<E> {

      protected final IteratorWrapper<E>[] iterators;
      protected int lastReturnedIteratorIndex = -1;

      protected NWayMergeIterator() {
        iterators = new IteratorWrapper[sortedResults.size()];
        Iterator<? extends Collection<E>> listIter = sortedResults.iterator();
        int index = 0;
        while (listIter.hasNext()) {
          IteratorWrapper<E> temp = new IteratorWrapper<>(listIter.next().iterator());
          iterators[index++] = temp;
          // initialize
          temp.move();
        }
      }

      @Override
      public boolean hasNext() {
        boolean hasNext = false;
        for (int i = 0; i < iterators.length; ++i) {
          if (i == lastReturnedIteratorIndex) {
            hasNext = iterators[i].hasNext();
          } else {
            hasNext = !iterators[i].EOF;
          }
          if (hasNext) {
            break;
          }
        }
        return hasNext;
      }

      protected E basicNext() {
        if (iterators.length == 1) {
          lastReturnedIteratorIndex = 0;
          if (iterators[0].EOF) {
            throw new NoSuchElementException();
          }
          return iterators[0].get();
        }

        int iteratorIndex = -1;
        E refObject = null;
        for (int j = 0; j < iterators.length; ++j) {
          if (!iterators[j].EOF) {
            E temp = iterators[j].get();
            iteratorIndex = j;
            refObject = temp;
            break;

          }
        }
        if (iteratorIndex == -1) {
          throw new NoSuchElementException();
        }

        E currentOptima = null;
        int indexOfIteratorForOptima = -1;

        currentOptima = refObject;
        indexOfIteratorForOptima = iteratorIndex;
        for (int j = iteratorIndex + 1; j < iterators.length; ++j) {
          if (iterators[j].EOF) {
            continue;
          }
          E temp = iterators[j].get();

          int compareResult = compare(currentOptima, temp);

          if (compareResult > 0) {
            currentOptima = temp;
            indexOfIteratorForOptima = j;
          }
        }
        lastReturnedIteratorIndex = indexOfIteratorForOptima;
        return currentOptima;
      }

      protected int compare(E obj1, E obj2) {
        return collectionType.getElementType().isStructType() ? comparator
            .compare(((StructImpl) obj1).getFieldValues(), ((StructImpl) obj2).getFieldValues())
            : comparator.compare(obj1, obj2);

      }

      @Override
      public E next() {
        if (lastReturnedIteratorIndex != -1) {
          iterators[lastReturnedIteratorIndex].move();
        }
        return basicNext();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove not supported");

      }

      private class IteratorWrapper<T> {
        private final Iterator<T> iter;
        private T current = null;
        private boolean EOF = false;

        private IteratorWrapper(Iterator<T> iter) {
          this.iter = iter;
        }

        T get() {
          return current;
        }

        boolean hasNext() {
          return iter.hasNext();
        }

        void move() {
          if (iter.hasNext()) {
            current = iter.next();
          } else {
            current = null;
            EOF = true;
          }
        }

      }
    }

    private class NWayMergeDistinctIterator extends NWayMergeIterator {

      private E lastReturned = null;
      private Boolean cachedHasNext = null;
      private boolean uninitialized = true;

      NWayMergeDistinctIterator() {}

      @Override
      public boolean hasNext() {
        if (cachedHasNext != null) {
          return cachedHasNext.booleanValue();
        }
        boolean hasNext = false;
        for (int i = 0; i < iterators.length; ++i) {
          if (uninitialized) {
            hasNext = !iterators[i].EOF;
            if (hasNext) {
              break;
            }
          } else {
            if (lastReturnedIteratorIndex == i) {
              do {
                iterators[i].move();
                if (iterators[i].EOF) {
                  break;
                } // else if (!this.lastReturned.equals(this.iterators[i].get()))
                  // {
                else if (compare(lastReturned, iterators[i].get()) != 0) {
                  hasNext = true;
                  break;
                }
              } while (true);
            } else {
              do {
                if (iterators[i].EOF) {
                  break;
                } // else if
                  // (!this.iterators[i].get().equals(this.lastReturned)) {
                else if (compare(iterators[i].get(), lastReturned) != 0) {
                  hasNext = true;
                  break;
                } else {
                  iterators[i].move();
                }
              } while (true);
            }
          }
        }
        uninitialized = false;
        cachedHasNext = Boolean.valueOf(hasNext);
        return hasNext;
      }



      @Override
      public E next() {
        if (cachedHasNext == null) {
          hasNext();
        }
        cachedHasNext = null;
        lastReturned = basicNext();
        return lastReturned;
      }

      @Override
      public void remove() {
        super.remove();
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
    collectionType = new CollectionTypeImpl(NWayMergeResults.class, elementType);
    boolean isStruct = elementType.isStructType();
    isDistinct = DataSerializer.readPrimitiveBoolean(in);
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
    return NWAY_MERGE_RESULTS;
  }

  // TODO : optimize for struct elements , by directly writing the fields
  // instead
  // of struct
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    boolean isStruct = collectionType.getElementType().isStructType();
    context.getSerializer().writeObject(collectionType.getElementType(), out);
    DataSerializer.writePrimitiveBoolean(isDistinct, out);
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
    StringBuilder builder =
        new StringBuilder("NWayMergeResults:: isDistinct=" + isDistinct).append(":");
    builder.append('[');
    Iterator<E> iter = iterator();
    while (iter.hasNext()) {
      builder.append(iter.next()).append(',');
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append(']');
    return builder.toString();
  }

  @Override
  public Comparator comparator() {
    if (data instanceof NWayMergeResults.NWayMergeResultsCollection) {
      return ((NWayMergeResultsCollection) data).comparator;
    } else {
      return null;
    }
  }

  @Override
  public boolean dataPreordered() {
    return !(data instanceof NWayMergeResults.NWayMergeResultsCollection);
  }
}
