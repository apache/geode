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
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.geode.cache.query.CqResults;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.cache.CachePerfStats;

// @todo probably should assert element type when elements added
// @todo support generics when no longer support Java 1.4
/**
 * Implementation of SelectResults that allows duplicates . The keys store the elements of the
 * collection and the the values store the number of occurrences as an int.
 *
 * @see ResultsBag
 * @see StructBag
 * @see SortedResultsBag
 *
 * @since GemFire 8.1
 */
public abstract class Bag<E> extends AbstractCollection<E> implements CqResults<E> {
  protected ObjectType elementType;
  // protected ObjectIntHashMap map;
  protected int size = 0;

  /** adds support for null elements */

  protected int numNulls = 0;
  // Asif: These fields are used for limiting the results based on the limit
  // clause in the query
  private int limit = -1;
  boolean hasLimitIterator = false;
  final Object limitLock = new Object();

  public Bag() {

  }

  /**
   * This constructor should only be used by the DataSerializer. Creates a ResultsBag with no
   * fields.
   */
  public Bag(boolean ignored) {}

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  public Bag(CachePerfStats stats) {

  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  Bag(Collection c, CachePerfStats stats) {
    this(stats);
    for (Iterator itr = c.iterator(); itr.hasNext();) {
      add(itr.next());
    }
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  Bag(SelectResults sr, CachePerfStats stats) {
    this((Collection) sr, stats);
    // grab type info
    setElementType(sr.getCollectionType().getElementType());
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  Bag(ObjectType elementType, CachePerfStats stats) {
    this(stats);
    setElementType(elementType);
  }

  @Override
  public void setElementType(ObjectType elementType) {
    if (elementType instanceof StructType) {
      throw new IllegalArgumentException(
          "This collection does not support struct elements");
    }
    this.elementType = elementType;
  }

  // @todo Currently does an iteration, could make this more efficient
  // by providing a ListView
  /**
   * Returns this bag as a list.
   */
  @Override
  public List asList() {
    return new ArrayList(this);
  }

  /**
   * Return an unmodifiable Set view of this bag. Does not require an iteration by using a
   * lightweight wrapper.
   */
  @Override
  public Set asSet() {
    return new SetView();
  }

  @Override
  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(Collection.class, elementType);
  }

  @Override
  public abstract boolean isModifiable();

  @Override
  public int occurrences(Object element) {
    if (hasLimitIterator) {
      // Asif: If limit iterator then occurrence should be calculated
      // via the limit iterator
      int count = 0;
      boolean encounteredObject = false;
      for (Iterator itr = iterator(); itr.hasNext();) {
        Object v = itr.next();
        if (element == null ? v == null : element.equals(v)) {
          count++;
          encounteredObject = true;
        } else if (encounteredObject) {
          // Asif: No possibility of its occurrence again
          break;
        }
      }
      return count;
    } else {
      if (element == null) {
        return numNulls;
      }
      return mapGet(element); // returns 0 if not
                              // found
    }
  }

  protected abstract int mapGet(Object element);

  /**
   * Return an iterator over the elements in this collection. Duplicates will show up the number of
   * times it has occurrences.
   */
  @Override
  public Iterator iterator() {
    if (hasLimitIterator) {
      // Asif: Return a new LimitIterator in the block so that
      // the current Limit does not get changed by a remove
      // operation of another thread. The current limit is
      // set in the limit iterator. So the setting of the current limit
      // & creation of iterator should be atomic .If any subsequent
      // modifcation in limit occurs, the iterator will itself fail.
      // Similarly a remove called by a iterator should decrease the
      // current limit and underlying itr.remove atomically
      synchronized (limitLock) {
        return new LimitBagIterator();
      }
    } else {
      return new BagIterator();
    }
  }

  @Override
  public boolean contains(Object element) {
    if (hasLimitIterator) {
      return super.contains(element);
    } else {
      if (element == null) {
        return numNulls > 0;
      }
      return mapContainsKey(element);
    }
  }

  protected abstract boolean mapContainsKey(Object element);

  // not thread safe!
  @Override
  public boolean add(Object element) {
    if (limit > -1) {
      throw new UnsupportedOperationException(
          "Addition to the SelectResults not allowed as the query result is constrained by LIMIT");
    }
    if (element == null) {
      numNulls++;
    } else {
      int count = mapGet(element); // 0 if not
                                   // found
      mapPut(element, count + 1);
    }
    size++;
    assert size >= 0 : size;
    return true;
  }

  protected abstract void mapPut(Object element, int count);

  // Internal usage method
  // Asif :In case of StructBag , we will ensure that it
  // gets an Object [] indicating field values as parameter
  // from the CompiledSelect
  public int addAndGetOccurence(Object element) {
    int occurrence;
    if (element == null) {
      numNulls++;
      occurrence = numNulls;
    } else {
      occurrence = mapGet(element); // 0 if not
                                    // found
      mapPut(element, ++occurrence);
    }
    size++;
    assert size >= 0 : size;
    return occurrence;
  }

  @Override
  public int size() {
    if (hasLimitIterator) {
      synchronized (limitLock) {
        return limit;
      }
    } else {
      return size;
    }
  }

  protected abstract int mapSize();

  // not thread safe!
  @Override
  public boolean remove(Object element) {
    if (hasLimitIterator) {
      return super.remove(element);
    } else {
      if (element == null) {
        if (numNulls > 0) {
          numNulls--;
          size--;
          assert size >= 0 : size;
          return true;
        } else {
          return false;
        }
      }
      int count = mapGet(element); // 0 if not
                                   // found
      if (count == 0) {
        return false;
      }
      if (count == 1) {
        mapRemove(element);
      } else {
        mapPut(element, --count);
      }
      size--;
      assert size >= 0 : size;
      return true;
    }
  }

  protected abstract int mapRemove(Object element);

  // not thread safe!
  @Override
  public void clear() {
    mapClear();// this.map.clear();
    numNulls = 0;
    size = 0;
    if (hasLimitIterator) {
      synchronized (limitLock) {
        limit = 0;
      }
    }
  }

  protected abstract void mapClear();

  // not thread safe!
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Bag)) {
      return false;
    }
    Bag otherBag = (Bag) o;
    return size == otherBag.size && elementType.equals(otherBag.elementType)

        && getMap().equals(otherBag.getMap()) && numNulls == otherBag.numNulls;
  }

  protected abstract Object getMap();

  @Override
  // GemStoneAddition
  public int hashCode() {
    return mapHashCode();
  }

  protected abstract int mapHashCode();

  @Override
  public boolean addAll(Collection coll) {
    if (limit > -1) {
      throw new UnsupportedOperationException(
          "Addition to the SelectResults not allowed as the query result is constrained by LIMIT");
    } else {
      return super.addAll(coll);
    }
  }

  void writeNumNulls(DataOutput out) throws IOException {
    out.writeInt(numNulls);
  }

  void readNumNulls(DataInput in) throws IOException {
    numNulls = in.readInt();
  }

  void applyLimit(int limit) {
    this.limit = limit;
    // Asif : From the code of IntHashMap, it appears that if no data is
    // going to be added , then the rehash does not occur & default code
    // of rehash does not appear to change the order of data . So we can assume
    // that this iterator will be returning data in order.
    // Limit Iterator is needed if the limit happens to be less than the size
    if (this.limit > -1 && size > this.limit) {
      hasLimitIterator = true;
    }
  }

  protected boolean nullOutputAtBegining() {
    return true;
  }

  protected abstract boolean mapEmpty();

  protected abstract Iterator mapEntryIterator();

  protected abstract Iterator mapKeyIterator();

  protected abstract Object keyFromEntry(Object entry);

  protected abstract Integer valueFromEntry(Object entry);

  private void checkModifiablity() {
    if (!isModifiable()) {
      throw new UnsupportedOperationException("Collection unmodifiable");
    }
  }

  protected class BagIterator implements Iterator {
    private final Iterator iter;

    protected BagIterator() {
      if (nullOutputAtBegining()) {
        iter = new NullFirstBagIterator();
      } else {
        iter = new NullLastBagIterator();
      }
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Object next() {
      return iter.next();
    }

    @Override
    public void remove() {
      iter.remove();
    }

    private class NullLastBagIterator implements Iterator {
      final Iterator mapIterator = mapEntryIterator();

      Object currentEntry = null;

      /**
       * duplicates are numbered from 1 to n; 0 = no current, otherwise goes from 1 to dupLimit,
       * indicating the last dup that was emitted by next()
       */
      int currentDup = 0;
      int dupLimit = 0;

      int nullDup = 0;
      int nullDupLimit = numNulls;

      @Override
      public boolean hasNext() {
        return mapIterator.hasNext() || currentDup < dupLimit
            || nullDup < nullDupLimit;
      }

      @Override
      public Object next() {
        // see if there is another duplicate to emit
        if (currentDup < dupLimit) {
          currentDup++;
          return keyFromEntry(currentEntry);
        } else if (mapIterator.hasNext()) {
          // otherwise, go to next object
          currentEntry = mapIterator.next();
          dupLimit = valueFromEntry(currentEntry);
          currentDup = 1;
          return keyFromEntry(currentEntry);
        } else if (nullDup < nullDupLimit) {
          ++nullDup;
          return null;
        } else {
          throw new NoSuchElementException();
        }

      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove not supported");
      }
    }

    private class NullFirstBagIterator implements Iterator {
      final Iterator mapIterator = mapEntryIterator();
      Object currentEntry = null;

      /**
       * duplicates are numbered from 1 to n; 0 = no current, otherwise goes from 1 to dupLimit,
       * indicating the last dup that was emitted by next()
       */
      int currentDup = 0;
      /**
       * dupLimit is the total number of occurrences; start by emitting the nulls
       */
      int dupLimit = numNulls;

      @Override
      public boolean hasNext() {
        return mapIterator.hasNext() || currentDup < dupLimit;
      }

      @Override
      public Object next() {
        // see if there is another duplicate to emit
        if (currentDup < dupLimit) {
          currentDup++;
          return (currentEntry == null) ? null : keyFromEntry(currentEntry);
        }
        // otherwise, go to next object
        currentEntry = mapIterator.next();
        dupLimit = valueFromEntry(currentEntry); // (Integer)

        currentDup = 1;
        return keyFromEntry(currentEntry);
      }

      @Override
      public void remove() {
        checkModifiablity();
        if (currentDup == 0) {
          // next has not yet been called
          throw new IllegalStateException(
              "next() must be called before remove()");
        }

        dupLimit--;
        assert dupLimit >= 0 : dupLimit;
        if (currentEntry == null) {
          numNulls = dupLimit;
          assert numNulls >= 0 : numNulls;
        } else {
          if (dupLimit > 0) {
            mapPut(keyFromEntry(currentEntry), dupLimit);
          } else {
            mapIterator.remove();
          }
        }
        size--;
        currentDup--;
        assert size >= 0 : size;
        assert currentDup >= 0 : currentDup;
      }
    }
  }

  /**
   * package visibility so ResultsCollectionWrapper can reference it. This SetView is serialized as
   * a special case by a ResultsCollectionWrapper. Keith: Refactored to add consideration for LIMIT,
   * April 1, 2009
   *
   * @see ResultsCollectionWrapper#toData
   */
  class SetView extends AbstractSet {

    private int localLimit;

    SetView() {
      localLimit = limit;
    }

    @Override
    public Iterator iterator() {
      if (localLimit > -1) {
        return new LimitSetViewIterator();
      } else {
        return new SetViewIterator();
      }
    }

    @Override
    public boolean add(Object o) {
      if (contains(o)) {
        return false;
      }
      return Bag.this.add(o);
    }

    @Override
    public void clear() {
      Bag.this.clear();
    }

    @Override
    public int size() {
      int calculatedSize = mapSize() + (numNulls > 0 ? 1 : 0);
      if (localLimit > -1) {
        return Math.min(localLimit, calculatedSize);
      }
      return calculatedSize;
    }

    @Override
    public boolean contains(Object o) {
      if (o == null) {
        return numNulls > 0;
      }
      return mapContainsKey(o);
    }

    @Override
    public boolean isEmpty() {
      if (localLimit == 0) {
        return true;
      }
      if (numNulls > 0) {
        return false;
      }
      return mapEmpty();
    }

    public class SetViewIterator implements Iterator {
      /** need to emit a null value if true */
      boolean emitNull = numNulls > 0;
      final Iterator it = mapKeyIterator();
      boolean currentIsNull = false;

      @Override
      public Object next() {
        if (emitNull) {
          emitNull = false;
          currentIsNull = true;
          return null;
        }
        Object key = it.next();
        currentIsNull = false;
        return key;
      }

      @Override
      public boolean hasNext() {
        if (emitNull) {
          return true;
        }
        return it.hasNext();
      }

      @Override
      public void remove() {
        if (currentIsNull) {
          numNulls = 0;
        } else {
          it.remove();
        }
      }
    }

    class LimitSetViewIterator extends SetViewIterator {
      private int currPos = 0;
      private Object currentKey;

      @Override
      public Object next() {
        if (currPos == localLimit) {
          throw new NoSuchElementException();
        } else {
          currentKey = super.next();
          ++currPos;
          return currentKey;
        }
      }

      @Override
      public boolean hasNext() {
        return (currPos < localLimit) && super.hasNext();
      }

      @Override
      public void remove() {
        if (currPos == 0) {
          // next has not yet been called
          throw new IllegalStateException("next() must be called before remove()");
        }
        synchronized (limitLock) {
          if (currentIsNull) {
            limit -= numNulls;
            numNulls = 0;
            localLimit--;
          } else {
            int count = mapRemove(currentKey);
            assert count != 0 : "Attempted to remove an element that was not in the map.";
            limit -= count;
            localLimit--;
          }
        }
      }
    }
  }

  protected class LimitBagIterator extends Bag.BagIterator {
    private final int localLimit;

    private int currPos = 0;

    /**
     * guarded by ResultsBag.this.limitLock object
     */
    public LimitBagIterator() {
      localLimit = limit;
    }

    @Override
    public boolean hasNext() {
      return currPos < localLimit;
    }

    @Override
    public Object next() {
      if (currPos == localLimit) {
        throw new NoSuchElementException();
      } else {
        Object next = super.next();
        ++currPos;
        return next;
      }

    }

    @Override
    public void remove() {
      if (currPos == 0) {
        // next has not yet been called
        throw new IllegalStateException("next() must be called before remove()");
      }
      synchronized (limitLock) {
        super.remove();
        --limit;
      }
    }
  }

}
