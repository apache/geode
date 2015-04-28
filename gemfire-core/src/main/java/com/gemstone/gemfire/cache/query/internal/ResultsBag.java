/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.query.CqResults;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.internal.ObjectIntHashMap.Entry;
import com.gemstone.gemfire.cache.query.internal.types.CollectionTypeImpl;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

// @todo probably should assert element type when elements added
// @todo support generics when no longer support Java 1.4
/**
 * Implementation of SelectResults that allows duplicates and
 * extends IntHashMap. The keys store the elements of the collection and the
 * the values store the number of occurrences as an int.
 * If the elements are Structs, then use a StructBag instead.
 *
 * @author Eric Zoerner
 * @since 5.1
 */
public class ResultsBag extends AbstractCollection
implements CqResults, DataSerializableFixedID {
  protected ObjectType elementType;
  protected ObjectIntHashMap map;
  protected int size = 0;

  /** adds support for null elements*/

  protected int numNulls = 0;
  //Asif: These fields are used for limiting the results based on the limit 
  //clause in the query 
  private int limit = -1;
  boolean hasLimitIterator = false;
  final Object limitLock = new Object();

  public ResultsBag() {
    this.map = new ObjectIntHashMap();
  }
  
  /**
   * This constructor should only be used by the DataSerializer.
   * Creates a ResultsBag with no fields.
   */
  public ResultsBag(boolean ignored) {
  }
  
    
  /**
   * @param stats the CachePerfStats to track hash collisions. Should
   * be null unless this is used as a query execution-time result set.
   */
  public ResultsBag(CachePerfStats stats) {    
    this.map = new ObjectIntHashMap();
  }

  protected ResultsBag(HashingStrategy strategy,
                       CachePerfStats stats) {
    this.map = new ObjectIntHashMap(strategy);
  }
  
  /**
   * @param stats the CachePerfStats to track hash collisions. Should
   * be null unless this is used as a query execution-time result set.
   */
  ResultsBag(Collection c, CachePerfStats stats) {
    this(stats);
    for (Iterator itr = c.iterator(); itr.hasNext(); ) {
      this.add(itr.next());
    }
  }

  protected ResultsBag(Collection c,
                       HashingStrategy strategy,
                       CachePerfStats stats) {
    this(strategy, stats);
    for (Iterator itr = c.iterator(); itr.hasNext(); ) {
      this.add(itr.next());
    }
  }
  
  /**
   * @param stats the CachePerfStats to track hash collisions. Should
   * be null unless this is used as a query execution-time result set.
   */
  ResultsBag(SelectResults sr, CachePerfStats stats) {
    this((Collection)sr, stats);
    // grab type info
    setElementType(sr.getCollectionType().getElementType());
  }
  
  /**
   * @param stats the CachePerfStats to track hash collisions. Should
   * be null unless this is used as a query execution-time result set.
   */
  ResultsBag(ObjectType elementType, CachePerfStats stats) {
    this(stats);
    setElementType(elementType);
  }
  
  /**
   * @param stats the CachePerfStats to track hash collisions. Should
   * be null unless this is used as a query execution-time result set.
   */
  ResultsBag(ObjectType elementType,
             int initialCapacity,
             CachePerfStats stats) {
    this(initialCapacity, stats);
    setElementType(elementType);
  }
  
  /**
   * @param stats the CachePerfStats to track hash collisions. Should
   * be null unless this is used as a query execution-time result set.
   */
  ResultsBag(int initialCapacity, float loadFactor, CachePerfStats stats) {
    this.map = new ObjectIntHashMap(initialCapacity, loadFactor);
  }
  
  protected ResultsBag(int initialCapacity,
                       float loadFactor,
                       HashingStrategy strategy,
                       CachePerfStats stats) {
    this.map = new ObjectIntHashMap(initialCapacity, loadFactor, strategy);
  }
  
  ResultsBag(int initialCapacity, CachePerfStats stats) {
    this.map = new ObjectIntHashMap(initialCapacity);
    
  }
  
  protected ResultsBag(int initialCapacity,
                       HashingStrategy strategy,
                       CachePerfStats stats) {
    this.map = new ObjectIntHashMap(initialCapacity, strategy);
    
  }

  public void setElementType(ObjectType elementType) {
    if (elementType instanceof StructType)
      throw new IllegalArgumentException(LocalizedStrings.ResultsBag_THIS_COLLECTION_DOES_NOT_SUPPORT_STRUCT_ELEMENTS.toLocalizedString());
    this.elementType = elementType;
  }
    
  // @todo Currently does an iteration, could make this more efficient
  // by providing a ListView
  /**
   * Returns this bag as a list.
   */
  public List asList() {
    return new ArrayList(this);
  }
  
  /**
   * Return an unmodifiable Set view of this bag.
   * Does not require an iteration by using a lightweight wrapper.
   */
  public Set asSet() {
    return new SetView();
  }
  
  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(Collection.class, this.elementType);
  }
  
  public boolean isModifiable() {
    return true;
  }
    
  public int occurrences(Object element) {
    if (this.hasLimitIterator) {
      // Asif: If limit iterator then occurrence should be calculated
      // via the limit iterator
      int count = 0;
      boolean encounteredObject = false;
      for (Iterator itr = this.iterator()/* this.base.iterator() */; itr
          .hasNext();) {
        Object v = itr.next();
        if (element == null ? v == null : element.equals(v)) {
          count++;
          encounteredObject = true;
        }
        else if (encounteredObject) {
          // Asif: No possibility of its occurrence again
          break;
        }
      }
      return count;
    }
    else {
      if (element == null) {
        return this.numNulls;
      }
      return this.map.get(element); // returns 0 if not found
    }
  }

  /**
   * Return an iterator over the elements in this collection. Duplicates will
   * show up the number of times it has occurrences.
   */
  @Override
  public Iterator iterator() {
    if (this.hasLimitIterator) {
      // Asif: Return a new LimitIterator in the block so that
      // the current Limit does not get changed by a remove
      // operation of another thread. The current limit is
      // set in the limit iterator. So the setting of the current limit
      // & creation of iterator should be atomic .If any subsequent
      // modifcation in limit occurs, the iterator will itself fail.
      // Similarly a remove called by a iterator should decrease the
      // current limit and underlying itr.remove atomically
      synchronized (this.limitLock) {
        return new LimitResultsBagIterator();
      }
    }
    else {
      return new ResultsBagIterator();
    }
  }

  @Override
  public boolean contains(Object element) {
    if (this.hasLimitIterator) {
      return super.contains(element);
    }
    else {
      if (element == null) {
        return this.numNulls > 0;
      }
      return this.map.containsKey(element);
    }
  }
  
  // not thread safe!
  @Override
  public boolean add(Object element) {
    if( this.limit > -1) {
      throw new UnsupportedOperationException("Addition to the SelectResults not allowed as the query result is constrained by LIMIT");
    }
    if (element == null) {
      numNulls++;
    }
    else {
      int count = this.map.get(element); // 0 if not found
      this.map.put(element, count + 1);
    }
    this.size++;
    assert this.size >= 0 : this.size;
    return true;
  }

  // Internal usage method
  // Asif :In case of StructBag , we will ensure that it
  // gets an Object [] indicating field values as parameter
  // from the CompiledSelect
  public int addAndGetOccurence(Object element) {
    int occurence;
    if (element == null) {
      numNulls++;
      occurence = numNulls;
    }
    else {
      occurence = this.map.get(element); // 0 if not found
      this.map.put(element, ++occurence);
    }
    this.size++;
    assert this.size >= 0: this.size;
    return occurence;
  }
  
  @Override
  public int size() {
    if (this.hasLimitIterator) {
      synchronized (this.limitLock) {
        return this.limit;
      }
    } else {
      return this.size; 
    }
  }    
  

  public int distinctElementsSize() {
    return this.map.size();
  }  

  // not thread safe!
  @Override
  public boolean remove(Object element) {
    if(this.hasLimitIterator) {
      return super.remove(element);
    }else {
    if (element == null) {
      if (this.numNulls > 0) {
        this.numNulls--;
        this.size--;
        assert this.size >= 0 : this.size;
        return true;
      }
      else {
        return false;
      }
    }      
    int count = this.map.get(element); // 0 if not found
    if (count == 0) {
      return false;
    }
    if (count == 1) {
      this.map.remove(element);
    }
    else {
      this.map.put(element, --count);
    }
    this.size--;
    assert this.size >= 0 : this.size;
    return true;
    }
  }
    
  // not thread safe!
  @Override
  public void clear() {
    this.map.clear();
    this.numNulls = 0;
    this.size = 0;
    if (this.hasLimitIterator) {
      synchronized (this.limitLock) {
        this.limit = 0;
      }
    }
  }
  
  // not thread safe!
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ResultsBag)) {
      return false;
    }
    ResultsBag otherBag = (ResultsBag)o;
    return this.size == otherBag.size
      && this.elementType.equals(otherBag.elementType)
      && this.map.equals(otherBag.map)
      && this.numNulls == otherBag.numNulls;
  }
  
  @Override // GemStoneAddition
  public int hashCode() {
    return this.map.hashCode();
  }
  public boolean addAll(Collection coll) {
    if(this.limit > -1) {
      throw new UnsupportedOperationException("Addition to the SelectResults not allowed as the query result is constrained by LIMIT");
    }else {
      return super.addAll(coll);
    }
  }
  protected ObjectIntHashMap createMapForFromData() {
    return new ObjectIntHashMap(this.size);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.elementType = (ObjectType)DataSerializer.readObject(in);
    this.size = in.readInt();
    assert this.size >= 0: this.size;
    this.map = createMapForFromData();
    this.readNumNulls(in);
    // Asif: The size will be including null so the Map should at max contain
    // size - number of nulls
    int numLeft = this.size - this.numNulls;

    while (numLeft > 0) {
      Object key = DataSerializer.readObject(in);
      int occurence = in.readInt();
      this.map.put(key, occurence);
      numLeft -= occurence;
    }
  }

  public int getDSFID() {
    return RESULTS_BAG;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.elementType, out);
    out.writeInt(this.size());
    this.writeNumNulls(out);
    // TODO:Asif: Should we actually pass the limit in serialization?
    // For the time being not passing , assuming PR Has parsed
    // it
    // out.writeInt(this.limit);
    int numLeft = this.size() - this.numNulls;
    for (Iterator<Entry> itr = this.map.entrySet().iterator(); itr.hasNext()
        && numLeft > 0;) {
      Entry entry = itr.next();
      Object key = entry.getKey();
      DataSerializer.writeObject(key, out);
      int occurence = entry.getValue();
      if (numLeft < occurence) {
        occurence = numLeft;
      }
      out.writeInt(occurence);
      numLeft -= occurence;
    }
  }
  
  /**
   * 
   * @param out
   * @throws IOException
   */
  void writeNumNulls(DataOutput out) throws IOException {
    out.writeInt(this.numNulls);
  }
  
  /**
   * 
   * @param in
   * @throws IOException
   */
  void readNumNulls(DataInput in) throws IOException {
    this.numNulls = in.readInt();
  }

  /**
   */
  void createIntHashMap() {
    this.map = new ObjectIntHashMap(this.size - this.numNulls);
  }

  void applyLimit(int limit) {
    this.limit = limit;
    // Asif : From the code of IntHashMap, it appears that if no data is
    // going to be added , then the rehash does not occur & default code
    // of rehash does not appear to change the order of data . So we can assume
    // that this iterator will be returning data in order.
    // Limit Iterator is needed if the limit happens to be less than the size
    if (this.limit > -1 && this.size > this.limit) {
      this.hasLimitIterator = true;
    }
  }
 
  protected class ResultsBagIterator implements Iterator {
    final Iterator<Entry> mapIterator = ResultsBag.this.map.entrySet().iterator();
    Entry currentEntry = null;
    
    /**
     * duplicates are numbered from 1 to n;
     * 0 = no current, otherwise goes from 1 to dupLimit,
     * indicating the last dup that was emitted by next()
     */
    int currentDup = 0;
    /**
     * dupLimit is the total number of occurrences;
     * start by emitting the nulls
     */
    int dupLimit = ResultsBag.this.numNulls;
    
    public boolean hasNext() {
      return this.mapIterator.hasNext() || this.currentDup < this.dupLimit;
    }
    
    public Object next() {
      // see if there is another duplicate to emit
      if (this.currentDup < this.dupLimit) {
        this.currentDup++;
        return (this.currentEntry == null) ? null : this.currentEntry.getKey();
      }
      //otherwise, go to next object
      currentEntry = this.mapIterator.next();
      this.dupLimit = (Integer) currentEntry.getValue();
      this.currentDup = 1;
      return currentEntry.getKey();
    }
    
    public void remove() {
      if (this.currentDup == 0) {
        // next has not yet been called
        throw new IllegalStateException(LocalizedStrings.ResultsBag_NEXT_MUST_BE_CALLED_BEFORE_REMOVE.toLocalizedString());
      }
      
      this.dupLimit--;
      assert this.dupLimit >= 0 : this.dupLimit;
      if (this.currentEntry == null) {
        ResultsBag.this.numNulls = this.dupLimit;
        assert ResultsBag.this.numNulls >= 0 : ResultsBag.this.numNulls;
      }
      else {
        if (this.dupLimit > 0) {
          ResultsBag.this.map.put(currentEntry.getKey(), this.dupLimit);
        }
        else {
          this.mapIterator.remove();
        }
      }
      ResultsBag.this.size--;
      this.currentDup--;
      assert ResultsBag.this.size >= 0 : ResultsBag.this.size;
      assert this.currentDup >= 0 : this.currentDup;
    }    
  }
  
  /** package visibility so ResultsCollectionWrapper can reference
   * it.
   * This SetView is serialized as a special case by a
   * ResultsCollectionWrapper.
   * Keith: Refactored to add consideration for LIMIT, April 1, 2009
   * @see ResultsCollectionWrapper#toData
   */
  class SetView extends AbstractSet {

    private int localLimit;

    SetView() {
      localLimit = ResultsBag.this.limit;
    }

    public Iterator iterator() {
      if (localLimit > -1) {
        return new LimitSetViewIterator();
      } else {
        return new SetViewIterator();
      }
    }

    @Override
    public boolean add(Object o) {
      if(contains(o)) {
        return false;
      }
      return ResultsBag.this.add(o);
    }

    @Override
    public void clear() {
      ResultsBag.this.clear();
    }
    
    @Override
    public int size() {      
      int calculatedSize = ResultsBag.this.map.size() +
                           (ResultsBag.this.numNulls > 0 ? 1 : 0);
      if (localLimit > -1) {
        return Math.min(localLimit, calculatedSize);
      }
      return calculatedSize;
    }
    
    @Override
    public boolean contains(Object o) {
      if (o == null) {
        return ResultsBag.this.numNulls > 0;
      }
      return ResultsBag.this.map.containsKey(o);
    }
    
    @Override
    public boolean isEmpty() {
      if(localLimit == 0) {
        return true;
      }
      if (ResultsBag.this.numNulls > 0) {
        return false;
      }
      return ResultsBag.this.map.isEmpty();
    }

    public class SetViewIterator implements Iterator {
      /** need to emit a null value if true */
      boolean emitNull = ResultsBag.this.numNulls > 0;
      final Iterator it = ResultsBag.this.map.keySet().iterator();
      boolean currentIsNull = false;  

      public Object next() {
        if (this.emitNull) {
          this.emitNull = false;
          currentIsNull = true;
          return null;
        }
        Object key = it.next();
        currentIsNull = false;
        return key;
      }
        
      public boolean hasNext() {
        if (this.emitNull) {
          return true;
        }
        return it.hasNext();
      }
        
      public void remove() {
        if(currentIsNull) {
          ResultsBag.this.numNulls = 0;
        } else {
          it.remove();
        }
      }
    };

    class LimitSetViewIterator extends SetViewIterator {
      private int currPos = 0;
      private Object currentKey;
      @Override
      public Object next() {
        if (this.currPos == ResultsBag.SetView.this.localLimit) {
          throw new NoSuchElementException();
        }
        else {
          currentKey = super.next();
          ++currPos;
          return currentKey;
        }
      }

      @Override
      public boolean hasNext() {
        return (this.currPos < ResultsBag.SetView.this.localLimit)
                && super.hasNext();
      }

      @Override
      public void remove() {
        if (this.currPos == 0) {
          // next has not yet been called
          throw new IllegalStateException("next() must be called before remove()");
        }
        synchronized (ResultsBag.this.limitLock) {
          if(currentIsNull) {
            ResultsBag.this.limit -= ResultsBag.this.numNulls;
            ResultsBag.this.numNulls = 0;
            ResultsBag.SetView.this.localLimit--;
          } else {
            int count = ResultsBag.this.map.remove(currentKey);
            assert count != 0 : "Attempted to remove an element that was not in the map.";
            ResultsBag.this.limit -= count; 
            ResultsBag.SetView.this.localLimit--;
          }
        }
      }
    }
  }
  
  /**
   * @author Asif
   *
   */
  protected class LimitResultsBagIterator extends ResultsBag.ResultsBagIterator {
    final private int localLimit;

    private int currPos = 0;

    /**
     *guarded by ResultsBag.this.limitLock object 
     */
    public LimitResultsBagIterator() {
      localLimit = ResultsBag.this.limit;
    }

    public boolean hasNext() {
      return this.currPos < this.localLimit;
    }

    public Object next() {
      if (this.currPos == this.localLimit) {
        throw new NoSuchElementException();
      }
      else {
        Object next = super.next();
        ++currPos;
        return next;
      }

    }

    public void remove() {
      if (this.currPos == 0) {
        // next has not yet been called
        throw new IllegalStateException("next() must be called before remove()");
      }
      synchronized (ResultsBag.this.limitLock) {
        super.remove();
        --ResultsBag.this.limit;
      }
    }
  }

  @Override
  public Version[] getSerializationVersions() {
     return null;
  }
  
}
