/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal.index;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.AttributeDescriptor;
import com.gemstone.gemfire.cache.query.internal.index.AbstractIndex.InternalIndexStatistics;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.util.ObjectProcedure;
import com.gemstone.gemfire.internal.util.PrimeFinder;

/**
 * An implementation of the <tt>Set</tt> interface that uses an open-addressed
 * hash table to store its contents.
 * 
 * On collisions, will store contents in an IndexElemArray and once a threshold
 * has been hit, will store in ConcurrentHashSets.
 */

public class HashIndexSet implements Set {

  /**
   * optional statistics object to track number of hash collisions and time
   * spent probing based on hash collisions
   */
  private transient CachePerfStats cacheStats;

  /** the current number of occupied slots in the hash. */
  protected transient int _size;
  
  /** the current number of free slots in the hash. */
  protected transient int _free;
  
  /** the current number of occupied slots in the hash. */
  protected transient int _removedTokens;

  /** the load above which rehashing occurs. */
  protected static final float DEFAULT_LOAD_FACTOR = 0.5f;

  /**
   * the default initial capacity for the hash table. This is one less than a
   * prime value because one is added to it when searching for a prime capacity
   * to account for the free slot required by open addressing. Thus, the real
   * default capacity is 11.
   */
  protected static final int DEFAULT_INITIAL_CAPACITY = 100;

  /**
   * Determines how full the internal table can become before rehashing is
   * required. This must be a value in the range: 0.0 < loadFactor < 1.0. The
   * default value is 0.5, which is about as large as you can get in open
   * addressing without hurting performance. Cf. Knuth, Volume 3., Chapter 6.
   */
  protected float _loadFactor;

  /**
   * The maximum number of elements allowed without allocating more space.
   */
  protected int _maxSize;

  protected static final int CONDITIONAL_COMPACT_FACTOR = 2;
  
  //If after an update, the number of removed tokens X percent of the max size,
  //we will compact and rehash to remove the tokens.
  protected static final float CONDITIONAL_REMOVED_TOKEN_REHASH_FACTOR = .7f;

  /** the set of Objects */
  protected transient Object[] _set;

  /** the strategy used to hash objects in this collection. */
  protected HashIndexStrategy _hashingStrategy;

  protected static final Object REMOVED = new Object();
  
  static boolean TEST_ALWAYS_REHASH = false;

  /**
   * Map for RegionEntries=>value of indexedExpression (reverse map)
   */
  private ConcurrentMap<Object, Object> entryToValuesMap;
  protected ThreadLocal<Object2ObjectOpenHashMap> entryToOldKeysMap;
  protected InternalIndexStatistics internalIndexStats;
  
  private AttributeDescriptor attDesc;

  /**
   * Creates a new <code>HashIndexSet</code> instance with the default capacity
   * and load factor.
   */
  public HashIndexSet() {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
  }

  public HashIndexSet(ConcurrentMap reverseMap, ThreadLocal<Object2ObjectOpenHashMap> entryToOldKeysMap, InternalIndexStatistics internalIndexStats) {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    this.entryToValuesMap = reverseMap;
    this.entryToOldKeysMap = entryToOldKeysMap;
    this.internalIndexStats = internalIndexStats;
  }
  
  /**
   * Creates a new <code>HashIndexSet</code> instance with the default capacity
   * and load factor.
   * 
   * @param strategy used to compute hash codes and to compare objects.
   */
  public HashIndexSet(HashIndexStrategy strategy) {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    this._hashingStrategy = strategy;
  }

  /**
   * Creates a new <code>HashIndexSet</code> instance with a prime capacity
   * equal to or greater than <tt>initialCapacity</tt> and with the default load
   * factor.
   * 
   * @param initialCapacity
   *          an <code>int</code> value
   */
  public HashIndexSet(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Creates a new <code>HashIndexSet</code> instance with a prime capacity
   * equal to or greater than <tt>initialCapacity</tt> and with the default load
   * factor.
   * 
   * @param initialCapacity an <code>int</code> value
   * @param strategy used to compute hash codes and to compare objects.
   */
  public HashIndexSet(int initialCapacity, HashIndexStrategy strategy) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
    this._hashingStrategy = strategy;
  }

  /**
   * Creates a new <code>HashIndexSet</code> instance with a prime capacity
   * equal to or greater than <tt>initialCapacity</tt> and with the specified
   * load factor.
   * 
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public HashIndexSet(int initialCapacity, float loadFactor) {
    _loadFactor = loadFactor;
    setUp((int) Math.ceil(initialCapacity / loadFactor));
  }

  /**
   * Creates a new <code>HashIndexSet</code> instance with a prime capacity
   * equal to or greater than <tt>initialCapacity</tt> and with the specified
   * load factor.
   * 
   * @param initialCapacity
   *          an <code>int</code> value
   * @param loadFactor
   *          a <code>float</code> value
   * @param strategy
   *          used to compute hash codes and to compare objects.
   */
  public HashIndexSet(int initialCapacity, float loadFactor,
      HashIndexStrategy strategy) {
    this(initialCapacity, loadFactor);
    this._hashingStrategy = strategy;
  }

  /**
   * Creates a new <code>HashIndexSet</code> instance containing the elements of
   * <tt>collection</tt>.
   * 
   * @param collection
   *          a <code>Collection</code> value
   */
  public HashIndexSet(Collection collection) {
    this(collection.size());
    addAll(collection);
  }

  /**
   * Creates a new <code>HashIndexSet</code> instance containing the elements of
   * <tt>collection</tt>.
   * 
   * @param collection
   *          a <code>Collection</code> value
   * @param strategy
   *          used to compute hash codes and to compare objects.
   */
  public HashIndexSet(Collection collection, HashIndexStrategy strategy) {
    this(collection.size(), strategy);
    addAll(collection);
  }

  public void setHashIndexStrategy(HashIndexStrategy hashingStrategy) {
    this._hashingStrategy = hashingStrategy;
  }

  /**
   * Set the statistics object for tracking hash code collisions. Should be
   * called before the first element is added.
   */
  public void setCachePerfStats(CachePerfStats stats) {
    this.cacheStats = stats;
  }

  /**
   * Searches the set for <tt>obj</tt>
   * 
   * @param obj
   *          an <code>Object</code> value
   * @return a <code>boolean</code> value
   */
  public boolean contains(Object obj) {
    return index(obj) >= 0;
  }
  
  /**
   * @param object can be either a region entry or index key
   * @param recomputeKey
              whether the object is a region entry and needs to have the key recomputed
   * @return the hash key
   */
  
  private int computeHash(Object object, boolean recomputeKey) {
    return _hashingStrategy.computeHashCode(object, recomputeKey) & 0x7fffffff;
  }

  /**
   * Locates the index of <tt>obj</tt>.
   * 
   * @param obj an <code>Object</code> value, expected to be a
   * @return the index of <tt>obj</tt> or -1 if it isn't in the set.
   */
  protected int index(Object obj) {
    return index(_hashingStrategy.computeKey(obj), obj);
  }
  
  protected int index(Object key, Object obj) {
    return index(key, obj, -1);
  }
  
  /**
   * Locates index slot of object using the provided key (in this case we are passing in old key)
   * @param key
   * @param obj
   * @return the indexSlot of the given key/object combination
   */
  protected int index(Object key, Object obj, int ignoreThisSlot) {
    int hash, probe, index, length;
    Object[] set;
    Object cur;

    set = _set;
    length = set.length;
    hash = computeHash(key, false);
    index = hash % length;
    cur = set[index];
    
    long start = -1L;
    if (this.cacheStats != null) {
      start = this.cacheStats.getStatTime();
    }
    //Find the correct collection that matches key of the object we are looking for
    //if one exists, we then look to see if the element exists in the collection
    //If a collection is not correct, we probe for the next collection until a null is found
    while (cur != null) {
      if (cur != REMOVED && index != ignoreThisSlot) {
        if (cur instanceof RegionEntry) {
          if (_hashingStrategy.equalsOnAdd(obj, cur)) {
            return index;
          }
        }
      }

      probe = 1 + (hash % (length - 2));
      index -= probe;
      if (index < 0) {
        index += length;
      }
      cur = set[index];
    }
    return -1;
  }

  public Iterator getAll() {
    return getAllNotMatching(Collections.EMPTY_LIST);
  }
  
  public Iterator getAllNotMatching(Collection keysToRemove) {
    return new HashIndexSetIterator(keysToRemove, _set);
  }
  
  /**
   * Locates the index of <tt>obj</tt>.
   * 
   * @param indexKey
   *          an <code>Object</code> value that represents the index key
   * @return the a collection of objects that match the index key or an empty
   *         collection if none match
   */
  public Iterator get(Object indexKey) {
    return new HashIndexSetIterator(indexKey, _set);
  }

  /**
   * 
   * @param set the array that all elements are stored in
   * @param index
   *          the array index location to store the object. This should be
   *          calculated by one of the insertionIndex methods
   * @param newObject the object to add to the set
   * @return true if object was added
   */
  private boolean addObjectToSet(Object[] set, int index, Object newObject) {
    boolean added = true;
    if (index < 0) {
      throwObjectContractViolation(set[(-index - 1)], newObject);
    }
    Object oldObject = set[index];
    if (oldObject == null || oldObject == REMOVED) {
      set[index] = newObject;
    } else if (oldObject instanceof RegionEntry) {
      IndexElemArray elemArray = new IndexElemArray();
      elemArray.add(oldObject);
      elemArray.add(newObject);
      set[index] = elemArray;
    } else if (oldObject instanceof IndexConcurrentHashSet) {
      added = ((IndexConcurrentHashSet) oldObject).add(newObject);
    } else if (oldObject instanceof IndexElemArray) {
      IndexElemArray elemArray = (IndexElemArray) oldObject;
      if (elemArray.size() >= IndexManager.INDEX_ELEMARRAY_THRESHOLD) {
        IndexConcurrentHashSet newSet = new IndexConcurrentHashSet(
            IndexManager.INDEX_ELEMARRAY_THRESHOLD + 20, 0.75f, 1);
        newSet.addAll(elemArray);
        newSet.add(newObject);
        set[index] = newSet;
      } else {
        elemArray.add(newObject);
      }
    }

    return added;
  }

  /**
   * Inserts a value into the set.
   * 
   * @param obj an <code>Object</code> value
   * @return true if the set was modified by the add operation
   */
  public synchronized boolean add(Object obj){
    throw new UnsupportedOperationException(
        "add(Object) not supported, try add(Object key, Object obj) instead");
  }

  public synchronized boolean add(Object indexKey, Object obj) throws TypeMismatchException {   
    // Before adding the entry with new value, remove it from reverse map and
    // using the oldValue remove entry from the forward map.
    // Reverse-map is used based on the system property
    Object oldKey = null;
    if (IndexManager.isObjectModificationInplace() && this.entryToValuesMap.containsKey(obj)){
        oldKey = this.entryToValuesMap.get(obj);
    }
    else if (!IndexManager.isObjectModificationInplace() && this.entryToOldKeysMap != null) {
      Map oldKeyMap = this.entryToOldKeysMap.get();
      if (oldKeyMap != null) {
        oldKey = TypeUtils.indexKeyFor(oldKeyMap.get(obj));        
      }
    }
    // Note we cannot make the optimization for hash index.  Due to in place modification
    // where old key == new key (when no reverse map) we end up not updating to the correct slot in this case
    // If oldKey and the newKey are same there is no need to update the
    // index-maps.
    // if ((oldKey == null && indexKey == null)
    // || indexKey.equals(oldKey)) {
    // return false;
    // }
    
    //grow/shrink capacity if needed
    preInsertHook();
    int indexSlot = insertionIndex(indexKey, obj, _set);
    if (indexSlot < 0) {
      return false; // already present in set, nothing to add
    }

    Object old = _set[indexSlot];
    boolean added = addObjectToSet(_set, indexSlot, obj);
    
    if (added) {
      //Update the reverse map
      if ( IndexManager.isObjectModificationInplace()) {
        this.entryToValuesMap.put(obj, indexKey);
      }
      if (indexKey != null && oldKey != null) {
        remove(oldKey, obj, false, indexSlot);
      }
      // Update Stats after real addition
      internalIndexStats.incNumValues(1);
    }
    
    // only call this now if we are adding to an actual empty slot, otherwise we
    // have reused
    // and inserted into a set or array
    if (old == null) {
      postInsertHook(true);
    }
    else {
      postInsertHook(false);
    }
    return added; // yes, we added something
  }

  /**
   * Locates the index at which <tt>obj</tt> can be inserted. if there is
   * already a value equal()ing <tt>obj</tt> in the set, returns that value's
   * index as <tt>-index - 1</tt>.
   * 
   * @param obj
   *          an <code>Object</code> value
   * @return the index of a FREE slot at which obj can be inserted or, if obj is
   *         already stored in the hash, the negative value of that index, minus
   *         1: -index -1.
   */
  protected int insertionIndex(Object obj) {
    return insertionIndex(_hashingStrategy.computeKey(obj), obj, _set);
  }
  
  protected int insertionIndex(Object obj, Object[] set) {
    return insertionIndex(_hashingStrategy.computeKey(obj), obj, set);
  }
  
  
  /**
   * Locates the index at which <tt>obj</tt> can be inserted. if there is
   * already a value equal()ing <tt>obj</tt> in the set, returns that value's
   * index as <tt>-index - 1</tt>.
   * 
   * @param obj
   *          an <code>Object</code> value
   * @return the index of a FREE slot at which obj can be inserted or, if obj is
   *         already stored in the hash, the negative value of that index, minus
   *         1: -index -1.
   */
  protected int insertionIndex(Object indexKey, Object obj, Object[] set) {
    int hash, probe, indexSlot, length;
    Object cur;

    length = set.length;
    hash = computeHash(indexKey, false);
    indexSlot = hash % length;

    cur = set[indexSlot];

    if (cur == null) {
      return indexSlot; // empty, all done
    }

    // Getting here means we have yet to find the correct key collection
    // so we must find the double hash
    long start = -1L;
    if (this.cacheStats != null) {
      start = this.cacheStats.getStatTime();
      this.cacheStats.incQueryResultsHashCollisions();
    }
    try {

      // compute the double hash
      probe = 1 + (hash % (length - 2));
      // if the slot we landed on is FULL (but not removed), probe
      // until we find an empty slot, a REMOVED slot, or an element
      // equal to the one we are trying to insert.
      // finding an empty slot means that the value is not present
      // and that we should use that slot as the insertion point;
      // finding a REMOVED slot means that we need to keep searching,
      // however we want to remember the offset of that REMOVED slot
      // so we can reuse it in case a "new" insertion (i.e. not an update)
      // is possible.
      // finding a matching value means that we've found that our desired
      // key is already in the table
      if (cur != REMOVED) {
        // starting at the natural offset, probe until we find an
        // offset that isn't full.
        do {

          indexSlot -= probe;
          if (indexSlot < 0) {
            indexSlot += length;
          }
          cur = set[indexSlot];
        } while (cur != null && cur != REMOVED);
      }
      return indexSlot;
    } finally {
      if (this.cacheStats != null) {
        this.cacheStats.endQueryResultsHashCollisionProbe(start);
      }
    }
  }

  @Override
  // GemStoneAddition
  public boolean equals(Object other) {
    if (!(other instanceof Set)) {
      return false;
    }
    Set that = (Set) other;
    if (that.size() != this.size()) {
      return false;
    }
    return containsAll(that);
  }

  @Override
  // GemStoneAddition
  public int hashCode() {
    HashProcedure p = new HashProcedure();
    forEach(p);
    return p.getHashCode();
  }

  /**
   * Executes <tt>procedure</tt> for each element in the set.
   * 
   * @param procedure
   *          a <code>TObjectProcedure</code> value
   * @return false if the loop over the set terminated because the procedure
   *         returned false for some value.
   */
  public boolean forEach(ObjectProcedure procedure) {
    Object[] set = _set;
    for (int i = set.length; i-- > 0;) {
      if (set[i] != null && set[i] != REMOVED && !procedure.executeWith(set[i])) {
        return false;
      }
    }
    return true;
  }

  protected/* GemStoneAddition */final class HashProcedure implements
      ObjectProcedure {
    private int h = 0;

    public int getHashCode() {
      return h;
    }

    public final boolean executeWith(Object key) {
      h += _hashingStrategy.computeHashCode(key);
      return true;
    }
  }

  /**
   * Expands the set to accomodate new values.
   * 
   * @param newCapacity
   *          an <code>int</code> value
   */
  // GemStoneAddition
  protected void rehash(int newCapacity) {
    if (TEST_ALWAYS_REHASH) {
        Thread.yield();
    }
    int oldCapacity = _set.length;
    Object[] oldSet = _set;
    
    Object[] newSet = new Object[newCapacity];
    _removedTokens = 0;
    //adds/removes/rehash should all be synchronized by the hashindex
    //we are ok to clear this map and repopulate
    //we do not do this for _set because we could still be querying 
    //but the reversemap is only used for adds/removes/rehash
    if (IndexManager.isObjectModificationInplace()) {
      entryToValuesMap.clear();
    }
    for (int i = oldCapacity; i-- > 0;) {
      if (oldSet[i] != null && oldSet[i] != REMOVED) {
        Object o = oldSet[i];

        if (o instanceof RegionEntry) {
          Object key = _hashingStrategy.computeKey(o);
          if (key == null) {
            key = IndexManager.NULL;
          }
          int index = insertionIndex(key, o, newSet);
          if (index >= 0)
            if (addObjectToSet(newSet, index, o)) {
              updateReverseMap(o, key);
            }
        } 
      }
    }
    _set = newSet;
  }
  
  private void updateReverseMap(Object regionEntry, Object key) {
    if (IndexManager.isObjectModificationInplace()) {
      entryToValuesMap.put(regionEntry, key);
    }
  }

  /**
   * Convenience methods for subclasses to use in throwing exceptions about
   * badly behaved user objects employed as keys. We have to throw an
   * IllegalArgumentException with a rather verbose message telling the user
   * that they need to fix their object implementation to conform to the general
   * contract for java.lang.Object.
   * 
   * @param o1
   *          the first of the equal elements with unequal hash codes.
   * @param o2
   *          the second of the equal elements with unequal hash codes.
   * @exception IllegalArgumentException
   *              the whole point of this method.
   */
  protected final void throwObjectContractViolation(Object o1, Object o2)
      throws IllegalArgumentException {
    throw new IllegalArgumentException(
        "Equal objects must have equal hashcodes. "
            + "During rehashing, Trove discovered that "
            + "the following two objects claim to be "
            + "equal (as in java.lang.Object.equals()) "
            + "but their hashCodes (or those calculated by "
            + "your HashIndexStrategy) are not equal."
            + "This violates the general contract of "
            + "java.lang.Object.hashCode().  See bullet point two "
            + "in that method's documentation. " + "object #1 ="
            + objToString(o1) + "; object #2 =" + objToString(o2));
  }

  private static String objToString(Object o) {
    if (o instanceof Object[]) {
      return java.util.Arrays.toString((Object[]) o);
    } else {
      return String.valueOf(o);
    }
  }

  /**
   * Returns a new array containing the objects in the set.
   * 
   * @return an <code>Object[]</code> value
   */
  public Object[] toArray() {
    throw new UnsupportedOperationException(
        "toArray not yet supported");
  }

  /**
   * Returns a typed array of the objects in the set.
   * 
   * @param a
   *          an <code>Object[]</code> value
   * @return an <code>Object[]</code> value
   */
  public Object[] toArray(Object[] a) {
    throw new UnsupportedOperationException(
        "toArray(Object[] a) not yet supported");
  }

  /**
   * Empties the set.
   */
  // GemStoneAddition
  public void clear() {
    _size = 0;
    _free = capacity();
    _removedTokens = 0;
    if (IndexManager.isObjectModificationInplace()) {
      entryToValuesMap.clear();
    }
    Object[] set = _set;

    for (int i = set.length; i-- > 0;) {
      set[i] = null;
    }
  }

  protected int capacity() {
    return _set.length;
  }

  /**
   * Removes <tt>obj</tt> from the set.  
   * Currently not implemented correctly, use {@link HashIndexSet#remove(Object, Object, boolean)}
   * 
   * @param obj an <code>Object</code> value
   * @return true if the set was modified by the remove operation.
   */
  public boolean remove(Object obj) {

    throw new UnsupportedOperationException(
        "remove(Object) not supported, try remove(Object key, Object obj) instead");
  }
  
  
  public synchronized boolean remove(Object key, Object obj, boolean updateReverseMap) {
    return remove(key, obj, updateReverseMap, -1);
  }
  
  /**
   * 
   * @param key assumed to not be null, rather needs to be NULL token
   * @param obj
   * @param updateReverseMap
   * @param newIndexSlot if inplace modification occurs with out having a reversemap
   *  we end up scanning the entire index.  We want to remove the region entry from the index slot
   *  but not the newly added (correct) slot.  Rather only the "old/wrong" slot
   * @return true if object was removed, false otherwise
   */
  public synchronized boolean remove(Object key, Object obj, boolean updateReverseMap, int newIndexSlot) {
    int indexSlot = index(key, obj, newIndexSlot);
    boolean removed = false;
    //The check for newIndexSlot != indexSlot is incase of in place modification.
    //When inplace occurs, oldkey == newkey and we end up wiping out the "new key" slow rather
    //than the old key slot.  Instead let's get to the else portion
    if (indexSlot >= 0 && indexSlot != newIndexSlot) {
      removed = removeAt(indexSlot);
      if (removed) {
        if (updateReverseMap && IndexManager.isObjectModificationInplace()) {
          entryToValuesMap.remove(obj);
        }
        internalIndexStats.incNumValues(-1);
      }
      return removed;
    }
    else if (!IndexManager.isObjectModificationInplace()){
      //object could not be found so it's possible there was an inplace modification
        HashIndexSetIterator iterator = (HashIndexSetIterator)getAll();
        while (iterator.hasNext()) {
          Object indexedObject = iterator.next();
          if (_hashingStrategy.equalsOnAdd(indexedObject, obj) && iterator.currentObjectIndex() != newIndexSlot) {
            iterator.remove();
            internalIndexStats.incNumValues(-1);
            return true;
          }
        }
    }
    return false;
  }
  
  

  /**
   * Creates an iterator over the values of the set. The iterator supports
   * element deletion.
   * 
   * @return an <code>Iterator</code> value
   */
  public Iterator iterator() {
    return getAll();
  }

  /**
   * Tests the set to determine if all of the elements in <tt>collection</tt>
   * are present.
   * 
   * @param collection a <code>Collection</code> value
   * @return true if all elements were present in the set.
   */
  public boolean containsAll(Collection collection) {
    for (Iterator i = collection.iterator(); i.hasNext();) {
      if (!contains(i.next())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Adds all of the elements in <tt>collection</tt> to the set.
   * 
   * @param collection a <code>Collection</code> value
   * @return true if the set was modified by the add all operation.
   */
  public boolean addAll(Collection collection) {
    boolean changed = false;
    int size = collection.size();
    Iterator it;

    ensureCapacity(size);
    it = collection.iterator();
    while (size-- > 0) {
      Object obj = it.next();
      if (obj != null && add(obj)) {
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Removes all of the elements in <tt>collection</tt> from the set.
   * 
   * @param collection a <code>Collection</code> value
   * @return true if the set was modified by the remove all operation.
   */
  public boolean removeAll(Collection collection) {
    boolean changed = false;
    int size = collection.size();
    Iterator it;

    it = collection.iterator();
    while (size-- > 0) {
      if (remove(it.next())) {
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Removes any values in the set which are not contained in
   * <tt>collection</tt>.
   * 
   * @param collection a <code>Collection</code> value
   * @return true if the set was modified by the retain all operation
   */
  public boolean retainAll(Collection collection) {
    boolean changed = false;
    int size = size();
    Iterator it;

    it = iterator();
    while (size-- > 0) {
      if (!collection.contains(it.next())) {
        it.remove();
        changed = true;
      }
    }
    return changed;
  }


  @Override
  // GemStoneAddition

  /**
   * Tells whether this set is currently holding any elements.
   * 
   * @return a <code>boolean</code> value
   */
  public boolean isEmpty() {
    return 0 == _size;
  }

  /**
   * Returns the number of slots used in the backing array
   * Is not a true representation of the number of elements in the array
   * 
   * @return an <code>int</code> value
   */
  public int size() {
    return _size;
  }
  
  public int size(Object indexKey) {
    int hash, probe, index, length;
    Object[] set;
    Object cur;
    int size = 0;

    //find the first array index location
    set = _set;
    length = set.length;
    hash = computeHash(indexKey, false);
    index = hash % length;
    cur = set[index];

    if (cur == null) {
      // return
      return 0;
    }

    while (cur != null) {
      if (cur != REMOVED) {
        if (cur instanceof RegionEntry) {
          if (_hashingStrategy.equalsOnGet(indexKey, cur)) {
            size++;
          }
        }
        break;
      }
      //If this is not the correct collection, one that does not match the key
      //we are looking for, then continue our search
      probe = 1 + (hash % (length - 2));
      index -= probe;
      if (index < 0) {
        index += length;
      }
      cur = set[index];
    }
    return size;
  }
  
  /**
   * Ensure that this hashtable has sufficient capacity to hold
   * <tt>desiredCapacity<tt> <b>additional</b> elements without
   * requiring a rehash.  This is a tuning method you can call
   * before doing a large insert.
   * 
   * @param desiredCapacity an <code>int</code> value
   */
  public void ensureCapacity(int desiredCapacity) {
    if (desiredCapacity > (_maxSize - size())) {
      rehash(PrimeFinder.nextPrime((int) Math.ceil(desiredCapacity + size()
          / _loadFactor) + 1));
      computeMaxSize(capacity());
    }
  }

  /**
   * Compresses the hashtable to the minimum prime size (as defined by
   * PrimeFinder) that will hold all of the elements currently in the table. If
   * you have done a lot of <tt>remove</tt> operations and plan to do a lot of
   * queries or insertions or iteration, it is a good idea to invoke this
   * method. Doing so will accomplish two things:
   * 
   * <ol>
   * <li>You'll free memory allocated to the table but no longer needed because
   * of the remove()s.</li>
   * 
   * <li>You'll get better query/insert/iterator performance because there won't
   * be any <tt>REMOVED</tt> slots to skip over when probing for indices in the
   * table.</li>
   * </ol>
   */
  public void compact() {
    // need at least one free spot for open addressing
    rehash(PrimeFinder.nextPrime((int) Math.ceil(size() / _loadFactor) + 1));
    computeMaxSize(capacity());
  }

  // GemStoneAddition
  /**
   * Calls compact by taking next set expansion into account. The set is
   * expanded based on the capacity and load factor (default .5) this method
   * calls the compact if the size is well below next expansion.
   */
  public void conditionalCompact() {
    if (_size < (capacity() * (_loadFactor / CONDITIONAL_COMPACT_FACTOR))) {
      compact();
    }
  }

  /**
   * This simply calls {@link #compact compact}. It is included for symmetry
   * with other collection classes. Note that the name of this method is
   * somewhat misleading (which is why we prefer <tt>compact</tt>) as the load
   * factor may require capacity above and beyond the size of this collection.
   * 
   * @see #compact
   */
  public final void trimToSize() {
    compact();
  }

  /**
   * Delete the record at <tt>index</tt>. Reduces the size of the collection by
   * one.
   * 
   * @param index an <code>int</code> value
   */
  protected boolean removeAt(int index) {
    Object cur;
    cur = _set[index];

    if (cur == null || cur == REMOVED) {
      //nothing removed
      return false;
    } else {
      _set[index] = REMOVED;
      _size--;
      _removedTokens ++;
      return true;
    } 
  }

  /**
   * initializes the hashtable to a prime capacity which is at least
   * <tt>initialCapacity + 1</tt>.
   * 
   * @param initialCapacity an <code>int</code> value
   * @return the actual capacity chosen
   */
  protected int setUp(int initialCapacity) {
    int capacity;
    capacity = PrimeFinder.nextPrime(initialCapacity);
    computeMaxSize(capacity);
    _set = new Object[capacity];
    return capacity;
  }

  /**
   * Computes the values of maxSize. There will always be at least one free slot
   * required.
   * 
   * @param capacity an <code>int</code> value
   */
  private final void computeMaxSize(int capacity) {
    // need at least one free slot for open addressing
    _maxSize = Math.min(capacity - 1, (int) Math.floor(capacity * _loadFactor));
    _free = capacity - _size; // reset the free element count
  }

  /**
   * After an insert, this hook is called to adjust the size/free values of the
   * set and to perform rehashing if necessary.
   */
  protected final void postInsertHook(boolean usedFreeSlot) {
    if (usedFreeSlot) {
      _free--;
    }
    else {
      //we used a removeToken
      _removedTokens--;
    }
    _size++;
  }
  
  protected final void preInsertHook() {
 // rehash whenever we exhaust the available space in the table
    if (_size > _maxSize || _free == 0 || TEST_ALWAYS_REHASH) {
      // choose a new capacity suited to the new state of the table
      // if we've grown beyond our maximum size, double capacity;
      // if we've exhausted the free spots, rehash to the same capacity,
      // which will free up any stale removed slots for reuse.
      int newCapacity = _size > _maxSize ? PrimeFinder
          .nextPrime(capacity() << 1) : capacity();
      rehash(newCapacity);
      computeMaxSize(capacity());
    }
    else if (_removedTokens > _maxSize * CONDITIONAL_REMOVED_TOKEN_REHASH_FACTOR) {
      compact();
    }
  }

  final class ToObjectArrayProcedure implements ObjectProcedure {
    private final Object[] target;
    private int pos = 0;

    public ToObjectArrayProcedure(final Object[] target) {
      this.target = target;
    }

    public final boolean executeWith(Object value) {
      target[pos++] = value;
      return true;
    }
  } // ToObjectArrayProcedure

  public String printAll() {
    StringBuffer s = new StringBuffer();
    for (int i = 0; i < _set.length; i++) {
      Object object = _set[i];
      if (object != null && object != REMOVED) {
        s.append("\n slot[" + i + "]:");
        if (object instanceof Collection) {
          for (Object o : ((Collection) object)) {
            if (o != null) {
              RegionEntry re = (RegionEntry) o;
              Object val = re._getValue();
              if (val instanceof CachedDeserializable) {
                val = ((CachedDeserializable) val).getDeserializedForReading();
              }
              s.append(re.getKey() + " =>  " + val + " # ");
            }
          }
        } else {
          RegionEntry re = (RegionEntry) object;
          Object val = re._getValue();
          if (val instanceof CachedDeserializable) {
            val = ((CachedDeserializable) val).getDeserializedForReading();
          }
          s.append(re.getKey() + " =>  " + val);
        }
      }
    }
    return s.toString();
     }
     
  
  private class HashIndexSetIterator implements Iterator {
    private Object keyToMatch;
    //objects at time of iterator creation
    private final Object[] objects;
    private int indexSlot;
    private Collection keysToRemove;
    private Object current;
    private int hash;
    private int length;
    private int probe;
    
    private HashIndexSetIterator(Collection keysToRemove, Object[] objects ) {
      this.keysToRemove = keysToRemove;
      this.indexSlot = 0;
      this.objects = objects;
      current = objects[indexSlot];
    }
    
    private HashIndexSetIterator(Object keyToMatch, Object[] objects) {
      this.keyToMatch = keyToMatch;
      this.objects = objects;
      
      length = objects.length;
      hash = computeHash(keyToMatch, false);
      probe = 1 + (hash % (length - 2));
      indexSlot = hash % length;
      current = objects[indexSlot];
    }
    
    @Override
    public boolean hasNext() {
      // For Not Equals we need to look in the entire set
      if (keysToRemove != null) {
        while (indexSlot < objects.length) {
          current = objects[indexSlot];
          if (current == null || current.equals(REMOVED)) {
            //continue searching
          }
          else if (notMatchingAnyKeyToRemove(keysToRemove, current)) {
            return true;
          }
     
          indexSlot++;
        }
        return false;
      } else {

        current = objects[indexSlot];
        // For Equals query
        while (current != null) {
          if (current != REMOVED) {
            if (_hashingStrategy.equalsOnGet(keyToMatch, current)) {
              return true;
            }
          }
          // If this is not the correct collection, one that does not match the
          // key we are looking for, then continue our search
          indexSlot -= probe;
          if (indexSlot < 0) {
            indexSlot += length;
          }
          
          current = objects[indexSlot];
        } 
      }
      return false;
    }
    private boolean notMatchingAnyKeyToRemove(Collection keysToRemove, Object current) {
      Iterator keysToRemoveIterator = keysToRemove.iterator();
      while (keysToRemoveIterator.hasNext()) {
        Object keyToMatch = keysToRemoveIterator.next();
        if (_hashingStrategy.equalsOnGet(keyToMatch, current)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Object next() throws NoSuchElementException {
        Object obj = current;
        if (keysToRemove != null) {
          // for Not equals we need to continue looking
          // so increment the index here
          indexSlot++;
        } else {
          //advance the pointer
          indexSlot -= probe;
          if (indexSlot < 0) {
            indexSlot += length;
          }
        }
        return obj;
    }

    int currentObjectIndex() {
      int indexToRemove = 0;
      //Because we advanced on the next() call, we need to get the indexSlot prior to advancing
      if (keysToRemove != null) {
        // for Not equals we need to continue looking
        // so increment the index here
        indexToRemove = indexSlot - 1;
      } else {
        //move back the pointer
        indexToRemove = indexSlot + probe;
        if (indexSlot >= objects.length) {
          indexToRemove = indexSlot - length;
        }
      }
      return indexToRemove;
    }
    
    @Override
    public void remove() {
      removeAt(currentObjectIndex());
    }
    
  }
} // HashIndexSet

