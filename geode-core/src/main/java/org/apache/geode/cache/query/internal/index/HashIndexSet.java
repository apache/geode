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

/*
 * insertionIndex(), index(), trimToSize() are based on code provided by fastutil They are based
 * from add(), contains() and other methods from ObjectOpenHashSet We have used the traversing
 * mechanism and the HashCommon.mix()
 */
package org.apache.geode.cache.query.internal.index;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import it.unimi.dsi.fastutil.HashCommon;

import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.pdx.internal.PdxString;

/**
 * An implementation of the <tt>Set</tt> interface for the HashIndex Not exactly a set as the hash
 * keys can actually collide but will continue to look for an empty location to store the value
 *
 */
public class HashIndexSet implements Set {

  /**
   * optional statistics object to track number of hash collisions and time spent probing based on
   * hash collisions
   */
  class HashIndexSetProperties {
    /** the set of Objects */
    protected final transient Object[] set;
    /** used for hashing into the table **/
    protected final int mask;

    /** the current number of entries in the set */
    protected transient int size = 0;

    /**
     * the current number of open slots in the hash. Originally used when we collapsed collided keys
     * into collections Not really used now
     */
    protected transient int free;

    /** number of removed tokens in the set, these are index positions that may be reused */
    transient int removedTokens;

    /** size of the backing table (-1) **/
    protected int n;

    /**
     * The maximum number of elements before rehashing
     */
    protected int maxSize;

    private int computeNumFree() {
      return this.n - this.size;
    }

    public HashIndexSetProperties(final Object[] set, final int mask) {
      this.set = set;
      this.mask = mask;
    }
  }

  private transient CachePerfStats cacheStats;

  /** the load above which rehashing occurs. */
  protected static final float DEFAULT_LOAD_FACTOR = 0.5f;

  protected static final int DEFAULT_INITIAL_CAPACITY = 128;

  protected float _loadFactor;

  /**
   * If after an update, the number of removed tokens X percent of the max size, we will compact and
   * rehash to remove the tokens.
   */
  protected static final float CONDITIONAL_REMOVED_TOKEN_REHASH_FACTOR = .7f;

  HashIndexSetProperties hashIndexSetProperties;

  protected HashIndex.IMQEvaluator _imqEvaluator;

  /**
   * The removed token
   */
  protected static final Object REMOVED = new Object();

  @MutableForTesting
  static boolean TEST_ALWAYS_REHASH = false;

  /**
   * This is used when inplace modification is off to detect old key
   */

  public HashIndexSet() {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);

  }

  /**
   * Creates a new <code>HashIndexSet</code> instance with a prime capacity equal to or greater than
   * <tt>initialCapacity</tt> and with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  private HashIndexSet(int initialCapacity, float loadFactor) {
    setUp(initialCapacity, loadFactor);
  }

  public void setEvaluator(HashIndex.IMQEvaluator evaluator) {
    this._imqEvaluator = evaluator;
  }

  /**
   * Set the statistics object for tracking hash code collisions. Should be called before the first
   * element is added.
   */
  public void setCachePerfStats(CachePerfStats stats) {
    this.cacheStats = stats;
  }

  /**
   * Searches the set for <tt>obj</tt>
   *
   * @param obj an <code>Object</code> value
   * @return a <code>boolean</code> value
   */
  @Override
  public boolean contains(Object obj) {
    return index(obj) >= 0;
  }

  /**
   * @param object is the index key
   * @return the hash key
   */

  private int computeHash(Object object) {
    return object.hashCode();
  }

  /**
   * Locates the index of <tt>obj</tt>.
   *
   * @param obj an <code>Object</code> value, expected to be the value object
   * @return the index of <tt>obj</tt> or -1 if it isn't in the set.
   */
  protected int index(Object obj) {
    return index(_imqEvaluator.evaluateKey(obj), obj);
  }

  protected int index(Object key, Object obj) {
    return index(key, obj, -1);
  }

  /**
   * Locates index slot of object using the provided key (in this case we are passing in old key)
   *
   * @return the indexSlot of the given key/object combination
   */
  protected int index(Object key, Object obj, int ignoreThisSlot) {
    HashIndexSetProperties metaData = hashIndexSetProperties;
    int hash;
    int pos;
    Object[] set = metaData.set;
    int mask = metaData.mask;
    Object curr;
    hash = computeHash(key);

    /*
     * Code originated from fastutils Copyright (C) 2002-2014 Sebastiano Vigna
     *
     * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
     * except in compliance with the License. You may obtain a copy of the License at
     *
     * http://www.apache.org/licenses/LICENSE-2.0
     */
    if (!((curr = set[pos = (it.unimi.dsi.fastutil.HashCommon.mix(hash)) & mask]) == null
        || curr == REMOVED)) {
      if (((curr).equals(obj) && pos != ignoreThisSlot))
        return pos;
      while (!((curr = set[pos = (pos + 1) & mask]) == null || curr == REMOVED)) {
        if (((curr).equals(obj)) && pos != ignoreThisSlot)
          return pos;
      }
    }
    return -1;
  }

  public Iterator getAll() {
    return getAllNotMatching(Collections.EMPTY_LIST);
  }

  public Iterator getAllNotMatching(Collection keysToRemove) {
    return new HashIndexSetIterator(keysToRemove, hashIndexSetProperties);
  }

  /**
   * Locates the index of <tt>obj</tt>.
   *
   * @param indexKey an <code>Object</code> value that represents the index key
   * @return Iterator over a collection of objects that match the key
   */
  public Iterator get(Object indexKey) {
    return new HashIndexSetIterator(indexKey, hashIndexSetProperties);
  }

  /**
   *
   * @param set represents the array that all elements are stored in
   * @param index the array index location to store the object. This should be calculated by one of
   *        the insertionIndex methods
   * @param newObject the object to add to the set
   * @return true if object was added
   */
  private boolean addObjectToSet(Object[] set, int index, Object newObject) {
    boolean added = true;
    if (index < 0) {
      throw new ArrayIndexOutOfBoundsException(
          "Cannot add:" + newObject + " into array position:" + index);
    }
    Object oldObject = set[index];
    if (oldObject == null || oldObject == REMOVED) {
      set[index] = newObject;
    }

    return added;
  }

  /**
   * Unsupported, we do not use HashIndexSet as a general all purpose set
   */
  @Override
  public synchronized boolean add(Object obj) {
    throw new UnsupportedOperationException(
        "add(Object) not supported, try add(Object key, Object obj) instead");
  }

  /**
   * Add an object using the hash value of the provided indexKey
   *
   * @param obj the object to add
   * @return true if object has been added
   */
  public synchronized int add(Object indexKey, Object obj) throws TypeMismatchException {
    if (indexKey == null) {
      indexKey = IndexManager.NULL;
    }
    // Note we cannot make the below optimization for hash index. Due to in place modification
    // where old key == new key (when no reverse map) we end up not updating to the correct slot in
    // this case
    // If oldKey and the newKey are same there is no need to update the
    // index-maps.
    // if ((oldKey == null && indexKey == null)
    // || indexKey.equals(oldKey)) {
    // return false;
    // }

    // grow/shrink capacity if needed
    preInsertHook();
    HashIndexSetProperties metaData = hashIndexSetProperties;
    int indexSlot = insertionIndex(indexKey, metaData);

    Object old = metaData.set[indexSlot];
    addObjectToSet(metaData.set, indexSlot, obj);
    hashIndexSetProperties = metaData;
    // only call this now if we are adding to an actual empty slot, otherwise we
    // have reused
    // and inserted into a set or array
    if (old == null) {
      postInsertHook(true);
    } else {
      postInsertHook(false);
    }
    return indexSlot; // yes, we added something
  }

  /**
   * Locates the next available insertion index for the provided indexKey and set
   *
   * @return the index of an open or resused position
   */
  protected int insertionIndex(Object indexKey, HashIndexSetProperties metaData) {
    int hash;
    int pos;
    int mask = metaData.mask;
    Object curr;
    final Object[] array = metaData.set;
    hash = computeHash(indexKey);

    long start = -1L;
    if (this.cacheStats != null) {
      start = this.cacheStats.getTime();
      this.cacheStats.incQueryResultsHashCollisions();
    }
    try {
      /*
       * Code originated from fastutils Copyright (C) 2002-2014 Sebastiano Vigna
       *
       * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
       * except in compliance with the License. You may obtain a copy of the License at
       *
       * http://www.apache.org/licenses/LICENSE-2.0
       */
      if (!((curr = array[pos = (it.unimi.dsi.fastutil.HashCommon.mix(hash)) & mask]) == null
          || curr == REMOVED)) {
        while (!((curr = array[pos = (pos + 1) & mask]) == null || curr == REMOVED)) {
        }
      }
      return pos;
    } finally {
      if (this.cacheStats != null) {
        this.cacheStats.endQueryResultsHashCollisionProbe(start);
      }
    }
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof HashIndexSet)) {
      return false;
    }
    Set that = (Set) other;
    if (that.size() != this.size()) {
      return false;
    }
    return containsAll(that);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    Object[] set = hashIndexSetProperties.set;
    for (int i = set.length; i-- > 0;) {
      if (set[i] != null && set[i] != REMOVED) {
        hash += set[i].hashCode();
      }
    }
    return hash;
  }

  /**
   * Expands or contracts a set to the new specified n.
   *
   * @param newN the expected size
   */
  protected void rehash(int newN) {
    HashIndexSetProperties metaData = hashIndexSetProperties;
    if (TEST_ALWAYS_REHASH) {
      Thread.yield();
    }
    Object[] oldSet = metaData.set;
    int oldCapacity = oldSet.length;



    int mask = newN - 1;
    int _maxSize = computeMaxSize(newN, _loadFactor);
    Object[] newSet = new Object[newN + 1];
    HashIndexSetProperties newHashIndexProperties = new HashIndexSetProperties(newSet, mask);
    newHashIndexProperties.size = metaData.size;
    newHashIndexProperties.free = hashIndexSetProperties.computeNumFree();
    newHashIndexProperties.removedTokens = 0;
    newHashIndexProperties.n = newN;
    newHashIndexProperties.maxSize = _maxSize;
    for (int i = oldCapacity; i-- > 0;) {
      if (oldSet[i] != null && oldSet[i] != REMOVED) {
        Object o = oldSet[i];

        Object key = _imqEvaluator.evaluateKey(o);
        if (key == null) {
          key = IndexManager.NULL;
        }
        int index = insertionIndex(key, newHashIndexProperties);
        if (index >= 0) {
          addObjectToSet(newHashIndexProperties.set, index, o);
        }
      }
    }
    hashIndexSetProperties = newHashIndexProperties;

  }


  /**
   * Unsupported as the hash index does not use this method call
   *
   * @return an <code>Object[]</code> value
   */
  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("toArray not yet supported");
  }

  /**
   * Unsupported as the hash index does not use this method call
   *
   * @param a an <code>Object[]</code> value
   * @return an <code>Object[]</code> value
   */
  @Override
  public Object[] toArray(Object[] a) {
    throw new UnsupportedOperationException("toArray(Object[] a) not yet supported");
  }

  /**
   * Empties the set.
   */
  @Override
  public void clear() {
    HashIndexSetProperties metaData = hashIndexSetProperties;
    metaData.size = 0;
    metaData.free = capacity();
    metaData.removedTokens = 0;
    Object[] set = metaData.set;
    for (int i = set.length; i-- > 0;) {
      set[i] = null;
    }
    hashIndexSetProperties = metaData;
  }

  protected int capacity() {
    return hashIndexSetProperties.set.length;
  }


  @Override
  public boolean remove(Object obj) {
    return remove(_imqEvaluator.evaluateKey(obj), obj);
  }

  public synchronized boolean remove(Object key, Object obj) {
    return remove(key, obj, -1);
  }

  /**
   *
   * @param key assumed to not be null, rather needs to be NULL token
   * @param newIndexSlot if inplace modification occurs with out having a reversemap we end up
   *        scanning the entire index. We want to remove the region entry from the index slot but
   *        not the newly added (correct) slot. Rather only the "old/wrong" slot
   * @return true if object was removed, false otherwise
   */
  public synchronized boolean remove(Object key, Object obj, int newIndexSlot) {
    int indexSlot = index(key, obj, newIndexSlot);
    boolean removed = false;
    // The check for newIndexSlot != indexSlot is incase of in place modification.
    // When inplace occurs, oldkey == newkey and we end up wiping out the "new key" slow rather
    // than the old key slot. Instead let's get to the else portion
    if (indexSlot >= 0 && indexSlot != newIndexSlot) {
      removed = removeAt(indexSlot);
      return removed;
    } else if (!IndexManager.isObjectModificationInplace()) {
      // object could not be found so it's possible there was an inplace modification
      HashIndexSetIterator iterator = (HashIndexSetIterator) getAll();
      while (iterator.hasNext()) {
        Object indexedObject = iterator.next();
        if (areObjectsEqual(indexedObject, obj) && iterator.currentObjectIndex() != newIndexSlot) {
          iterator.remove();
          return true;
        }
      }
    }
    return false;
  }

  public boolean areObjectsEqual(Object o1, Object o2) {
    if (o1 == null) {
      return o2 == null;
    }
    try {
      return TypeUtils.compare(o1, o2, OQLLexerTokenTypes.TOK_EQ).equals(Boolean.TRUE);
    } catch (TypeMismatchException e) {
      return o1.equals(o2);
    }
  }

  /**
   * Creates an iterator over the values of the set. The iterator supports element deletion.
   *
   * @return an <code>Iterator</code> value
   */
  @Override
  public Iterator iterator() {
    return getAll();
  }

  /**
   * Determine if all of the elements in <tt>collection</tt> are present.
   *
   * @param collection a <code>Collection</code> value
   * @return true if all elements are present.
   */
  @Override
  public boolean containsAll(Collection collection) {
    for (Iterator i = collection.iterator(); i.hasNext();) {
      if (!contains(i.next())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Unsupported because type mismatch exception cannot be thrown from Set interface
   */
  @Override
  public boolean addAll(Collection collection) {
    throw new UnsupportedOperationException("Add all not implemented");
  }

  /**
   * Removes all of the elements in <tt>collection</tt> from the set.
   *
   * @param collection a <code>Collection</code> value
   * @return true if the set was modified by the remove all operation.
   */
  @Override
  public boolean removeAll(Collection collection) {
    boolean changed = false;
    int size = collection.size();

    Iterator it = collection.iterator();
    while (size-- > 0) {
      if (remove(it.next())) {
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Removes any values in the set which are not contained in <tt>collection</tt>.
   *
   * @param collection a <code>Collection</code> value
   * @return true if the set was modified by the retain all operation
   */
  @Override
  public boolean retainAll(Collection collection) {
    boolean changed = false;
    int size = size();

    Iterator it = iterator();
    while (it.hasNext()) {
      Object object = it.next();
      if (!collection.contains(object)) {
        it.remove();
        changed = true;
      }
    }
    return changed;
  }

  @Override
  /*
   * return true if no elements exist in the array that are non null or REMOVED tokens
   */
  public boolean isEmpty() {
    return 0 == hashIndexSetProperties.size;
  }

  /**
   * Returns the number of positions used in the backing array Is not a true representation of the
   * number of elements in the array as the array may contain REMOVED tokens
   *
   * @return an <code>int</code> value
   */
  @Override
  public int size() {
    return hashIndexSetProperties.size;
  }

  /**
   * only used for query optimization. Instead of crawling the entire array doing matches let's just
   * return the size of the array as that is the worst case size
   */
  public int size(Object indexKey) {
    return hashIndexSetProperties.size;
  }

  /**
   * Compress the backing array if possible
   */
  public void compact() {
    trimToSize(hashIndexSetProperties.size);
  }

  public boolean trimToSize(final int n) {
    final int l = HashCommon.nextPowerOfTwo((int) Math.ceil(n / _loadFactor));
    if (this.hashIndexSetProperties.n <= l)
      return true;
    try {
      rehash(l);
    } catch (OutOfMemoryError cantDoIt) {
      return false;
    }
    return true;
  }

  /**
   * Remove the object at <tt>index</tt>.
   *
   * @param index an <code>int</code> value
   */
  protected boolean removeAt(int index) {
    Object cur;
    HashIndexSetProperties metaData = hashIndexSetProperties;
    cur = metaData.set[index];
    if (cur == null || cur == REMOVED) {
      // nothing removed
      return false;
    } else {
      metaData.set[index] = REMOVED;
      metaData.size--;
      metaData.removedTokens++;
      hashIndexSetProperties = metaData;

      return true;
    }
  }

  /**
   * initializes this index set
   */
  protected int setUp(final int expectedCapacity, final float loadFactor) {
    int n = arraySize(expectedCapacity, loadFactor);
    this._loadFactor = loadFactor;
    int _maxSize = computeMaxSize(n, loadFactor);
    int mask = n - 1;
    Object[] set = new Object[n + 1];
    HashIndexSetProperties metaData = new HashIndexSetProperties(set, mask);
    metaData.n = n;
    metaData.maxSize = _maxSize;
    hashIndexSetProperties = metaData;
    hashIndexSetProperties.free = hashIndexSetProperties.computeNumFree();

    return n;
  }

  private int computeMaxSize(int n, float loadFactor) {
    return Math.min((int) Math.ceil(n * loadFactor), n - 1);
  }

  /**
   * After insert, allows for calculating metadata
   */
  protected void postInsertHook(boolean usedFreeSlot) {
    if (usedFreeSlot) {
      hashIndexSetProperties.free--;
    } else {
      // we used a removeToken
      hashIndexSetProperties.removedTokens--;
    }
    hashIndexSetProperties.size++;
  }

  /**
   * Before inserting we can ensure we have capacity
   */
  protected void preInsertHook() {
    if (hashIndexSetProperties.size > hashIndexSetProperties.maxSize
        || hashIndexSetProperties.free == 0 || TEST_ALWAYS_REHASH) {
      rehash(arraySize(hashIndexSetProperties.size + 1, _loadFactor));
      computeMaxSize(capacity(), _loadFactor);
      hashIndexSetProperties.free = hashIndexSetProperties.computeNumFree();
    } else if (hashIndexSetProperties.removedTokens > hashIndexSetProperties.maxSize
        * CONDITIONAL_REMOVED_TOKEN_REHASH_FACTOR) {
      compact();
    }
  }

  private class HashIndexSetIterator implements Iterator {
    private Object keyToMatch;
    // objects at time of iterator creation
    private final Object[] objects;
    private int pos;
    private int prevPos;
    private Collection keysToRemove;
    private Object current;
    private int hash;
    private int mask;
    private int probe;

    private HashIndexSetIterator(Collection keysToRemove, HashIndexSetProperties metaData) {
      this.keysToRemove = keysToRemove;
      this.pos = 0;
      this.prevPos = 0;
      this.objects = metaData.set;
      current = objects[pos];
    }

    private HashIndexSetIterator(Object keyToMatch, HashIndexSetProperties metaData) {
      this.keyToMatch = keyToMatch;
      this.objects = metaData.set;
      mask = metaData.mask;
      hash = computeHash(keyToMatch);
      pos = (it.unimi.dsi.fastutil.HashCommon.mix(hash)) & mask;
      prevPos = pos;
      current = this.objects[pos];
    }

    private void setPos(int pos) {
      this.prevPos = this.pos;
      this.pos = pos;
    }

    @Override
    public boolean hasNext() {
      // For Not Equals we need to look in the entire set
      if (keysToRemove != null) {
        while (pos < objects.length) {
          current = objects[pos];
          if (current == null || current.equals(REMOVED)) {
            // continue searching
          } else if (notMatchingAnyKeyToRemove(keysToRemove, current)) {
            return true;
          }
          setPos(pos + 1);
        }
        return false;
      } else {
        current = objects[pos];
        // For Equals query
        while (current != null) {
          if (current != REMOVED) {
            if (objectMatchesIndexKey(keyToMatch, current)) {
              return true;
            }
          }
          // If this is not the correct collection, one that does not match the
          // key we are looking for, then continue our search
          setPos((pos + 1) & mask);
          current = objects[pos];
        }
      }
      return false;
    }

    private boolean notMatchingAnyKeyToRemove(Collection keysToRemove, Object current) {
      Iterator keysToRemoveIterator = keysToRemove.iterator();
      while (keysToRemoveIterator.hasNext()) {
        Object keyToMatch = keysToRemoveIterator.next();
        if (objectMatchesIndexKey(keyToMatch, current)) {
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
        setPos(pos + 1);
      } else {
        // advance the pointer
        setPos((pos + 1) & mask);
      }
      return obj;
    }

    int currentObjectIndex() {
      return prevPos;
    }

    @Override
    public void remove() {
      removeAt(currentObjectIndex());
    }


    public boolean objectMatchesIndexKey(Object indexKey, Object o) {
      Object fieldValue = _imqEvaluator.evaluateKey(o);

      if (fieldValue == IndexManager.NULL && indexKey == IndexManager.NULL) {
        return true;
      } else {
        try {
          if (fieldValue instanceof PdxString) {
            if (indexKey instanceof String) {
              fieldValue = ((PdxString) fieldValue).toString();
            }
          } else if (indexKey instanceof PdxString) {
            if (fieldValue instanceof String) {
              fieldValue = new PdxString((String) fieldValue);
            }
          }
          return TypeUtils.compare(fieldValue, indexKey, OQLLexerTokenTypes.TOK_EQ)
              .equals(Boolean.TRUE);
        } catch (TypeMismatchException e) {
          return fieldValue.equals(indexKey);
        }
      }
    }
  }
}
