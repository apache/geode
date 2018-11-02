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
package org.apache.geode.cache.query.internal.index;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.valueOf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledSortCriterion;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.internal.index.IndexManager.TestHook;
import org.apache.geode.cache.query.internal.index.IndexStore.IndexStoreEntry;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;

public class RangeIndex extends AbstractIndex {

  protected volatile int valueToEntriesMapSize = 0;

  /**
   * Map for valueOf(indexedExpression)=>RegionEntries. SortedMap<Object, (RegionEntry |
   * List<RegionEntry>)>. Package access for unit tests.
   */
  final ConcurrentNavigableMap valueToEntriesMap =
      new ConcurrentSkipListMap(TypeUtils.getExtendedNumericComparator());

  // Map for RegionEntries=>value of indexedExpression (reverse map)
  private final RegionEntryToValuesMap entryToValuesMap;

  // Map for RegionEntries=>values when indexedExpression evaluates to null
  protected RegionEntryToValuesMap nullMappedEntries;

  // Map for RegionEntries=>values when indexedExpression evaluates to UNDEFINED
  protected RegionEntryToValuesMap undefinedMappedEntries;

  // All following data-structures are used only at index update time.
  // So minimum memory must be allocated for these collections.
  protected ThreadLocal<Map> keysToHashSetMap = new ThreadLocal<Map>();
  protected ThreadLocal<List> nullEntries = new ThreadLocal<List>();
  protected ThreadLocal<List> undefinedEntries = new ThreadLocal<List>();

  public static TestHook testHook;

  // TODO: need more specific list of exceptions
  /**
   * Create an Range Index that can be used when executing queries.
   *
   * @param indexName the name of this index, used for statistics collection
   * @param indexedExpression the expression to index on, a function dependent on region entries
   *        individually.
   * @param fromClause expression that evaluates to the collection(s) that will be queried over,
   *        must contain one and only one region path.
   * @param projectionAttributes expression that transforms each element in the result set of a
   *        query Return the newly created Index
   */
  public RangeIndex(InternalCache cache, String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes, String origFromClause,
      String origIndexExpr, String[] definitions, IndexStatistics stats) {
    super(cache, indexName, region, fromClause, indexedExpression, projectionAttributes,
        origFromClause, origIndexExpr, definitions, stats);

    RegionAttributes ra = region.getAttributes();
    this.entryToValuesMap = new RegionEntryToValuesMap(
        new java.util.concurrent.ConcurrentHashMap(ra.getInitialCapacity(), ra.getLoadFactor(),
            ra.getConcurrencyLevel()),
        false /* use set */);
    nullMappedEntries = new RegionEntryToValuesMap(true /* use list */);
    undefinedMappedEntries = new RegionEntryToValuesMap(true /* use list */);
  }

  @Override
  protected boolean isCompactRangeIndex() {
    return false;
  }

  @Override
  void instantiateEvaluator(IndexCreationHelper indexCreationHelper) {
    this.evaluator = new IMQEvaluator(indexCreationHelper);
  }

  @Override
  public void initializeIndex(boolean loadEntries) throws IMQException {
    // Collection results = evaluator.initializeIndex();
    long startTime = System.nanoTime();
    evaluator.initializeIndex(loadEntries);
    long endTime = System.nanoTime();
    this.internalIndexStats.incUpdateTime(endTime - startTime);
  }

  void addMapping(RegionEntry entry) throws IMQException {
    // Save oldKeys somewhere first
    this.evaluator.evaluate(entry, true);
    addSavedMappings(entry);
    clearCurrState();
  }

  void saveMapping(Object key, Object indxResultSet, RegionEntry entry) {
    if (key == null) {
      List nullSet = nullEntries.get();
      if (nullSet == null) {
        nullSet = new ArrayList(1);
        nullEntries.set(nullSet);
      }
      nullSet.add(indxResultSet);
    } else if (key == QueryService.UNDEFINED) {
      List undefinedSet = undefinedEntries.get();
      if (undefinedSet == null) {
        undefinedSet = new ArrayList(1);
        undefinedEntries.set(undefinedSet);
      }

      if (indxResultSet != null) {
        if (indxResultSet.getClass().getName().startsWith("org.apache.geode.internal.cache.Token$")
            || indxResultSet == QueryService.UNDEFINED) {
          // do nothing, Entries are either removed or invalidated or destroyed
          // by other thread.
        } else {
          undefinedSet.add(indxResultSet);
        }
      }
    } else {
      Map keysToSetMap = keysToHashSetMap.get();
      if (keysToSetMap == null) {
        keysToSetMap = new Object2ObjectOpenHashMap(1);
        keysToHashSetMap.set(keysToSetMap);
      }
      Object value = keysToSetMap.get(key);
      if (value == null) {
        keysToSetMap.put(key, indxResultSet);
      } else if (value instanceof Collection) {
        ((Collection) value).add(indxResultSet);
      } else {
        List values = new ArrayList(2);
        values.add(indxResultSet);
        values.add(value);
        keysToSetMap.put(key, values);
      }
    }

    this.internalIndexStats.incNumUpdates();
  }

  public void addSavedMappings(RegionEntry entry) throws IMQException {

    List nullSet = nullEntries.get();
    List undefinedSet = undefinedEntries.get();
    Map keysMap = keysToHashSetMap.get();
    // Add nullEntries
    if (nullSet != null && nullSet.size() > 0) {
      this.internalIndexStats
          .incNumValues(-this.nullMappedEntries.getNumValues(entry) + nullSet.size());
      this.nullMappedEntries.replace(entry,
          (nullSet.size() > 1) ? nullSet : nullSet.iterator().next());
    } else {
      this.internalIndexStats.incNumValues(-this.nullMappedEntries.getNumValues(entry));
      this.nullMappedEntries.remove(entry);
    }

    // Add undefined entries
    if (undefinedSet != null && undefinedSet.size() > 0) {
      this.internalIndexStats
          .incNumValues(-this.undefinedMappedEntries.getNumValues(entry) + undefinedSet.size());
      this.undefinedMappedEntries.replace(entry,
          (undefinedSet.size() > 1) ? undefinedSet : undefinedSet.iterator().next());
    } else {
      this.internalIndexStats.incNumValues(-this.undefinedMappedEntries.getNumValues(entry));
      this.undefinedMappedEntries.remove(entry);
    }

    // Get existing keys from reverse map and remove new keys
    // from this list and remove index entries for these old keys.
    Object oldkeys = this.entryToValuesMap.remove(entry);
    if (keysMap != null) {
      Set keys = keysMap.keySet();
      try {
        if (oldkeys != null) {
          if (oldkeys instanceof Collection) {
            for (Object key : keys) {
              ((Collection) oldkeys).remove(TypeUtils.indexKeyFor(key));
            }
          } else {
            for (Object key : keys) {
              if (TypeUtils.indexKeyFor(key).equals(oldkeys)) {
                oldkeys = null;
              }
            }
          }
        }
      } catch (Exception ex) {
        throw new IMQException(String.format("Could not add object of type %s",
            oldkeys.getClass().getName()), ex);
      }

      // Perform replace of new index entries in index.
      if (keys.size() == 1) {
        Object key = keys.iterator().next();
        try {
          Object newKey = TypeUtils.indexKeyFor(key);
          boolean retry = false;
          do {
            retry = false;
            RegionEntryToValuesMap rvMap =
                (RegionEntryToValuesMap) this.valueToEntriesMap.get(newKey);
            if (rvMap == null) {
              rvMap = new RegionEntryToValuesMap(true /* use target list */);
              Object oldValue = this.valueToEntriesMap.putIfAbsent(newKey, rvMap);
              if (oldValue != null) {
                retry = true;
                continue;
              } else {
                this.internalIndexStats.incNumKeys(1);
                // TODO: non-atomic operation on volatile int
                ++valueToEntriesMapSize;
              }
            }
            // Locking the rvMap so that remove thread will wait to grab the lock on it
            // and only remove it if current rvMap size is zero.
            if (!retry) {
              synchronized (rvMap) {
                // If remove thread got the lock on rvMap first then we must retry.
                if (rvMap != this.valueToEntriesMap.get(newKey)) {
                  retry = true;
                } else {
                  // We got lock first so remove will wait and check the size of rvMap again
                  // before removing it.
                  Object newValues = keysMap.get(key);
                  Object oldValues = rvMap.get(entry);
                  rvMap.replace(entry, newValues);

                  // Update reverserMap (entry => values)
                  this.entryToValuesMap.add(entry, newKey);
                  // Calculate the difference in size.
                  int diff = calculateSizeDiff(oldValues, newValues);
                  this.internalIndexStats.incNumValues(diff);
                }
              }
            }
          } while (retry);
        } catch (TypeMismatchException ex) {
          throw new IMQException(String.format("Could not add object of type %s",
              key.getClass().getName()), ex);
        }
      } else {
        for (Object key : keys) {
          try {
            Object newKey = TypeUtils.indexKeyFor(key);
            boolean retry = false;
            // Going in a retry loop until concurrent index update is successful.
            do {
              retry = false;
              RegionEntryToValuesMap rvMap =
                  (RegionEntryToValuesMap) this.valueToEntriesMap.get(newKey);
              if (rvMap == null) {
                rvMap = new RegionEntryToValuesMap(true /* use target list */);
                Object oldValue = this.valueToEntriesMap.putIfAbsent(newKey, rvMap);
                if (oldValue != null) {
                  retry = true;
                  continue;
                } else {
                  this.internalIndexStats.incNumKeys(1);
                  // TODO: non-atomic operation on volatile int
                  ++valueToEntriesMapSize;
                }
              }
              // Locking the rvMap so that remove thread will wait to grab the
              // lock on it and only remove it if current rvMap size is zero.
              if (!retry) {
                synchronized (rvMap) {
                  // If remove thread got the lock on rvMap first then we must retry.
                  if (rvMap != this.valueToEntriesMap.get(newKey)) {
                    retry = true;
                  } else {
                    // We got lock first so remove will wait and check the size of
                    // rvMap again before removing it.
                    Object newValues = keysMap.get(key);
                    Object oldValues = rvMap.get(entry);
                    rvMap.replace(entry, newValues);
                    // Update reverserMap (entry => values)
                    this.entryToValuesMap.add(entry, newKey);
                    // Calculate the difference in size.
                    int diff = calculateSizeDiff(oldValues, newValues);
                    this.internalIndexStats.incNumValues(diff);
                  }
                }
              }
            } while (retry);
          } catch (TypeMismatchException ex) {
            throw new IMQException(String.format("Could not add object of type %s",
                key.getClass().getName()), ex);
          }
        } // for loop for keys
      }
    }
    // Remove the remaining old keys.
    if (oldkeys != null) {
      removeOldMapping(entry, oldkeys);
    }
  }

  private void removeOldMapping(RegionEntry entry, Object oldkeys) throws IMQException {

    if (oldkeys instanceof Collection) {
      Iterator valuesIter = ((Iterable) oldkeys).iterator();
      while (valuesIter.hasNext()) {
        Object key = valuesIter.next();
        RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) this.valueToEntriesMap.get(key);
        if (rvMap == null) {
          throw new IMQException(
              String.format("Indexed object's class %s compareTo function is errorneous.",
                  oldkeys.getClass().getName()));
        }
        this.internalIndexStats.incNumValues(-rvMap.getNumValues(entry));
        rvMap.remove(entry);
        if (rvMap.getNumEntries() == 0) {
          synchronized (rvMap) {
            // We should check for the size inside lock as some thread might
            // be adding to the same rvMap in add call.
            if (rvMap.getNumEntries() == 0) {
              if (this.valueToEntriesMap.remove(key, rvMap)) {
                this.internalIndexStats.incNumKeys(-1);
                --valueToEntriesMapSize;
              }
            }
          }
        }
      }
    } else {
      RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) this.valueToEntriesMap.get(oldkeys);

      // Shobhit: Fix for bug #44123
      // rvMap Can not be null if values were found in reverse map. rvMap can
      // only be null in case when "values" has wrong compareTo()
      // implementation.
      if (rvMap == null) {
        throw new IMQException(
            String.format("Indexed object's class %s compareTo function is errorneous.",
                oldkeys.getClass().getName()));
      }
      this.internalIndexStats.incNumValues(-rvMap.getNumValues(entry));
      rvMap.remove(entry);
      if (rvMap.getNumEntries() == 0) {
        synchronized (rvMap) {
          // We got lock first so remove will wait and check the size of
          // rvMap again before removing it.
          if (rvMap.getNumEntries() == 0) {
            if (this.valueToEntriesMap.remove(oldkeys, rvMap)) {
              this.internalIndexStats.incNumKeys(-1);
              --valueToEntriesMapSize;
            }
          }
        }
      }
    }
  }

  private int calculateSizeDiff(Object oldValues, Object newValues) {
    int oldSize = 0, newSize = 0;
    if (oldValues != null) {
      if (oldValues instanceof Collection) {
        // Its going to be subtracted.
        oldSize = -(((Collection) oldValues).size());
      } else {
        oldSize = -1;
      }
    }
    if (newValues != null) {
      if (newValues instanceof Collection) {
        // Its going to be added.
        newSize = (((Collection) newValues).size());
      } else {
        newSize = 1;
      }
    }
    return (oldSize + newSize);
  }

  public void clearCurrState() {
    this.nullEntries.remove();
    this.undefinedEntries.remove();
    this.keysToHashSetMap.remove();
  }

  //// IndexProtocol interface implementation
  public boolean clear() throws QueryException {
    throw new UnsupportedOperationException(
        "Not yet implemented");
  }

  @Override
  public ObjectType getResultSetType() {
    return this.evaluator.getIndexResultSetType();
  }

  /**
   * Get the index type
   *
   * @return the type of index
   */
  public IndexType getType() {
    return IndexType.FUNCTIONAL;
  }

  /*
   * We are NOT using any synchronization in this method as this is supposed to be called only
   * during initialization. Watch out for Initialization happening during region.clear() call
   * because that happens concurrently with other index updates WITHOUT synchronization on
   * RegionEntry.
   */
  void addMapping(Object key, Object value, RegionEntry entry) throws IMQException {

    // Find old entries for the entry
    if (key == null) {
      nullMappedEntries.add(entry, value);
      this.internalIndexStats.incNumValues(1);
    } else if (key == QueryService.UNDEFINED) {
      if (value != null) {
        if (value.getClass().getName().startsWith("org.apache.geode.internal.cache.Token$")
            || value == QueryService.UNDEFINED) {
          // do nothing, Entries are either removed or invalidated or destroyed
          // by other thread.
        } else {
          undefinedMappedEntries.add(entry, value);
          this.internalIndexStats.incNumValues(1);
        }
      }
    } else {
      try {
        Object newKey = TypeUtils.indexKeyFor(key);
        RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) this.valueToEntriesMap.get(newKey);
        if (rvMap == null) {
          rvMap = new RegionEntryToValuesMap(true /* use target list */);
          this.valueToEntriesMap.put(newKey, rvMap);
          this.internalIndexStats.incNumKeys(1);
          // TODO: non-atomic operation on volatile int
          ++valueToEntriesMapSize;
        }
        rvMap.add(entry, value);
        // Update reverserMap (entry => values)
        this.entryToValuesMap.add(entry, newKey);
        this.internalIndexStats.incNumValues(1);
      } catch (TypeMismatchException ex) {
        throw new IMQException(String.format("Could not add object of type %s",
            key.getClass().getName()), ex);
      }
    }
    this.internalIndexStats.incNumUpdates();
  }

  /**
   * @param opCode one of REMOVE_OP, BEFORE_UPDATE_OP, AFTER_UPDATE_OP.
   */
  void removeMapping(RegionEntry entry, int opCode) throws IMQException {
    // Now we are not going to remove anything before update or when cleaning thread locals
    // In fact we will only Replace not remove and add now on.
    if (opCode == BEFORE_UPDATE_OP || opCode == CLEAN_UP_THREAD_LOCALS) {
      return;
    }
    // System.out.println("RangeIndex.removeMapping "+entry.getKey());
    // @todo ericz Why is a removal being counted as an update?
    Object values = this.entryToValuesMap.get(entry);
    if (values == null) {
      if (nullMappedEntries.containsEntry(entry)) {
        this.internalIndexStats.incNumValues(-nullMappedEntries.getNumValues(entry));
        nullMappedEntries.remove(entry);
      } else {
        this.internalIndexStats.incNumValues(-undefinedMappedEntries.getNumValues(entry));
        undefinedMappedEntries.remove(entry);
      }
    } else if (values instanceof Collection) {
      Iterator valuesIter = ((Iterable) values).iterator();
      while (valuesIter.hasNext()) {
        Object key = valuesIter.next();
        RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) this.valueToEntriesMap.get(key);
        this.internalIndexStats.incNumValues(-rvMap.getNumValues(entry));
        rvMap.remove(entry);
        if (rvMap.getNumEntries() == 0) {
          synchronized (rvMap) {
            if (rvMap.getNumEntries() == 0) {
              if (this.valueToEntriesMap.remove(key, rvMap)) {
                this.internalIndexStats.incNumKeys(-1);
                --valueToEntriesMapSize;
              }
            }
          }
        }
      }

      this.entryToValuesMap.remove(entry);
    } else {
      RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) this.valueToEntriesMap.get(values);

      // Shobhit: Fix for bug #44123
      // rvMap Can not be null if values were found in reverse map. rvMap can
      // only be
      // null in case when "values" has wrong compareTo() implementation.
      if (rvMap == null) {
        throw new IMQException(
            String.format("Indexed object's class %s compareTo function is errorneous.",
                values.getClass().getName()));
      }
      this.internalIndexStats.incNumValues(-rvMap.getNumValues(entry));
      rvMap.remove(entry);
      if (rvMap.getNumEntries() == 0) {
        synchronized (rvMap) {
          if (rvMap.getNumEntries() == 0) {
            if (this.valueToEntriesMap.remove(values, rvMap)) {
              this.internalIndexStats.incNumKeys(-1);
              --valueToEntriesMapSize;
            }
          }
        }
      }
      this.entryToValuesMap.remove(entry);
    }
    this.internalIndexStats.incNumUpdates();
  }

  // Asif TODO: Provide explanation of the method. Test this method
  @Override
  public List queryEquijoinCondition(IndexProtocol indx, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    // get a read lock when doing a lookup
    long start = updateIndexUseStats();
    ((AbstractIndex) indx).updateIndexUseStats();
    List data = new ArrayList();
    Iterator inner = null;
    try {
      // We will iterate over each of the valueToEntries Map to obatin the keys
      // & its correspodning
      // Entry to ResultSet Map
      Iterator outer = this.valueToEntriesMap.entrySet().iterator();

      if (indx instanceof CompactRangeIndex) {
        inner = ((CompactRangeIndex) indx).getIndexStorage().iterator(null);
      } else {
        inner = ((RangeIndex) indx).getValueToEntriesMap().entrySet().iterator();
      }
      Map.Entry outerEntry = null;
      Object innerEntry = null;
      Object outerKey = null;
      Object innerKey = null;
      boolean incrementInner = true;
      outer: while (outer.hasNext()) {
        outerEntry = (Map.Entry) outer.next();
        outerKey = outerEntry.getKey();
        while (!incrementInner || inner.hasNext()) {
          if (incrementInner) {
            innerEntry = inner.next();
            if (innerEntry instanceof IndexStoreEntry) {
              innerKey = ((IndexStoreEntry) innerEntry).getDeserializedKey();
            } else {
              innerKey = ((Map.Entry) innerEntry).getKey();
            }

          }
          int compare = ((Comparable) outerKey).compareTo(innerKey);
          if (compare == 0) {
            // Asif :Select the data
            // incrementOuter = true;
            Object innerValue = null;
            if (innerEntry instanceof IndexStoreEntry) {
              innerValue = ((CompactRangeIndex) indx).getIndexStorage().get(outerKey);
            } else {
              innerValue = ((Map.Entry) innerEntry).getValue();
            }
            // GEODE-5440: need to pass in the key value to do EquiJoin
            populateListForEquiJoin(data, outerEntry.getValue(), innerValue, context,
                outerEntry.getKey());
            incrementInner = true;
            continue outer;
          } else if (compare < 0) {
            // Asif :The outer key is smaller than the inner key. That means
            // that we need to increment the outer loop without moving inner loop.
            // incrementOuter = true;
            incrementInner = false;
            continue outer;
          } else {
            // Asif : The outer key is greater than inner key , so increment the
            // inner loop without changing outer
            incrementInner = true;
          }
        }
        break;
      }
      return data;
    } finally {
      ((AbstractIndex) indx).updateIndexUseEndStats(start);
      updateIndexUseEndStats(start);
      if (inner != null && indx instanceof CompactRangeIndex) {
        ((CloseableIterator<IndexStoreEntry>) inner).close();
      }
    }
  }

  public int getSizeEstimate(Object key, int operator, int matchLevel)
      throws TypeMismatchException {
    // Get approx size;
    int size = 0;
    long start = updateIndexUseStats(false);
    try {
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ: {
          if (key == null) {
            size = this.nullMappedEntries.getNumValues();
          } else if (key == QueryService.UNDEFINED) {
            size = this.undefinedMappedEntries.getNumValues();
          } else {
            key = TypeUtils.indexKeyFor(key);
            key = getPdxStringForIndexedPdxKeys(key);
            RegionEntryToValuesMap valMap =
                (RegionEntryToValuesMap) this.valueToEntriesMap.get(key);

            size = valMap == null ? 0 : valMap.getNumValues();
          }
          break;
        }
        case OQLLexerTokenTypes.TOK_NE_ALT:
        case OQLLexerTokenTypes.TOK_NE: // add all btree values
          // size = matchLevel <=0 ?this.valueToEntriesMap.size():Integer.MAX_VALUE;
          size = this.region.size();
          if (key == null) {
            size -= this.nullMappedEntries.getNumValues();
          } else if (key == QueryService.UNDEFINED) {
            size -= this.undefinedMappedEntries.getNumValues();
          } else {
            key = TypeUtils.indexKeyFor(key);
            key = getPdxStringForIndexedPdxKeys(key);
            RegionEntryToValuesMap valMap =
                (RegionEntryToValuesMap) this.valueToEntriesMap.get(key);
            size -= valMap == null ? 0 : valMap.getNumValues();
          }
          break;
        case OQLLexerTokenTypes.TOK_LE:
        case OQLLexerTokenTypes.TOK_LT:
          if (matchLevel <= 0 && key instanceof Number) {
            int totalSize = valueToEntriesMapSize;// this.valueToEntriesMap.size();
            if (RangeIndex.testHook != null) {
              RangeIndex.testHook.hook(1);
            }
            if (totalSize > 1) {
              Number keyAsNum = (Number) key;
              int x = 0;
              Map.Entry firstEntry = this.valueToEntriesMap.firstEntry();
              Map.Entry lastEntry = this.valueToEntriesMap.lastEntry();

              if (firstEntry != null && lastEntry != null) {
                Number first = (Number) firstEntry.getKey();
                Number last = (Number) lastEntry.getKey();
                if (first.doubleValue() != last.doubleValue()) {
                  // Shobhit: Now without ReadLoack on index we can end up with 0 in
                  // denominator.
                  // can end up with 0 in denominator if the numbers are floating-point
                  // and truncated with conversion to long, and the first and last keys
                  // truncate to the same long, so safest calculation is to convert to doubles
                  x = (int) (((keyAsNum.doubleValue() - first.doubleValue()) * totalSize)
                      / (last.doubleValue() - first.doubleValue()));
                }
              }
              if (x < 0) {
                x = 0;
              }
              size = (int) x;
            } else {
              // not attempting to differentiate between LT & LE
              size = this.valueToEntriesMap.containsKey(key) ? 1 : 0;
            }
          } else {
            size = MAX_VALUE;
          }
          break;

        case OQLLexerTokenTypes.TOK_GE:
        case OQLLexerTokenTypes.TOK_GT:
          if (matchLevel <= 0 && key instanceof Number) {
            int totalSize = valueToEntriesMapSize;// this.valueToEntriesMap.size();
            if (testHook != null) {
              testHook.hook(2);
            }
            if (totalSize > 1) {
              Number keyAsNum = (Number) key;
              int x = 0;
              Map.Entry firstEntry = this.valueToEntriesMap.firstEntry();
              Map.Entry lastEntry = this.valueToEntriesMap.lastEntry();
              if (firstEntry != null && lastEntry != null) {
                Number first = (Number) firstEntry.getKey();
                Number last = (Number) lastEntry.getKey();
                if (first.doubleValue() != last.doubleValue()) {
                  // Shobhit: Now without ReadLoack on index we can end up with 0
                  // in
                  // denominator.
                  // can end up with 0 in denominator if the numbers are
                  // floating-point
                  // and truncated with conversion to long, and the first and last
                  // keys
                  // truncate to the same long,
                  // so safest calculation is to convert to doubles
                  double totalRange = last.doubleValue() - first.doubleValue();
                  assert totalRange != 0.0 : this.valueToEntriesMap.keySet();
                  double part = last.doubleValue() - keyAsNum.doubleValue();
                  x = (int) ((part * totalSize) / totalRange);
                }
              }
              if (x < 0) {
                x = 0;
              }
              size = x;
            } else {
              // not attempting to differentiate between GT & GE
              size = this.valueToEntriesMap.containsKey(key) ? 1 : 0;
            }
          } else {
            size = MAX_VALUE;
          }
          break;
      }
    } finally {
      updateIndexUseEndStats(start, false);
    }
    return size;
  }

  private void evaluate(Object key, int operator, Collection results, Set keysToRemove, int limit,
      ExecutionContext context) throws TypeMismatchException {
    key = TypeUtils.indexKeyFor(key);
    Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
    boolean multiColOrderBy = false;
    boolean asc = true;
    List orderByAttrs = null;
    if (orderByClause != null && orderByClause) {
      orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttrs.get(0);
      asc = !csc.getCriterion();
      multiColOrderBy = orderByAttrs.size() > 1;
    }

    limit = multiColOrderBy ? -1 : limit;

    try {
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ: {
          assert keysToRemove == null;
          addValuesToResult(this.valueToEntriesMap.get(key), results, keysToRemove, limit, context);
          break;
        }
        case OQLLexerTokenTypes.TOK_LT: {
          NavigableMap sm = this.valueToEntriesMap.headMap(key, false);
          sm = asc ? sm : sm.descendingMap();

          addValuesToResult(sm, results, keysToRemove, limit, context);
          break;
        }
        case OQLLexerTokenTypes.TOK_LE: {

          NavigableMap sm = this.valueToEntriesMap.headMap(key, true);
          sm = asc ? sm : sm.descendingMap();

          addValuesToResult(sm, results, keysToRemove, limit, context);
          break;
        }
        case OQLLexerTokenTypes.TOK_GT: {
          // Asif:As tail Map returns the SortedMap vie which is greater than or
          // equal to the key passed, the equal to key needs to be removed.
          // However if the boundtary key is already part of the keysToRemove set
          // then we do not have to remove it as it is already taken care of

          NavigableMap sm = this.valueToEntriesMap.tailMap(key, false);
          sm = asc ? sm : sm.descendingMap();
          addValuesToResult(sm, results, keysToRemove, limit, context);
          break;
        }
        case OQLLexerTokenTypes.TOK_GE: {
          NavigableMap sm = this.valueToEntriesMap.tailMap(key, true);
          sm = asc ? sm : sm.descendingMap();
          addValuesToResult(sm, results, keysToRemove, limit, context);
          break;
        }
        case OQLLexerTokenTypes.TOK_NE_ALT:
        case OQLLexerTokenTypes.TOK_NE: {
          NavigableMap sm = this.valueToEntriesMap;
          if (!asc) {
            sm = sm.descendingMap();

          }
          if (keysToRemove == null) {
            addValuesToResultSingleKeyToRemove(sm, results, key, limit, context);
          } else {
            // TODO:Asif Somehow avoid this removal & then addition
            keysToRemove.add(key);
            addValuesToResult(sm, results, keysToRemove, limit, context);
          }
          nullMappedEntries.addValuesToCollection(results, limit, context);
          undefinedMappedEntries.addValuesToCollection(results, limit, context);
          // removeValuesFromResult(this.valueToEntriesMap.get(key), results);

          break;
        }
        default: {
          throw new IllegalArgumentException(
              String.format("Operator, %s", valueOf(operator)));
        }
      } // end switch
    } catch (ClassCastException ex) {
      if (operator == OQLLexerTokenTypes.TOK_EQ) { // result is empty set
        return;
      } else if (operator == OQLLexerTokenTypes.TOK_NE
          || operator == OQLLexerTokenTypes.TOK_NE_ALT) { // put
        // all
        // in
        // result
        NavigableMap sm = this.valueToEntriesMap;
        if (!asc) {
          sm = sm.descendingMap();
        }
        addValuesToResult(sm, results, keysToRemove, limit, context);
        nullMappedEntries.addValuesToCollection(results, limit, context);
        undefinedMappedEntries.addValuesToCollection(results, limit, context);
      } else { // otherwise throw exception
        throw new TypeMismatchException("", ex);
      }
    }
  }

  private void evaluate(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection, int limit, boolean applyOrderBy,
      List orderByAttribs) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    key = TypeUtils.indexKeyFor(key);
    boolean multiColOrderBy = false;
    boolean asc = true;
    if (applyOrderBy) {
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttribs.get(0);
      asc = !csc.getCriterion();
      multiColOrderBy = orderByAttribs.size() > 1;
    }
    limit = multiColOrderBy ? -1 : limit;


    try {
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ: {
          addValuesToResult(this.valueToEntriesMap.get(key), results, null, iterOps, runtimeItr,
              context, projAttrib, intermediateResults, isIntersection, limit);
          break;
        }
        case OQLLexerTokenTypes.TOK_LT: {

          NavigableMap sm = this.valueToEntriesMap.headMap(key, false);
          sm = asc ? sm : sm.descendingMap();

          addValuesToResult(sm, results, null, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit);

          break;
        }
        case OQLLexerTokenTypes.TOK_LE: {
          NavigableMap sm = this.valueToEntriesMap.headMap(key, true);
          sm = asc ? sm : sm.descendingMap();

          addValuesToResult(sm, results, null, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit);
          break;
        }
        case OQLLexerTokenTypes.TOK_GT: {
          // Asif:As tail Map returns the SortedMap vie which is greater
          // than or equal
          // to the key passed, the equal to key needs to be removed.
          // However if the boundary key is already part of the
          // keysToRemove set
          // then we do not have to remove it as it is already taken care
          // of

          NavigableMap sm = this.valueToEntriesMap.tailMap(key, false);
          sm = asc ? sm : sm.descendingMap();

          addValuesToResult(sm, results, null, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit);


          break;
        }
        case OQLLexerTokenTypes.TOK_GE: {
          NavigableMap sm = this.valueToEntriesMap.tailMap(key, true);
          sm = asc ? sm : sm.descendingMap();

          addValuesToResult(sm, results, null, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit);

          break;
        }
        case OQLLexerTokenTypes.TOK_NE_ALT:
        case OQLLexerTokenTypes.TOK_NE: {
          NavigableMap sm = this.valueToEntriesMap;
          if (!asc) {
            sm = sm.descendingMap();
          }
          addValuesToResult(sm, results, key, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit);

          nullMappedEntries.addValuesToCollection(results, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit);
          undefinedMappedEntries.addValuesToCollection(results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, limit);

          break;
        }
        default: {
          throw new IllegalArgumentException("Operator = " + operator);
        }
      } // end switch
    } catch (ClassCastException ex) {
      if (operator == OQLLexerTokenTypes.TOK_EQ) { // result is empty
        // set
        return;
      } else if (operator == OQLLexerTokenTypes.TOK_NE
          || operator == OQLLexerTokenTypes.TOK_NE_ALT) { // put
        // all
        // in
        // result
        NavigableMap sm = this.valueToEntriesMap;
        if (!asc) {
          sm = sm.descendingMap();
        }
        addValuesToResult(sm, results, key, iterOps, runtimeItr, context, projAttrib,
            intermediateResults, isIntersection, limit);

        nullMappedEntries.addValuesToCollection(results, iterOps, runtimeItr, context, projAttrib,
            intermediateResults, isIntersection, limit);
        undefinedMappedEntries.addValuesToCollection(results, iterOps, runtimeItr, context,
            projAttrib, intermediateResults, isIntersection, limit);
      } else { // otherwise throw exception
        throw new TypeMismatchException("", ex);
      }
    }
  }

  /**
   *
   * @param entriesMap SortedMap object containing the indexed key as the key & the value being
   *        RegionEntryToValues Map object containing the indexed results
   * @param result Index Results holder Collection used for fetching the index results
   * @param keysToRemove Set containing the index keys for which index results should not be taken
   *        from the entriesMap
   */

  private void addValuesToResult(Object entriesMap, Collection result, Set keysToRemove, int limit,
      ExecutionContext context) {
    if (entriesMap == null || result == null)
      return;
    QueryObserver observer = QueryObserverHolder.getInstance();
    if (verifyLimit(result, limit)) {
      observer.limitAppliedAtIndexLevel(this, limit, result);
      return;
    }
    if (entriesMap instanceof SortedMap) {
      if (((Map) entriesMap).isEmpty()) { // bug#40514
        return;
      }

      SortedMap sortedMap = (SortedMap) entriesMap;
      Iterator entriesIter = sortedMap.entrySet().iterator();
      Map.Entry entry = null;
      while (entriesIter.hasNext()) {
        entry = (Map.Entry) entriesIter.next();
        Object key = entry.getKey();
        if (keysToRemove == null || !keysToRemove.remove(key)) {
          RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) entry.getValue();
          rvMap.addValuesToCollection(result, limit, context);
          if (verifyLimit(result, limit)) {
            observer.limitAppliedAtIndexLevel(this, limit, result);
            return;
          }
        }
      }
    } else if (entriesMap instanceof RegionEntryToValuesMap) {
      // We have already been passed the collection to add, assuming keys to remove is null or
      // already been applied
      RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) entriesMap;
      rvMap.addValuesToCollection(result, limit, context);
      if (limit != -1 && result.size() == limit) {
        observer.limitAppliedAtIndexLevel(this, limit, result);
        return;
      }
    } else {
      throw new RuntimeException(
          "Problem in index query");
    }
  }



  /**
   *
   * @param entriesMap SortedMap object containing the indexed key as the key & the value being
   *        RegionEntryToValues Map object containing the indexed results
   * @param result Index Results holder Collection used for fetching the index results
   * @param keyToRemove The index key object for which index result should not be taken from the
   *        entriesMap
   * @param limit The limit to be applied on the resultset. If no limit is to be applied the value
   *        will be -1
   */
  private void addValuesToResultSingleKeyToRemove(Object entriesMap, Collection result,
      Object keyToRemove, int limit, ExecutionContext context) {
    if (entriesMap == null || result == null)
      return;
    QueryObserver observer = QueryObserverHolder.getInstance();
    if (verifyLimit(result, limit)) {
      observer.limitAppliedAtIndexLevel(this, limit, result);
      return;
    }
    assert entriesMap instanceof SortedMap;
    Iterator entriesIter = ((Map) entriesMap).entrySet().iterator();
    Map.Entry entry = null;
    boolean foundKeyToRemove = false;
    while (entriesIter.hasNext()) {
      entry = (Map.Entry) entriesIter.next();
      // Object key = entry.getKey();
      if (foundKeyToRemove || !keyToRemove.equals(entry.getKey())) {
        RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) entry.getValue();
        rvMap.addValuesToCollection(result, limit, context);
        if (verifyLimit(result, limit)) {
          observer.limitAppliedAtIndexLevel(this, limit, result);
          return;
        }
      } else {
        foundKeyToRemove = true;
      }
    }
  }

  private void addValuesToResult(Object entriesMap, Collection result, Object keyToRemove,
      CompiledValue iterOps, RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection, int limit)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    boolean limitApplied = false;
    if (entriesMap == null || result == null) {
      if (verifyLimit(result, limit)) {
        QueryObserver observer = QueryObserverHolder.getInstance();
        if (observer != null) {
          observer.limitAppliedAtIndexLevel(this, limit, result);
        }
      }
      return;
    }
    QueryObserver observer = QueryObserverHolder.getInstance();
    if (entriesMap instanceof SortedMap) {
      Iterator entriesIter = ((Map) entriesMap).entrySet().iterator();
      Map.Entry entry = null;
      boolean foundKeyToRemove = false;

      // That means we aren't removing any keys (remember if we are matching for nulls, we have the
      // null maps
      if (keyToRemove == null) {
        foundKeyToRemove = true;
      }
      while (entriesIter.hasNext()) {
        entry = (Map.Entry) entriesIter.next();
        // Object key = entry.getKey();
        if (foundKeyToRemove || !keyToRemove.equals(entry.getKey())) {
          RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) entry.getValue();
          rvMap.addValuesToCollection(result, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit);
          if (verifyLimit(result, limit)) {
            observer.limitAppliedAtIndexLevel(this, limit, result);
            break;
          }
        } else {
          foundKeyToRemove = true;
        }
      }
    } else if (entriesMap instanceof RegionEntryToValuesMap) {
      RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) entriesMap;
      rvMap.addValuesToCollection(result, iterOps, runtimeItr, context, projAttrib,
          intermediateResults, isIntersection, limit);
    } else {
      throw new RuntimeException(
          "Problem in index query");
    }
  }

  /*
   * private void removeValuesFromResult(Object entriesMap, Collection result) { if (entriesMap ==
   * null || result == null) return; if (entriesMap instanceof SortedMap) { Iterator entriesIter =
   * ((SortedMap) entriesMap).values().iterator(); while (entriesIter.hasNext()) {
   * RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) entriesIter .next();
   * rvMap.removeValuesFromCollection(result); } } else if (entriesMap instanceof
   * RegionEntryToValuesMap) { RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap) entriesMap;
   * rvMap.removeValuesFromCollection(result); } else { throw new
   * RuntimeException("Problem in index query"); } }
   *
   * private void removeValuesFromResult(Object entriesMap, Collection result,CompiledValue iterOps,
   * RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib, SelectResults
   * intermediateResults, boolean isIntersection) throws FunctionDomainException,
   * TypeMismatchException, NameResolutionException, QueryInvocationTargetException { if (entriesMap
   * == null || result == null) return; if (entriesMap instanceof SortedMap) { Iterator entriesIter
   * = ((SortedMap)entriesMap).values().iterator(); while (entriesIter.hasNext()) {
   * RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap)entriesIter .next();
   * rvMap.removeValuesFromCollection(result, iterOps, runtimeItr, context, projAttrib,
   * intermediateResults, isIntersection); } } else if (entriesMap instanceof
   * RegionEntryToValuesMap) { RegionEntryToValuesMap rvMap = (RegionEntryToValuesMap)entriesMap;
   * rvMap.removeValuesFromCollection(result, iterOps, runtimeItr, context, projAttrib,
   * intermediateResults, isIntersection); } else { throw new
   * RuntimeException("Problem in index query"); } }
   */

  void recreateIndexData() throws IMQException {
    /*
     * Asif : Mark the data maps to null & call the initialization code of index
     */
    // TODO:Asif : The statistics data needs to be modified appropriately
    // for the clear operation
    this.valueToEntriesMap.clear();
    this.entryToValuesMap.clear();
    this.nullMappedEntries.clear();
    this.undefinedMappedEntries.clear();
    int numKeys = (int) this.internalIndexStats.getNumberOfKeys();
    if (numKeys > 0) {
      this.internalIndexStats.incNumKeys(-numKeys);
    }
    int numValues = (int) this.internalIndexStats.getNumberOfValues();
    if (numValues > 0) {
      this.internalIndexStats.incNumValues(-numValues);
    }
    int updates = (int) this.internalIndexStats.getNumUpdates();
    if (updates > 0) {
      this.internalIndexStats.incNumUpdates(updates);
    }
    this.initializeIndex(true);
  }

  void lockedQuery(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection) throws TypeMismatchException,
      FunctionDomainException, NameResolutionException, QueryInvocationTargetException {
    int limit = -1;

    Boolean applyLimit = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX);
    if (applyLimit != null && applyLimit) {
      limit = (Integer) context.cacheGet(CompiledValue.RESULT_LIMIT);
    }

    Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
    boolean multiColOrderBy = false;
    List orderByAttrs = null;
    boolean asc = true;
    boolean applyOrderBy = false;
    if (orderByClause != null && orderByClause) {
      orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttrs.get(0);
      asc = !csc.getCriterion();
      multiColOrderBy = orderByAttrs.size() > 1;
      applyOrderBy = true;
    }

    if (key == null) {
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ: {
          nullMappedEntries.addValuesToCollection(results, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit);
          break;
        }
        case OQLLexerTokenTypes.TOK_NE_ALT:
        case OQLLexerTokenTypes.TOK_NE: { // add all btree values
          NavigableMap sm = this.valueToEntriesMap;
          if (!asc) {
            sm = sm.descendingMap();

          }
          // keysToRemove should be null, meaning we aren't removing any keys
          addValuesToResult(sm, results, null, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
          undefinedMappedEntries.addValuesToCollection(results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, limit);
          break;
        }
        default: {
          throw new IllegalArgumentException("Invalid Operator");
        }
      } // end switch
    } else if (key == QueryService.UNDEFINED) { // do nothing
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ: {
          undefinedMappedEntries.addValuesToCollection(results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, limit);
          break;
        }
        case OQLLexerTokenTypes.TOK_NE:
        case OQLLexerTokenTypes.TOK_NE_ALT: { // add all btree values
          NavigableMap sm = this.valueToEntriesMap;

          if (!asc) {
            sm = sm.descendingMap();
          }
          // keysToRemove should be null
          addValuesToResult(sm, results, null, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
          nullMappedEntries.addValuesToCollection(results, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit);
          break;
        }
        default: {
          throw new IllegalArgumentException("Invalid Operator");
        }
      } // end switch
    } else {
      // return if the index map is still empty at this stage
      if (isEmpty()) {
        return;
      }
      key = getPdxStringForIndexedPdxKeys(key);
      evaluate(key, operator, results, iterOps, runtimeItr, context, projAttrib,
          intermediateResults, isIntersection, limit, applyOrderBy, orderByAttrs);
    } // end else
  }

  /** Method called while appropriate lock held */
  @Override
  void lockedQuery(Object key, int operator, Collection results, Set keysToRemove,
      ExecutionContext context) throws TypeMismatchException {
    int limit = -1;

    // not applying limit at this level currently as range index does not properly apply other
    // conditions
    // we could end up returning incorrect results - missing results
    // This querie's limits are restricted up in the RangeIndex/QueryUtils calls.
    // The next step may be to move the query utils cutdown/etc code into the range index itself and
    // pass down the runtimeItrs?
    // but that code is fragile and hard to read.
    // It looks like this path is only for AbstractGroupOrRangeJunction, CompositeGroupJunction,
    // CompiledComparison with some caveats as well as CompiledLike

    Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);

    boolean asc = true;
    List orderByAttrs = null;

    boolean multiColOrderBy = false;
    if (orderByClause != null && orderByClause) {
      orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttrs.get(0);
      asc = !csc.getCriterion();
      multiColOrderBy = orderByAttrs.size() > 1;
    }

    limit = multiColOrderBy ? -1 : limit;

    if (key == null) {
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ: {
          nullMappedEntries.addValuesToCollection(results, limit, context);
          break;
        }
        case OQLLexerTokenTypes.TOK_NE_ALT:
        case OQLLexerTokenTypes.TOK_NE: { // add all btree values
          NavigableMap sm = this.valueToEntriesMap;
          if (!asc) {
            sm = sm.descendingMap();
          }
          // keysToRemove should be null
          addValuesToResult(sm, results, keysToRemove, limit, context);
          undefinedMappedEntries.addValuesToCollection(results, limit, context);
          break;
        }
        default: {
          throw new IllegalArgumentException(
              "Invalid Operator");
        }
      } // end switch
    } else if (key == QueryService.UNDEFINED) { // do nothing
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ: {
          undefinedMappedEntries.addValuesToCollection(results, limit, context);
          break;
        }
        case OQLLexerTokenTypes.TOK_NE:
        case OQLLexerTokenTypes.TOK_NE_ALT: { // add all btree values
          NavigableMap sm = this.valueToEntriesMap.headMap(key, false);
          sm = asc ? sm : sm.descendingMap();
          // keysToRemove should be null
          addValuesToResult(sm, results, keysToRemove, limit, context);
          nullMappedEntries.addValuesToCollection(results, limit, context);
          break;
        }
        default: {
          throw new IllegalArgumentException(
              "Invalid Operator");
        }
      } // end switch
    } else {
      // return if the index map is still empty at this stage
      if (isEmpty()) {
        return;
      }
      key = getPdxStringForIndexedPdxKeys(key);
      evaluate(key, operator, results, keysToRemove, limit, context);
    } // end else
  }

  @Override
  @SuppressWarnings("fallthrough")
  void lockedQuery(Object lowerBoundKey, int lowerBoundOperator, Object upperBoundKey,
      int upperBoundOperator, Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException {
    int limit = -1;
    // not applying limit at this level currently as range index does not properly apply other
    // conditions
    // we could end up returning incorrect results - missing results
    // This querie's limits are restricted up in the RangeIndex/QueryUtils calls.
    // The next step may be to move the query utils cutdown/etc code into the range index itself and
    // pass down the runtimeItrs?
    // but that code is fragile and hard to read.

    Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
    boolean multiColOrderBy = false;
    List orderByAttrs = null;
    boolean asc = true;
    if (orderByClause != null && orderByClause) {
      orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttrs.get(0);
      asc = !csc.getCriterion();
      multiColOrderBy = orderByAttrs.size() > 1;
    }
    limit = multiColOrderBy ? -1 : limit;

    // return if the index map is still empty at this stage
    if (isEmpty()) {
      return;
    }

    lowerBoundKey = TypeUtils.indexKeyFor(lowerBoundKey);
    upperBoundKey = TypeUtils.indexKeyFor(upperBoundKey);
    lowerBoundKey = getPdxStringForIndexedPdxKeys(lowerBoundKey);
    upperBoundKey = getPdxStringForIndexedPdxKeys(upperBoundKey);

    boolean lowerBoundInclusive = lowerBoundOperator == OQLLexerTokenTypes.TOK_GE;
    boolean upperBoundInclusive = upperBoundOperator == OQLLexerTokenTypes.TOK_LE;

    NavigableMap dataset = this.valueToEntriesMap.subMap(lowerBoundKey, lowerBoundInclusive,
        upperBoundKey, upperBoundInclusive);
    dataset = asc ? dataset : dataset.descendingMap();

    Object lowerBoundKeyToRemove = null;
    addValuesToResult(dataset, results, keysToRemove, limit, context);
  }

  @Override
  public boolean containsEntry(RegionEntry entry) {
    return (this.entryToValuesMap.containsEntry(entry)
        || this.nullMappedEntries.containsEntry(entry)
        || this.undefinedMappedEntries.containsEntry(entry));
  }

  public String dump() {
    StringBuilder sb = new StringBuilder(toString()).append(" {\n");
    sb.append("Null Values\n");
    Iterator nI = nullMappedEntries.entrySet().iterator();
    while (nI.hasNext()) {
      Map.Entry mapEntry = (Map.Entry) nI.next();
      RegionEntry e = (RegionEntry) mapEntry.getKey();
      Object value = mapEntry.getValue();
      sb.append("  RegionEntry.key = ").append(e.getKey());
      sb.append("  Value.type = ").append(value.getClass().getName());
      if (value instanceof Collection) {
        sb.append("  Value.size = ").append(((Collection) value).size());
      }
      sb.append("\n");
    }
    sb.append(" -----------------------------------------------\n");
    sb.append("Undefined Values\n");
    Iterator uI = undefinedMappedEntries.entrySet().iterator();
    while (uI.hasNext()) {
      Map.Entry mapEntry = (Map.Entry) uI.next();
      RegionEntry e = (RegionEntry) mapEntry.getKey();
      Object value = mapEntry.getValue();
      sb.append("  RegionEntry.key = ").append(e.getKey());
      sb.append("  Value.type = ").append(value.getClass().getName());
      if (value instanceof Collection) {
        sb.append("  Value.size = ").append(((Collection) value).size());
      }
      sb.append("\n");
    }
    sb.append(" -----------------------------------------------\n");
    Iterator i1 = this.valueToEntriesMap.entrySet().iterator();
    while (i1.hasNext()) {
      Map.Entry indexEntry = (Map.Entry) i1.next();
      sb.append(" Key = ").append(indexEntry.getKey()).append("\n");
      sb.append(" Value Type = ").append(" ").append(indexEntry.getValue().getClass().getName())
          .append("\n");
      if (indexEntry.getValue() instanceof Map) {
        sb.append(" Value Size = ").append(" ").append(((Map) indexEntry.getValue()).size())
            .append("\n");
      }
      Iterator i2 = ((RegionEntryToValuesMap) indexEntry.getValue()).entrySet().iterator();
      while (i2.hasNext()) {
        Map.Entry mapEntry = (Map.Entry) i2.next();
        RegionEntry e = (RegionEntry) mapEntry.getKey();
        Object value = mapEntry.getValue();
        sb.append("  RegionEntry.key = ").append(e.getKey());
        sb.append("  Value.type = ").append(value.getClass().getName());
        if (value instanceof Collection) {
          sb.append("  Value.size = ").append(((Collection) value).size());
        }
        sb.append("\n");
        // sb.append(" Value.type = ").append(value).append("\n");
      }
      sb.append(" -----------------------------------------------\n");
    }
    sb.append("}// Index ").append(getName()).append(" end");
    return sb.toString();
  }

  public static void setTestHook(TestHook hook) {
    RangeIndex.testHook = hook;
  }

  protected InternalIndexStatistics createStats(String indexName) {
    return new RangeIndexStatistics(indexName);
  }

  class RangeIndexStatistics extends InternalIndexStatistics {

    private IndexStats vsdStats;

    public RangeIndexStatistics(String indexName) {

      this.vsdStats = new IndexStats(getRegion().getCache().getDistributedSystem(), indexName);
    }

    /**
     * Return the total number of times this index has been updated
     */
    public long getNumUpdates() {
      return this.vsdStats.getNumUpdates();
    }

    public void incNumValues(int delta) {
      this.vsdStats.incNumValues(delta);
    }

    public void updateNumKeys(long numKeys) {
      this.vsdStats.updateNumKeys(numKeys);
    }

    public void incNumKeys(long numKeys) {
      this.vsdStats.incNumKeys(numKeys);
    }

    public void incNumUpdates() {
      this.vsdStats.incNumUpdates();
    }

    public void incNumUpdates(int delta) {
      this.vsdStats.incNumUpdates(delta);
    }

    public void incUpdateTime(long delta) {
      this.vsdStats.incUpdateTime(delta);
    }

    public void incUpdatesInProgress(int delta) {
      this.vsdStats.incUpdatesInProgress(delta);
    }

    public void incNumUses() {
      this.vsdStats.incNumUses();
    }

    public void incUseTime(long delta) {
      this.vsdStats.incUseTime(delta);
    }

    public void incUsesInProgress(int delta) {
      this.vsdStats.incUsesInProgress(delta);
    }

    public void incReadLockCount(int delta) {
      this.vsdStats.incReadLockCount(delta);
    }

    /**
     * Returns the total amount of time (in nanoseconds) spent updating this index.
     */
    public long getTotalUpdateTime() {
      return this.vsdStats.getTotalUpdateTime();
    }

    /**
     * Returns the total number of times this index has been accessed by a query.
     */
    public long getTotalUses() {
      return this.vsdStats.getTotalUses();
    }

    /**
     * Returns the number of keys in this index.
     */
    public long getNumberOfKeys() {
      return this.vsdStats.getNumberOfKeys();
    }

    /**
     * Returns the number of values in this index.
     */
    public long getNumberOfValues() {
      return this.vsdStats.getNumberOfValues();
    }

    /**
     * Return the number of values for the specified key in this index.
     */
    public long getNumberOfValues(Object key) {
      if (key == null)
        return nullMappedEntries.getNumValues();
      if (key == QueryService.UNDEFINED)
        return undefinedMappedEntries.getNumValues();
      RegionEntryToValuesMap rvMap =
          (RegionEntryToValuesMap) RangeIndex.this.valueToEntriesMap.get(key);
      if (rvMap == null)
        return 0;
      return rvMap.getNumValues();
    }

    /**
     * Return the number of read locks taken on this index
     */
    public int getReadLockCount() {
      return this.vsdStats.getReadLockCount();
    }

    public void close() {
      this.vsdStats.close();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("No Keys = ").append(getNumberOfKeys()).append("\n");
      sb.append("No Values = ").append(getNumberOfValues()).append("\n");
      sb.append("No Uses = ").append(getTotalUses()).append("\n");
      sb.append("No Updates = ").append(getNumUpdates()).append("\n");
      sb.append("Total Update time = ").append(getTotalUpdateTime()).append("\n");
      return sb.toString();
    }
  }

  public boolean isEmpty() {
    return valueToEntriesMapSize == 0 ? true : false;
  }

  @Override
  public Map getValueToEntriesMap() {
    return valueToEntriesMap;
  }

}
