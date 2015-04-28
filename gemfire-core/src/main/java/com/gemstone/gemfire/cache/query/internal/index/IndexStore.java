/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;

import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;

/**
 * 
 * @author Tejas Nomulwar
 * @author Jason Huynh
 * @since 8.0
 */
public interface IndexStore {

  /**
   * Add a mapping to the index store
   * 
   * @param indexKey
   * @param re
   */
  public void addMapping(Object indexKey, RegionEntry re) throws IMQException;

  /**
   * Remove a mapping from the index store
   * 
   * @param indexKey
   * @param re
   */
  public void removeMapping(Object indexKey, RegionEntry re)
      throws IMQException;

  /**
   * Update a mapping in the index store. This method adds a new
   * mapping and removes the old mapping
   * 
   * @param indexKey
   * @param oldKey
   * @param re
   */
  public void updateMapping(Object indexKey, Object oldKey, RegionEntry re, Object oldValue)
      throws IMQException;

  public String printAll();
  
  /**
   * Return all of the IndexStoreEntries that map to a given region key.
   */
  public CloseableIterator<IndexStoreEntry> get(Object indexKey);

  /**
   * Return all of the IndexStoreEntries in the range between start and end.
   */
  public CloseableIterator<IndexStoreEntry> iterator(Object start,
      boolean startInclusive, Object end, boolean endInclusive,
      Collection keysToRemove);

  /**
   * Return all of the IndexStorageEntries that from start to the tail of the
   * map.
   */
  public CloseableIterator<IndexStoreEntry> iterator(Object start,
      boolean startInclusive, Collection keysToRemove);

  /**
   * Return all of the IndexStorageEntries in the map.
   */
  public CloseableIterator<IndexStoreEntry> iterator(Collection keysToRemove);

  /**
   * Return all of the IndexStoreEntries from the end to the head of the map.
   */
  public CloseableIterator<IndexStoreEntry> descendingIterator(Object end,
      boolean endInclusive, Collection keysToRemove);

  /**
   * Return all of the IndexStoreEntries in the map in descending order.
   */
  public CloseableIterator<IndexStoreEntry> descendingIterator(
      Collection keysToRemove);

  /**
   * Return all of the IndexStoreEntries in the range between end and start.
   */
  public CloseableIterator<IndexStoreEntry> descendingIterator(Object start,
      boolean startInclusive, Object end, boolean endInclusive,
      Collection keysToRemove);

  /**
   * Return the number of IndexStoreEntries that map to a given Index key.
   */
  public int size(Object key);

  /**
   * Return the total keys in the Index store
   */
  public int size();

  public boolean clear();

  interface IndexStoreEntry {
    /**
     * returns deserialized key
     */
    Object getDeserializedKey();

    /**
     * returns deserialized Region key or Value based on the whether the index
     * is on Region keys or Region values. Defaults to RegionEntry
     */
    Object getDeserializedValue();

    /**
     * returns deserialized region key
     */
    Object getDeserializedRegionKey();

    /**
     * 
     * @return true if the RegionEntry is under update
     * @see RegionEntry#isUpdateInProgress() isUpdateInProgress
     */
    boolean isUpdateInProgress();
  }

  public boolean isIndexOnRegionKeys();

  public void setIndexOnRegionKeys(boolean indexOnRegionKeys);

  public boolean isIndexOnValues();

  public void setIndexOnValues(boolean indexOnValues);

  public Object getTargetObject(RegionEntry entry);

  public Object getTargetObjectInVM(RegionEntry entry);
}
