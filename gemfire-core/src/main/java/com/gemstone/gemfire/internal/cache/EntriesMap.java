/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.InternalStatisticsDisabledException;
import com.gemstone.gemfire.internal.util.Callable;
//import com.gemstone.gemfire.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentMap;
//import com.gemstone.gemfire.util.concurrent.locks.*;
import com.gemstone.gemfire.distributed.internal.DM;

/**
 * Interface for accessing extended features of a ConcurrentMap used for
 * the entries of a Region.
 *
 * @author Eric Zoerner
 *
 */
public interface EntriesMap extends ConcurrentMap {
  
  /**
   * Parameter object used to facilitate construction of an EntriesMap.
   * Modification of fields after the map is constructed has no effect.
   */
  static class Attributes {
    /** The initial capacity. The implementation
     * performs internal sizing to accommodate this many elements. */
//    int initialCapacity = 16;
    
    /** the load factor threshold, used to control resizing. */
//    float loadFactor = 0.75f;
    
    /** the estimated number of concurrently
     * updating threads. The implementation performs internal sizing
     * to try to accommodate this many threads. */
//    int concurrencyLevel = 16;
    
    /** whether "api" statistics are enabled */
//    boolean statisticsEnabled = false;
    
    /** whether LRU stats are required */
//    boolean lru = false;
  }

  /**
   * Returns the internal entry object to which the specified key is mapped in
   * this table. For internal use only.
   * This entry may be cloned at any time that the write lock is not
   * acquired.
   *
   * @param   key   a key in the table.
   * @return  the entry to which the key is mapped in this table;
   *          <tt>null</tt> if the key is not mapped to any value in
   *          this table.
   * @throws  NullPointerException  if the key is
   *               <tt>null</tt>.
   */
  Entry getEntry(Object key);
  
  
  /** Execute runnable synchronized with modifications to map */
  void writeSynchronized(Object key, Runnable runnable);
  
  /** Execute callable synchronized with modifications to map */
  Object writeSynchronized(Object key, Callable callable) throws Exception;
 
  /**
   * Returns the value of an entry as it resides in the VM.
   * @return the value or EntryEvent.NOT_AVAILABLE token if it's not in
   * the VM or null if the entry doesn't exist.
   *
   * @see LocalRegion#getValueInVM
   */
  public Object getValueInVM(Object key);
 
  /**
   * Returns the value of an entry as it resides on disk.  For
   * testing purposes only.
   *
   * @see LocalRegion#getValueOnDisk
   */
  public Object getValueOnDisk(Object key)
    throws EntryNotFoundException;    

  /**
   * Fill in value, and isSerialized fields
   * in this entry object (used for getInitialImage and sync recovered)
   * Also sets the lastModified time in cacheTime.
   * Only called for DistributedRegions.
   *
   * @see InitialImageOperation.RequestImageMessage#chunkEntries
   *
   * @return false if map entry not found
   * @since 3.2.1
   */
  public boolean fillInValue(InitialImageOperation.Entry entry, DM mgr);       

  
  /**
   * Interface for an Entry returned by the getEntry method.
   * Note that an entry obtained from an entrySet() does not implement
   * this interface, but is a Map.Entry.
   *
   * SYNCHRONIZATION Rules:
   * Any method that calls a method on an Entry that modifies fields
   * in the entry must be protected by the writeSynchronized method
   * BEFORE the entry is obtained to prevent it
   * from being cloned in the map while you have a reference.
   */
  interface Entry {
    
    Object getKey();
    
    /** Gets the value for this entry. For DiskRegions, faults in value
     *  and returns it
     *  @param map the map this entry came from
     */
    Object getValue(EntriesMap map);
    
    long getLastModified();
    
    long getLastAccessed() throws InternalStatisticsDisabledException;
    
    long getHitCount() throws InternalStatisticsDisabledException;
    
    long getMissCount() throws InternalStatisticsDisabledException;

    /** CALLER MUST BE WRITESYNCHRONIZED FROM BEFORE ENTRY WAS RETRIEVED */
    void updateStatsForPut(LocalRegion region, long lastModifiedTime);
    
    /** CALLER MUST BE WRITESYNCHRONIZED FROM BEFORE ENTRY WAS RETRIEVED */
    void updateStatsForGet(LocalRegion region, boolean hit, long time)
    throws InternalStatisticsDisabledException;

    /** CALLER MUST BE WRITESYNCHRONIZED FROM BEFORE ENTRY WAS RETRIEVED */
    void resetCounts() throws InternalStatisticsDisabledException;
  }
}
