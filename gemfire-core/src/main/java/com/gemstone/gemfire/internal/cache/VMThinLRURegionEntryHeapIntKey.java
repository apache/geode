/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;
// DO NOT modify this class. It was generated from LeafRegionEntry.cpp
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import com.gemstone.gemfire.internal.cache.lru.EnableLRU;
import com.gemstone.gemfire.internal.cache.lru.LRUClockNode;
import com.gemstone.gemfire.internal.cache.lru.NewLRUClockHand;
import com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;
// macros whose definition changes this class:
// disk: DISK
// lru: LRU
// stats: STATS
// versioned: VERSIONED
// One of the following key macros must be defined:
// key object: KEY_OBJECT
// key int: KEY_INT
// key long: KEY_LONG
// key uuid: KEY_UUID
// key string1: KEY_STRING1
// key string2: KEY_STRING2
/**
 * Do not modify this class. It was generated.
 * Instead modify LeafRegionEntry.cpp and then run
 * bin/generateRegionEntryClasses.sh from the directory
 * that contains your build.xml.
 */
public class VMThinLRURegionEntryHeapIntKey extends VMThinLRURegionEntryHeap {
  public VMThinLRURegionEntryHeapIntKey (RegionEntryContext context, int key, Object value
      ) {
    super(context,
          value
        );
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    this.key = key;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VMThinLRURegionEntryHeapIntKey> lastModifiedUpdater
    = AtomicLongFieldUpdater.newUpdater(VMThinLRURegionEntryHeapIntKey.class, "lastModified");
  private volatile Object value;
  protected final Object areGetValue() {
    return this.value;
  }
  protected void areSetValue(Object v) {
    this.value = v;
  }
  protected long getlastModifiedField() {
    return lastModifiedUpdater.get(this);
  }
  protected boolean compareAndSetLastModifiedField(long expectedValue, long newValue) {
    return lastModifiedUpdater.compareAndSet(this, expectedValue, newValue);
  }
  /**
   * @see HashEntry#getEntryHash()
   */
  public final int getEntryHash() {
    return this.hash;
  }
  protected void setEntryHash(int v) {
    this.hash = v;
  }
  /**
   * @see HashEntry#getNextEntry()
   */
  public final HashEntry<Object, Object> getNextEntry() {
    return this.next;
  }
  /**
   * @see HashEntry#setNextEntry
   */
  public final void setNextEntry(final HashEntry<Object, Object> n) {
    this.next = n;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // lru code
  @Override
  public void setDelayedDiskId(LocalRegion r) {
  // nothing needed for LRUs with no disk
  }
  public final synchronized int updateEntrySize(EnableLRU capacityController) {
    return updateEntrySize(capacityController, _getValue()); // OFHEAP: _getValue ok w/o incing refcount because we are synced and only getting the size
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  public final synchronized int updateEntrySize(EnableLRU capacityController,
                                                Object value) {
    int oldSize = getEntrySize();
    int newSize = capacityController.entrySize( getKeyForSizing(), value);
  //   GemFireCacheImpl.getInstance().getLoggerI18n().info("DEBUG updateEntrySize: oldSize=" + oldSize
  //                                               + " newSize=" + newSize);
    setEntrySize(newSize);
    int delta = newSize - oldSize;
  //   if ( debug ) log( "updateEntrySize key=" + getKey()
  //                     + (_getValue() == Token.INVALID ? " invalid" :
  //                        (_getValue() == Token.LOCAL_INVALID ? "local_invalid" :
  //                         (_getValue()==null ? " evicted" : " valid")))
  //                     + " oldSize=" + oldSize
  //                     + " newSize=" + this.size );
    return delta;
  }
  public final boolean testRecentlyUsed() {
    return areAnyBitsSet(RECENTLY_USED);
  }
  @Override
  public final void setRecentlyUsed() {
    setBits(RECENTLY_USED);
  }
  public final void unsetRecentlyUsed() {
    clearBits(~RECENTLY_USED);
  }
  public final boolean testEvicted() {
    return areAnyBitsSet(EVICTED);
  }
  public final void setEvicted() {
    setBits(EVICTED);
  }
  public final void unsetEvicted() {
    clearBits(~EVICTED);
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private LRUClockNode nextLRU;
  private LRUClockNode prevLRU;
  private int size;
  public final void setNextLRUNode( LRUClockNode next ) {
    this.nextLRU = next;
  }
  public final LRUClockNode nextLRUNode() {
    return this.nextLRU;
  }
  public final void setPrevLRUNode( LRUClockNode prev ) {
    this.prevLRU = prev;
  }
  public final LRUClockNode prevLRUNode() {
    return this.prevLRU;
  }
  public final int getEntrySize() {
    return this.size;
  }
  protected final void setEntrySize(int size) {
    this.size = size;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
//@Override
//public StringBuilder appendFieldsToString(final StringBuilder sb) {
//  StringBuilder result = super.appendFieldsToString(sb);
//  result.append("; prev=").append(this.prevLRU==null?"null":"not null");
//  result.append("; next=").append(this.nextLRU==null?"null":"not null");
//  return result;
//}
  @Override
  public Object getKeyForSizing() {
    // inline keys always report null for sizing since the size comes from the entry size
    return null;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // key code
  private final int key;
  @Override
  public final Object getKey() {
    return this.key;
  }
  @Override
  public boolean isKeyEqual(Object k) {
    if (k instanceof Integer) {
      return ((Integer) k).intValue() == this.key;
    }
    return false;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
