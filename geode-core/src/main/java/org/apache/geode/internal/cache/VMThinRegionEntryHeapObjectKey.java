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
package org.apache.geode.internal.cache;

// DO NOT modify this class. It was generated from LeafRegionEntry.cpp
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;

// macros whose definition changes this class:
// disk: DISK
// lru: LRU
// stats: STATS
// versioned: VERSIONED
// offheap: OFFHEAP
// One of the following key macros must be defined:
// key object: KEY_OBJECT
// key int: KEY_INT
// key long: KEY_LONG
// key uuid: KEY_UUID
// key string1: KEY_STRING1
// key string2: KEY_STRING2
/**
 * Do not modify this class. It was generated. Instead modify LeafRegionEntry.cpp and then run
 * bin/generateRegionEntryClasses.sh from the directory that contains your build.xml.
 */
public class VMThinRegionEntryHeapObjectKey extends VMThinRegionEntryHeap {
  public VMThinRegionEntryHeapObjectKey(RegionEntryContext context, Object key, Object value) {
    super(context, value);
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    this.key = key;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VMThinRegionEntryHeapObjectKey> lastModifiedUpdater =
      AtomicLongFieldUpdater.newUpdater(VMThinRegionEntryHeapObjectKey.class, "lastModified");
  private volatile Object value;

  @Override
  protected final Object getValueField() {
    return this.value;
  }

  @Override
  protected void setValueField(Object v) {
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
  // key code
  private final Object key;

  @Override
  public final Object getKey() {
    return this.key;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
