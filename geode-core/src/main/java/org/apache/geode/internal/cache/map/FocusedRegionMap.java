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
package org.apache.geode.internal.cache.map;

import java.util.Map;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.TXEntryState;
import org.apache.geode.internal.cache.eviction.EvictableMap;

public interface FocusedRegionMap extends EvictableMap {

  RegionEntry getEntry(EntryEventImpl event);

  RegionEntryFactory getEntryFactory();

  RegionEntry putEntryIfAbsent(Object key, RegionEntry regionEntry);

  Map<Object, Object> getEntryMap();

  boolean confirmEvictionDestroy(RegionEntry regionEntry); // TODO: subclass

  void lruEntryDestroy(RegionEntry regionEntry); // TODO: subclass

  void removeEntry(Object key, RegionEntry regionEntry, boolean updateStat);

  void removeEntry(Object key, RegionEntry regionEntry, boolean updateStat, EntryEventImpl event,
      final InternalRegion internalRegion);

  void processVersionTag(RegionEntry regionEntry, EntryEventImpl event);

  void lruEntryUpdate(RegionEntry regionEntry);

  void lruEntryCreate(RegionEntry regionEntry);

  void incEntryCount(int delta);

  void runWhileEvictionDisabled(Runnable runnable);

  void txRemoveOldIndexEntry(Operation putOp, RegionEntry regionEntry);

  void processAndGenerateTXVersionTag(EntryEventImpl callbackEvent, RegionEntry regionEntry,
      TXEntryState txEntryState);
}
