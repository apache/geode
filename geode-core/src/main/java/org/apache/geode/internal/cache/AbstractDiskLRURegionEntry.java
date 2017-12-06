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


import org.apache.geode.internal.cache.entries.AbstractOplogDiskRegionEntry;
import org.apache.geode.internal.cache.lru.LRUEntry;

/**
 * Abstract implementation class of RegionEntry interface. This adds LRU support behavior to entries
 * that already have Disk support.
 *
 * @since GemFire 3.5.1
 *
 *
 */
public abstract class AbstractDiskLRURegionEntry extends AbstractOplogDiskRegionEntry
    implements LRUEntry {
  protected AbstractDiskLRURegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }

  // Do not add any instance variables to this class.
  // Instead add them to the LRU section of LeafRegionEntry.cpp.
}
