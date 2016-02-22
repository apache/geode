/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.region.entry;

import com.gemstone.gemfire.internal.cache.*;

public class RegionEntryFactoryBuilder {
  public RegionEntryFactory getRegionEntryFactoryOrNull(boolean statsEnabled, boolean isLRU, boolean isDisk, boolean withVersioning, boolean offHeap) {
    int bitRepresentation = 0;
    bitRepresentation |= statsEnabled ? 1 : 0;
    bitRepresentation |= isLRU ? 2 : 0;
    bitRepresentation |= isDisk ? 4 : 0;
    bitRepresentation |= withVersioning ? 8 : 0;
    bitRepresentation |= offHeap ? 16 : 0;

    /**
     * The bits represent all options
     * |offHeap|versioning|disk|lru|stats|
     */
    switch (bitRepresentation) {
    case (0):
      return VMThinRegionEntryHeap.getEntryFactory(); // Bits: 00000
    case (1):
      return VMStatsRegionEntryHeap.getEntryFactory(); // Bits: 00001
    case (2):
      return VMThinLRURegionEntryHeap.getEntryFactory(); // Bits: 00010
    case (3):
      return VMStatsLRURegionEntryHeap.getEntryFactory(); // Bits: 00011
    case (4):
      return VMThinDiskRegionEntryHeap.getEntryFactory(); // Bits: 00100
    case (5):
      return VMStatsDiskRegionEntryHeap.getEntryFactory(); // Bits: 00101
    case (6):
      return VMThinDiskLRURegionEntryHeap.getEntryFactory(); // Bits: 00110
    case (7):
      return VMStatsDiskLRURegionEntryHeap.getEntryFactory(); // Bits: 00111
    case (8):
      return VersionedThinRegionEntryHeap.getEntryFactory(); // Bits: 01000
    case (9):
      return VersionedStatsRegionEntryHeap.getEntryFactory(); // Bits: 01001
    case (10):
      return VersionedThinLRURegionEntryHeap.getEntryFactory(); // Bits: 01010
    case (11):
      return VersionedStatsLRURegionEntryHeap.getEntryFactory(); // Bits: 01011
    case (12):
      return VersionedThinDiskRegionEntryHeap.getEntryFactory(); // Bits: 01100
    case (13):
      return VersionedStatsDiskRegionEntryHeap.getEntryFactory(); // Bits: 01101
    case (14):
      return VersionedThinDiskLRURegionEntryHeap.getEntryFactory(); // Bits: 01110
    case (15):
      return VersionedStatsDiskLRURegionEntryHeap.getEntryFactory(); // Bits: 01111
    case (16):
      return VMThinRegionEntryOffHeap.getEntryFactory(); // Bits: 10000
    case (17):
      return VMStatsRegionEntryOffHeap.getEntryFactory(); // Bits: 10001
    case (18):
      return VMThinLRURegionEntryOffHeap.getEntryFactory(); // Bits: 10010
    case (19):
      return VMStatsLRURegionEntryOffHeap.getEntryFactory(); // Bits: 10011
    case (20):
      return VMThinDiskRegionEntryOffHeap.getEntryFactory(); // Bits: 10100
    case (21):
      return VMStatsDiskRegionEntryOffHeap.getEntryFactory(); // Bits: 10101
    case (22):
      return VMThinDiskLRURegionEntryOffHeap.getEntryFactory(); // Bits: 10110
    case (23):
      return VMStatsDiskLRURegionEntryOffHeap.getEntryFactory(); // Bits: 10111
    case (24):
      return VersionedThinRegionEntryOffHeap.getEntryFactory(); // Bits: 11000
    case (25):
      return VersionedStatsRegionEntryOffHeap.getEntryFactory(); // Bits: 11001
    case (26):
      return VersionedThinLRURegionEntryOffHeap.getEntryFactory(); // Bits: 11010
    case (27):
      return VersionedStatsLRURegionEntryOffHeap.getEntryFactory(); // Bits: 11011
    case (28):
      return VersionedThinDiskRegionEntryOffHeap.getEntryFactory(); // Bits: 11100
    case (29):
      return VersionedStatsDiskRegionEntryOffHeap.getEntryFactory(); // Bits: 11101
    case (30):
      return VersionedThinDiskLRURegionEntryOffHeap.getEntryFactory(); // Bits: 11110
    case (31):
      return VersionedStatsDiskLRURegionEntryOffHeap.getEntryFactory(); // Bits: 11111
    default:
      return null;
    }
  }
}
