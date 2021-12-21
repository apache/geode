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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * This class is used for sorting tombstones by region version number. Because two tombstones with
 * different members are not comparable, the iterator on this class tries to return the tombstones
 * in the order of the timestamps of the tombstones.
 *
 * The class maintains a map, per member, of the tombstones sorted by the version tag.
 *
 * When removing entries, we pick from the sorted map that has the lowest timestamp.
 *
 * This map is not threadsafe.
 *
 *
 */
public class OrderedTombstoneMap<T> {

  /**
   * A map of member id-> sort map of version tag-> region entry
   *
   */
  private final Map<VersionSource, TreeMap<VersionTag, T>> tombstoneMap = new HashMap();

  /**
   * Add a new version tag to map
   */
  public void put(VersionTag tag, T entry) {
    // Add the version tag to the appropriate map
    VersionSource member = tag.getMemberID();
    TreeMap<VersionTag, T> memberMap = tombstoneMap.get(member);
    if (memberMap == null) {
      memberMap = new TreeMap<VersionTag, T>(new VersionTagComparator());
      tombstoneMap.put(member, memberMap);
    }
    T oldValue = memberMap.put(tag, entry);
    Assert.assertTrue(oldValue == null);
  }

  /**
   * Remove a version tag from the map.
   */
  public Map.Entry<VersionTag, T> take() {
    if (tombstoneMap.isEmpty()) {
      // if there are no more entries, return null;
      return null;
    } else {
      // Otherwise, look at all of the members and find the tag with the
      // lowest timestamp.
      long lowestTimestamp = Long.MAX_VALUE;
      TreeMap<VersionTag, T> lowestMap = null;
      for (TreeMap<VersionTag, T> memberMap : tombstoneMap.values()) {
        VersionTag firstTag = memberMap.firstKey();
        long stamp = firstTag.getVersionTimeStamp();
        if (stamp < lowestTimestamp) {
          lowestTimestamp = stamp;
          lowestMap = memberMap;
        }
      }
      if (lowestMap == null) {
        return null;
      }
      // Remove the lowest entry
      Entry<VersionTag, T> result = lowestMap.firstEntry();
      lowestMap.remove(result.getKey());
      if (lowestMap.isEmpty()) {
        // if this is the last entry from a given member,
        // the map for that member
        tombstoneMap.remove(result.getKey().getMemberID());
      }

      return result;
    }
  }


  /**
   * A comparator that sorts version tags based on the region version, and then on the timestamp.
   *
   */
  public static class VersionTagComparator implements Comparator<VersionTag> {

    @Override
    public int compare(VersionTag o1, VersionTag o2) {
      long result = o1.getRegionVersion() - o2.getRegionVersion();
      if (result == 0) {
        result = o1.getVersionTimeStamp() - o2.getVersionTimeStamp();
      }
      return Long.signum(result);
    }

  }
}
