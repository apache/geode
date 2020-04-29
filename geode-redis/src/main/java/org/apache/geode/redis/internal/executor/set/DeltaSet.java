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

package org.apache.geode.redis.internal.executor.set;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;

public class DeltaSet implements Delta, DataSerializable {

  public static long sadd(Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {
    long result = -1;
    do {
      DeltaSet deltaSet = region.get(key);
      if (deltaSet != null) {
        // update existing value
        result = deltaSet.saddInstance(membersToAdd, region, key);
      } else {
        // create new set
        if (region.putIfAbsent(key, new DeltaSet(membersToAdd)) == null) {
          return membersToAdd.size();
        } else {
          // retry since another thread concurrently changed the region
        }
      }
    } while (result == -1);
    return result;
  }

  public static long srem(Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToRemove, AtomicBoolean setWasDeleted) {
    long result;
    do {
      DeltaSet deltaSet = region.get(key);
      if (deltaSet == null) {
        return 0L;
      }
      result = deltaSet.sremInstance(membersToRemove, region, key, setWasDeleted);
    } while (result == -1);
    return result;
  }

  public static boolean del(Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key) {
    while (true) {
      DeltaSet deltaSet = region.get(key);
      if (deltaSet == null) {
        return false;
      }
      if (deltaSet.delInstance(region, key)) {
        return true;
      } else {
        // retry since another thread concurrently changed the region
      }
    }
  }

  public static Set<ByteArrayWrapper> members(Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key) {
    DeltaSet deltaSet = region.get(key);
    if (deltaSet != null) {
      return deltaSet.members();
    } else {
      return Collections.emptySet();
    }
  }

  public synchronized boolean contains(ByteArrayWrapper member) {
    return members.contains(member);
  }

  public synchronized int size() {
    return members.size();
  }

  private HashSet<ByteArrayWrapper> members;
  private transient ArrayList<ByteArrayWrapper> deltas;
  // true if deltas contains adds; false if removes
  private transient boolean deltasAreAdds;

  DeltaSet(Collection<ByteArrayWrapper> members) {
    if (members instanceof HashSet) {
      this.members = (HashSet<ByteArrayWrapper>) members;
    } else {
      this.members = new HashSet<>(members);
    }
  }

  // for serialization
  public DeltaSet() {}


  // DELTA
  @Override
  public boolean hasDelta() {
    return deltas != null;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    DataSerializer.writeBoolean(deltasAreAdds, out);
    DataSerializer.writeArrayList(deltas, out);
  }

  @Override
  public synchronized void fromDelta(DataInput in)
      throws IOException, InvalidDeltaException {
    boolean deltaAdds = DataSerializer.readBoolean(in);
    try {
      ArrayList<ByteArrayWrapper> deltas = DataSerializer.readArrayList(in);
      if (deltas != null) {
        if (deltaAdds) {
          members.addAll(deltas);
        } else {
          members.removeAll(deltas);
        }
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  // DATA SERIALIZABLE

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashSet(members, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    members = DataSerializer.readHashSet(in);
  }


  /**
   * @param membersToAdd members to add to this set; NOTE this list may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @return the number of members actually added; -1 if concurrent modification
   */
  private synchronized long saddInstance(ArrayList<ByteArrayWrapper> membersToAdd,
      Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key) {
    if (region.get(key) != this) {
      return -1;
    }
    membersToAdd.removeIf(memberToAdd -> !members.add(memberToAdd));
    int membersAdded = membersToAdd.size();
    if (membersAdded != 0) {
      deltasAreAdds = true;
      deltas = membersToAdd;
      try {
        region.put(key, this);
      } finally {
        deltas = null;
      }
    }
    return membersAdded;
  }

  /**
   * @param membersToRemove members to remove from this set; NOTE this list may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to remove from
   * @param setWasDeleted set to true if this method deletes the set
   * @return the number of members actually removed; -1 if concurrent modification
   */
  private synchronized long sremInstance(ArrayList<ByteArrayWrapper> membersToRemove,
      Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key, AtomicBoolean setWasDeleted) {
    if (region.get(key) != this) {
      return -1;
    }
    membersToRemove.removeIf(memberToRemove -> !members.remove(memberToRemove));
    int membersRemoved = membersToRemove.size();
    if (membersRemoved != 0) {
      if (members.isEmpty()) {
        region.remove(key);
        if (setWasDeleted != null) {
          setWasDeleted.set(true);
        }
      } else {
        deltasAreAdds = false;
        deltas = membersToRemove;
        try {
          region.put(key, this);
        } finally {
          deltas = null;
        }
      }
    }
    return membersRemoved;
  }

  /**
   *
   * @param region the region the set is stored in
   * @param key the name of the set to delete
   * @return true if set deleted; false if not found
   */
  private synchronized boolean delInstance(
      Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key) {
    return region.remove(key, this);
  }

  /**
   * The returned set is a copy and will not be changed
   * by future changes to this DeltaSet.
   *
   * @return a set containing all the members in this set
   */
  synchronized Set<ByteArrayWrapper> members() {
    return new HashSet<>(members);
  }
}
