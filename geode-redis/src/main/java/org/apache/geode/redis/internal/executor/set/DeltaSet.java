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
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;

/**
 * TODO: it is probably a bad idea for this class to implement Set.
 * We want to be careful how other code interacts with these instances
 * to make sure that no modifications are made that are not thread safe
 * and that will always be stored in the region.
 * Currently the only "correct" methods on this class are:
 * members, delete, customAddAll, customRemoveAll, and the
 * serialization methods.
 */
class DeltaSet implements Set<ByteArrayWrapper>, Delta, DataSerializable {

  public static long sadd(Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key,
      Collection<ByteArrayWrapper> membersToAdd) {
    while (true) {
      DeltaSet deltaSet = region.get(key);
      if (deltaSet == null) {
        // create new set
        if (region.putIfAbsent(key, new DeltaSet(membersToAdd)) == null) {
          return membersToAdd.size();
        } else {
          // retry since another thread concurrently changed the region
        }
      } else {
        // update existing value
        try {
          return deltaSet.saddInstance(membersToAdd, region, key);
        } catch (DeltaSet.RetryDueToConcurrentModification ex) {
          // retry since another thread concurrently changed the region
        }
      }
    }
  }

  public static long srem(Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key,
      Collection<ByteArrayWrapper> membersToRemove) {
    while (true) {
      DeltaSet deltaSet = region.get(key);
      if (deltaSet == null) {
        return 0L;
      }
      try {
        return deltaSet.sremInstance(membersToRemove, region, key);
      } catch (DeltaSet.RetryDueToConcurrentModification ex) {
        // retry since another thread concurrently changed the region
      }
    }
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

  // SET INTERFACE
  @Override
  public int size() {
    return members.size();
  }

  @Override
  public boolean isEmpty() {
    return members.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return members.contains(o);
  }

  @Override
  public Iterator<ByteArrayWrapper> iterator() {
    return members.iterator();
  }

  @Override
  public Object[] toArray() {
    return members.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return members.toArray(a);
  }

  @Override
  public boolean add(ByteArrayWrapper byteArrayWrapper) {
    return members.add(byteArrayWrapper);
  }

  @Override
  public boolean remove(Object o) {
    return members.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return members.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends ByteArrayWrapper> c) {
    return members.addAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return members.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return members.removeAll(c);
  }

  @Override
  public void clear() {
    members.clear();
  }

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
  public void fromDelta(DataInput in)
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
   * @param membersToAdd members to add to this set; NOTE must be an ArrayList and it may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @return the number of members actually added
   * @throws RetryDueToConcurrentModification if a concurrent modification is detected
   */
  synchronized long saddInstance(Collection<ByteArrayWrapper> membersToAdd,
      Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key) {
    if (region.get(key) != this) {
      throw new RetryDueToConcurrentModification();
    }
    membersToAdd.removeIf(memberToAdd -> !members.add(memberToAdd));
    int membersAdded = membersToAdd.size();
    if (membersAdded != 0) {
      deltasAreAdds = true;
      deltas = (ArrayList<ByteArrayWrapper>) membersToAdd;
      try {
        region.put(key, this);
      } finally {
        deltas = null;
      }
    }
    return membersAdded;
  }

  /**
   * @param membersToRemove members to remove from this set; NOTE must be an ArrayList and it may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to remove from
   * @return the number of members actually removed
   * @throws RetryDueToConcurrentModification if a concurrent modification is detected
   */
  private synchronized long sremInstance(Collection<ByteArrayWrapper> membersToRemove,
      Region<ByteArrayWrapper, DeltaSet> region,
      ByteArrayWrapper key) {
    if (region.get(key) != this) {
      throw new RetryDueToConcurrentModification();
    }
    membersToRemove.removeIf(memberToRemove -> !members.remove(memberToRemove));
    int membersRemoved = membersToRemove.size();
    if (membersRemoved != 0) {
      deltasAreAdds = false;
      deltas = (ArrayList<ByteArrayWrapper>) membersToRemove;
      try {
        region.put(key, this);
      } finally {
        deltas = null;
      }
    }
    return membersRemoved;
  }

  /**
   * This exception is thrown if a modification fails because some other
   * thread changed what is stored in the region.
   */
  static class RetryDueToConcurrentModification extends RuntimeException {
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
  private synchronized Set<ByteArrayWrapper> members() {
    return new HashSet<>(members);
  }
}
