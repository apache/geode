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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;

/**
 * This class still uses "synchronized" to protect the
 * underlying HashSet even though all writers do so under
 * the {@link SynchronizedStripedExecutor}. The synchronization on this
 * class can be removed once readers are changed to
 * also use the {@link SynchronizedStripedExecutor}.
 */
public class RedisSet implements Delta, DataSerializable {

  private HashSet<ByteArrayWrapper> members;
  private transient ArrayList<ByteArrayWrapper> deltas;
  // true if deltas contains adds; false if removes
  private transient boolean deltasAreAdds;

  @SuppressWarnings("unchecked")
  RedisSet(Collection<ByteArrayWrapper> members) {
    if (members instanceof HashSet) {
      this.members = (HashSet<ByteArrayWrapper>) members;
    } else {
      this.members = new HashSet<>(members);
    }
  }

  // for serialization
  public RedisSet() {}

  public static long sadd(Region<ByteArrayWrapper, RedisSet> region,
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {

    RedisSet redisSet = region.get(key);

    if (redisSet != null) {
      // update existing value
      return redisSet.doSadd(membersToAdd, region, key);
    } else {
      region.create(key, new RedisSet(membersToAdd));
      return membersToAdd.size();
    }
  }

  public static long srem(Region<ByteArrayWrapper, RedisSet> region,
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToRemove, AtomicBoolean setWasDeleted) {
    RedisSet redisSet = region.get(key);
    if (redisSet == null) {
      return 0L;
    }
    return redisSet.doSrem(membersToRemove, region, key, setWasDeleted);
  }

  public static boolean del(Region<ByteArrayWrapper, RedisSet> region,
      ByteArrayWrapper key) {
    return region.remove(key) != null;
  }

  public static Set<ByteArrayWrapper> members(Region<ByteArrayWrapper, RedisSet> region,
      ByteArrayWrapper key) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.members();
    } else {
      return Collections.emptySet();
    }
  }

  public static int scard(Region<ByteArrayWrapper, RedisSet> region, ByteArrayWrapper key) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.size();
    } else {
      return -1;
    }
  }

  public static boolean sismember(Region<ByteArrayWrapper, RedisSet> region,
                                   ByteArrayWrapper key, ByteArrayWrapper member) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.contains(member);
    } else {
      return false;
    }
  }

  public static Collection<ByteArrayWrapper> srandmember(Region<ByteArrayWrapper, RedisSet> region,
                                                          ByteArrayWrapper key, int count) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.srandmember(count);
    } else {
      return null;
    }
  }

  public static Collection<ByteArrayWrapper> spop(Region<ByteArrayWrapper, RedisSet> region,
                                                   ByteArrayWrapper key, int popCount) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.doSpop(region, key, popCount);
    } else {
      return null;
    }
  }

  private synchronized Collection<ByteArrayWrapper> doSpop(
      Region<ByteArrayWrapper, RedisSet> region, ByteArrayWrapper key, int popCount) {
    int originalSize = size();
    if (originalSize == 0) {
      return null;
    }

    if (popCount >= originalSize) {
      // TODO: need to also cause key to be removed from the metaregion
      region.remove(key, this);
      return this.members;
    }

    ArrayList<ByteArrayWrapper> popped = new ArrayList<>();
    ByteArrayWrapper[] setMembers = members.toArray(new ByteArrayWrapper[originalSize]);
    Random rand = new Random();
    while (popped.size() < popCount) {
      int idx = rand.nextInt(originalSize);
      ByteArrayWrapper memberToPop = setMembers[idx];
      if (memberToPop != null) {
        setMembers[idx] = null;
        popped.add(memberToPop);
        members.remove(memberToPop);
      }
    }
    if (!popped.isEmpty()) {
      this.deltasAreAdds = false;
      this.deltas = popped;
      try {
        region.put(key, this);
      } finally {
        this.deltas = null;
      }
    }
    return popped;
  }

  private synchronized Collection<ByteArrayWrapper> srandmember(int count) {
    int membersSize = members.size();

    if (membersSize <= count && count != 1) {
      return new ArrayList<>(members);
    }

    Random rand = new Random();

    ByteArrayWrapper[] entries = members.toArray(new ByteArrayWrapper[membersSize]);

    if (count == 1) {
      ByteArrayWrapper randEntry = entries[rand.nextInt(entries.length)];
      // Note using ArrayList because Collections.singleton has serialization issues.
      ArrayList<ByteArrayWrapper> result = new ArrayList<>(1);
      result.add(randEntry);
      return result;
    }
    Set<ByteArrayWrapper> result = new HashSet<>();
    // Note that rand.nextInt can return duplicates when "count" is high
    // so we need to use a Set to collect the results.
    while (result.size() < count) {
      ByteArrayWrapper s = entries[rand.nextInt(entries.length)];
      result.add(s);
    }
    return result;
  }

  public synchronized boolean contains(ByteArrayWrapper member) {
    return members.contains(member);
  }

  public synchronized int size() {
    return members.size();
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
  private synchronized long doSadd(ArrayList<ByteArrayWrapper> membersToAdd,
      Region<ByteArrayWrapper, RedisSet> region,
      ByteArrayWrapper key) {

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
  private synchronized long doSrem(ArrayList<ByteArrayWrapper> membersToRemove,
      Region<ByteArrayWrapper, RedisSet> region,
      ByteArrayWrapper key, AtomicBoolean setWasDeleted) {

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
   * The returned set is a copy and will not be changed
   * by future changes to this DeltaSet.
   *
   * @return a set containing all the members in this set
   */
  synchronized Set<ByteArrayWrapper> members() {
    return new HashSet<>(members);
  }
}
