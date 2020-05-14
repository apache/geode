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

import static java.util.Collections.emptyList;
import static org.apache.geode.redis.internal.RedisDataType.REDIS_SET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisDataType;

/**
 * This class still uses "synchronized" to protect the
 * underlying HashSet even though all writers do so under
 * the {@link SynchronizedStripedExecutor}. The synchronization on this
 * class can be removed once readers are changed to
 * also use the {@link SynchronizedStripedExecutor}.
 */
public class RedisSet implements RedisData {

  public static transient RedisSet EMPTY = new EmptyRedisSet();
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

  synchronized List<Object> sscan(Pattern matchPattern, int count, int cursor) {

    List<Object> returnList = new ArrayList<>();
    int size = members.size();
    int beforeCursor = 0;
    int numElements = 0;
    int i = -1;
    for (ByteArrayWrapper value : members) {
      i++;
      if (beforeCursor < cursor) {
        beforeCursor++;
        continue;
      } else if (numElements < count) {
        if (matchPattern != null) {
          String valueAsString = Coder.bytesToString(value.toBytes());
          if (matchPattern.matcher(valueAsString).matches()) {
            returnList.add(value);
            numElements++;
          }
        } else {
          returnList.add(value);
          numElements++;
        }
      } else {
        break;
      }
    }

    if (i == size - 1) {
      returnList.add(0, String.valueOf(0));
    } else {
      returnList.add(0, String.valueOf(i));
    }
    return returnList;
  }

  synchronized Collection<ByteArrayWrapper> spop(
      Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key, int popCount) {
    int originalSize = scard();
    if (originalSize == 0) {
      return emptyList();
    }

    if (popCount >= originalSize) {
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

  synchronized Collection<ByteArrayWrapper> srandmember(int count) {
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

  public synchronized boolean sismember(ByteArrayWrapper member) {
    return members.contains(member);
  }

  public synchronized int scard() {
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
   * @return the number of members actually added
   */
  synchronized long sadd(ArrayList<ByteArrayWrapper> membersToAdd,
      Region<ByteArrayWrapper, RedisData> region,
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
   * @return the number of members actually removed
   */
  synchronized long srem(ArrayList<ByteArrayWrapper> membersToRemove,
      Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key) {

    membersToRemove.removeIf(memberToRemove -> !members.remove(memberToRemove));
    int membersRemoved = membersToRemove.size();
    if (membersRemoved != 0) {
      if (members.isEmpty()) {
        region.remove(key);
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
  synchronized Set<ByteArrayWrapper> smembers() {
    return new HashSet<>(members);
  }

  @Override
  public RedisDataType getType() {
    return REDIS_SET;
  }
}
