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
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.redis.internal.ByteArrayWrapper;

/**
 * This class still uses "synchronized" to protect the
 * underlying HashSet even though all writers do so under
 * the SynchronizedRunner. The synchronization on this
 * class can be removed once readers are changed to
 * also use the SynchronizedRunner.
 */
public class SetDelta implements Delta, DataSerializable {

  public static void sadd(ResultSender<Long> resultSender,
      Region<ByteArrayWrapper, SetDelta> localRegion, ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {
    resultSender.lastResult(sadd(localRegion, key, membersToAdd));
  }

  public static void srem(ResultSender<Long> resultSender,
      Region<ByteArrayWrapper, SetDelta> localRegion, ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToRemove) {
    AtomicBoolean setWasDeleted = new AtomicBoolean();
    long membersRemoved = srem(localRegion, key, membersToRemove, setWasDeleted);
    resultSender.sendResult(membersRemoved);
    resultSender.lastResult(setWasDeleted.get() ? 1L : 0L);
  }

  public static void del(ResultSender<Boolean> resultSender,
      Region<ByteArrayWrapper, SetDelta> localRegion, ByteArrayWrapper key) {
    resultSender.lastResult(del(localRegion, key));
  }

  public static void smembers(ResultSender<Set<ByteArrayWrapper>> resultSender,
      Region<ByteArrayWrapper, SetDelta> localRegion, ByteArrayWrapper key) {
    resultSender.lastResult(members(localRegion, key));
  }

  private HashSet<ByteArrayWrapper> members;
  private transient ArrayList<ByteArrayWrapper> deltas;
  // false if deltas contain only removes @todo: only removes? only adds? clarify...
  private transient boolean deltasContainAdds;

  @SuppressWarnings("unchecked")
  SetDelta(Collection<ByteArrayWrapper> members) {
    if (members instanceof HashSet) {
      this.members = (HashSet<ByteArrayWrapper>) members;
    } else {
      this.members = new HashSet<>(members);
    }
  }

  // for serialization
  public SetDelta() {}

  public static long sadd(Region<ByteArrayWrapper, SetDelta> region,
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {

    SetDelta setDelta = region.get(key); // is this weird that it contains and instance of itself?

    if (setDelta != null) {
      // update existing value
      return setDelta.performSaddInGeode(membersToAdd, region, key);
    } else {
      region.create(key, new SetDelta(membersToAdd));
      return membersToAdd.size();
    }
  }

  public static long srem(Region<ByteArrayWrapper, SetDelta> region,
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToRemove, AtomicBoolean setWasDeleted) {
    SetDelta setDelta = region.get(key);
    if (setDelta == null) {
      return 0L;
    }
    return setDelta.performSremInGeode(membersToRemove, region, key, setWasDeleted);
  }

  public static boolean del(Region<ByteArrayWrapper, SetDelta> region,
      ByteArrayWrapper key) {
    return region.remove(key) != null;
  }

  public static Set<ByteArrayWrapper> members(Region<ByteArrayWrapper, SetDelta> region,
      ByteArrayWrapper key) {
    SetDelta setDelta = region.get(key);
    if (setDelta != null) {
      return setDelta.members();
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

  // DELTA
  @Override
  public boolean hasDelta() {
    return deltas != null;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    DataSerializer.writeBoolean(deltasContainAdds, out);
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
  private synchronized long performSaddInGeode(ArrayList<ByteArrayWrapper> membersToAdd,
      Region<ByteArrayWrapper, SetDelta> region,
      ByteArrayWrapper key) {

    membersToAdd.removeIf(memberToAdd -> !members.add(memberToAdd));
    int membersAdded = membersToAdd.size();
    if (membersAdded != 0) {
      deltasContainAdds = true;
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
  private synchronized long performSremInGeode(ArrayList<ByteArrayWrapper> membersToRemove,
      Region<ByteArrayWrapper, SetDelta> region,
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
        deltasContainAdds = false;
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
