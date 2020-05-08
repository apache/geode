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
 *
 */

package org.apache.geode.redis.internal.executor.hash;

import static java.util.Collections.emptyList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;

public class RedisHash implements Delta, DataSerializable {
  private HashMap<ByteArrayWrapper, ByteArrayWrapper> hash;
  /**
   * When deltas are adds it will always contain an even number of field/value pairs.
   * When deltas are removes it will just contain field names.
   */
  private transient ArrayList<ByteArrayWrapper> deltas;
  // true if deltas contains adds; false if removes
  private transient boolean deltasAreAdds;


  public static boolean del(Region<ByteArrayWrapper, RedisHash> region, ByteArrayWrapper key) {
    return region.remove(key) != null;
  }

  public static int hset(Region<ByteArrayWrapper, RedisHash> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToSet, boolean nx) {
    RedisHash hash = region.get(key);
    if (hash != null) {
      return hash.doHset(region, key, fieldsToSet, nx);
    } else {
      region.put(key, new RedisHash(fieldsToSet));
      return fieldsToSet.size() / 2;
    }
  }

  public static int hdel(Region<ByteArrayWrapper, RedisHash> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToRemove) {
    RedisHash hash = region.get(key);
    if (hash != null) {
      return hash.doHdel(region, key, fieldsToRemove);
    } else {
      return 0;
    }
  }

  public static Collection<Map.Entry<ByteArrayWrapper, ByteArrayWrapper>> hgetall(
      Region<ByteArrayWrapper, RedisHash> region, ByteArrayWrapper key) {
    RedisHash hash = region.get(key);
    if (hash != null) {
      return hash.doHgetall();
    } else {
      return emptyList();
    }
  }

  public RedisHash(List<ByteArrayWrapper> fieldsToSet) {
    hash = new HashMap<>();
    Iterator<ByteArrayWrapper> iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      hash.put(iterator.next(), iterator.next());
    }
  }

  public RedisHash() {
    // for serialization
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashMap(hash, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    hash = DataSerializer.readHashMap(in);
  }

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
  public synchronized void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    boolean deltaAdds = DataSerializer.readBoolean(in);
    try {
      ArrayList<ByteArrayWrapper> deltas = DataSerializer.readArrayList(in);
      if (deltas != null) {
        Iterator<ByteArrayWrapper> iterator = deltas.iterator();
        while (iterator.hasNext()) {
          ByteArrayWrapper field = iterator.next();
          if (deltaAdds) {
            ByteArrayWrapper value = iterator.next();
            hash.put(field, value);
          } else {
            hash.remove(field);
          }
        }
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private synchronized int doHset(Region<ByteArrayWrapper, RedisHash> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToSet, boolean nx) {
    int fieldsAdded = 0;
    Iterator<ByteArrayWrapper> iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      ByteArrayWrapper field = iterator.next();
      ByteArrayWrapper value = iterator.next();
      boolean added;
      if (nx) {
        added = hash.putIfAbsent(field, value) == null;
      } else {
        added = hash.put(field, value) == null;
      }
      if (added) {
        if (deltas == null) {
          deltas = new ArrayList<>();
        }
        deltas.add(field);
        deltas.add(value);
        fieldsAdded++;
      }
    }
    storeChanges(region, key, true);
    return fieldsAdded;
  }

  private synchronized int doHdel(Region<ByteArrayWrapper, RedisHash> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToRemove) {
    int fieldsRemoved = 0;
    for (ByteArrayWrapper fieldToRemove : fieldsToRemove) {
      if (hash.remove(fieldToRemove) != null) {
        if (deltas == null) {
          deltas = new ArrayList<>();
        }
        deltas.add(fieldToRemove);
        fieldsRemoved++;
      }
    }
    storeChanges(region, key, false);
    return fieldsRemoved;
  }

  private synchronized Collection<Map.Entry<ByteArrayWrapper, ByteArrayWrapper>> doHgetall() {
    return new ArrayList<>(hash.entrySet());
  }

  private void storeChanges(Region<ByteArrayWrapper, RedisHash> region, ByteArrayWrapper key,
      boolean doingAdds) {
    if (hasDelta()) {
      if (!doingAdds && hash.isEmpty()) {
        region.remove(key);
      } else {
        deltasAreAdds = doingAdds;
        try {
          region.put(key, this);
        } finally {
          deltas = null;
        }
      }
    }
  }

  // the following are needed because not all the hash commands have been converted to functions.

  public synchronized boolean isEmpty() {
    return hash.isEmpty();
  }

  public synchronized Collection<Map.Entry<ByteArrayWrapper, ByteArrayWrapper>> entries() {
    return doHgetall();
  }

  public synchronized ByteArrayWrapper get(ByteArrayWrapper field) {
    return hash.get(field);
  }

  public synchronized void put(ByteArrayWrapper field, ByteArrayWrapper value) {
    hash.put(field, value);
  }

  public synchronized List<ByteArrayWrapper> keys() {
    return new ArrayList<>(hash.keySet());
  }

  public synchronized int size() {
    return hash.size();
  }

  public synchronized Collection<ByteArrayWrapper> values() {
    return new ArrayList<>(hash.values());
  }

  public synchronized boolean containsKey(ByteArrayWrapper field) {
    return hash.containsKey(field);
  }
}
