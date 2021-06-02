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

package org.apache.geode.redis.internal.data;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SORTED_SET;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.collections.OrderStatisticsSet;
import org.apache.geode.redis.internal.collections.OrderStatisticsTree;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.executor.sortedset.ZAddOptions;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisSortedSet extends AbstractRedisData {
  private Object2ObjectOpenCustomHashMap<byte[], byte[]> members;
  private OrderStatisticsSet<OrderedSetEntry> scoreSet;

  protected static final int BASE_REDIS_SORTED_SET_OVERHEAD = 184;
  protected static final int PER_PAIR_OVERHEAD = 48;

  private int sizeInBytes = BASE_REDIS_SORTED_SET_OVERHEAD;

  @Override
  public int getSizeInBytes() {
    return sizeInBytes;
  }

  int calculateSizeOfFieldValuePair(byte[] member, byte[] score) {
    return PER_PAIR_OVERHEAD + member.length + score.length;
  }

  RedisSortedSet(List<byte[]> members) {
    this.members =
        new Object2ObjectOpenCustomHashMap<>(members.size() / 2, ByteArrays.HASH_STRATEGY);
    scoreSet = new OrderStatisticsTree<>();

    Iterator<byte[]> iterator = members.iterator();

    while (iterator.hasNext()) {
      byte[] score = iterator.next();
      score = Coder.doubleToBytes(processByteArrayAsDouble(score));
      byte[] member = iterator.next();
      memberAdd(member, score);
    }
  }

  protected int getSortedSetSize() {
    return members.size();
  }

  // for serialization
  public RedisSortedSet() {}

  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
    if (deltaInfo instanceof AddsDeltaInfo) {
      AddsDeltaInfo addsDeltaInfo = (AddsDeltaInfo) deltaInfo;
      membersAddAll(addsDeltaInfo);
    } else {
      RemsDeltaInfo remsDeltaInfo = (RemsDeltaInfo) deltaInfo;
      membersRemoveAll(remsDeltaInfo);
    }
  }

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads are modifying this
   * object, the striped executor will not protect toData. So any methods that modify "members"
   * needs to be thread safe with toData.
   */

  @Override
  public synchronized void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    InternalDataSerializer.writePrimitiveInt(members.size(), out);
    for (Map.Entry<byte[], byte[]> entry : members.entrySet()) {
      byte[] member = entry.getKey();
      byte[] score = entry.getValue();
      InternalDataSerializer.writeByteArray(member, out);
      InternalDataSerializer.writeByteArray(score, out);
    }
    InternalDataSerializer.writePrimitiveInt(sizeInBytes, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int size = InternalDataSerializer.readPrimitiveInt(in);
    members = new Object2ObjectOpenCustomHashMap<>(size, ByteArrays.HASH_STRATEGY);
    scoreSet = new OrderStatisticsTree<>();
    for (int i = 0; i < size; i++) {
      byte[] member = InternalDataSerializer.readByteArray(in);
      byte[] score = InternalDataSerializer.readByteArray(in);
      members.put(member, score);
      scoreSet.add(new OrderedSetEntry(member, score));
    }
    sizeInBytes = InternalDataSerializer.readPrimitiveInt(in);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisSortedSet)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RedisSortedSet redisSortedSet = (RedisSortedSet) o;
    if (members.size() != redisSortedSet.members.size()) {
      return false;
    }

    for (Map.Entry<byte[], byte[]> entry : members.entrySet()) {
      if (!Arrays.equals(redisSortedSet.members.get(entry.getKey()), (entry.getValue()))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int getDSFID() {
    return REDIS_SORTED_SET_ID;
  }

  protected synchronized void memberAdd(byte[] memberToAdd, byte[] scoreToAdd) {
    byte[] oldScore = members.put(memberToAdd, scoreToAdd);
    scoreSet.add(new OrderedSetEntry(memberToAdd, scoreToAdd));
    if (oldScore == null) {
      sizeInBytes += calculateSizeOfFieldValuePair(memberToAdd, scoreToAdd);
    } else {
      sizeInBytes += scoreToAdd.length - oldScore.length;
    }
  }

  private synchronized void membersAddAll(AddsDeltaInfo addsDeltaInfo) {
    Iterator<byte[]> iterator = addsDeltaInfo.getAdds().iterator();
    while (iterator.hasNext()) {
      byte[] member = iterator.next();
      byte[] score = iterator.next();
      memberAdd(member, score);
    }
  }

  private synchronized void membersRemoveAll(RemsDeltaInfo remsDeltaInfo) {
    for (byte[] member : remsDeltaInfo.getRemoves()) {
      memberRemove(member);
    }
  }

  /**
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @param membersToAdd members to add to this set; NOTE this list may by modified by this call
   * @return the number of members actually added OR incremented value if INCR option specified
   */
  Object zadd(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToAdd,
      ZAddOptions options) {
    if (options.isINCR()) {
      return zaddIncr(region, key, membersToAdd, options);
    }
    AddsDeltaInfo deltaInfo = null;
    Iterator<byte[]> iterator = membersToAdd.iterator();
    int initialSize = getSortedSetSize();
    int changesCount = 0;
    while (iterator.hasNext()) {
      byte[] score =
          Coder.doubleToBytes(processByteArrayAsDouble(iterator.next()));
      byte[] member = iterator.next();
      if (options.isNX() && members.containsKey(member)) {
        continue;
      }
      if (options.isXX() && !members.containsKey(member)) {
        continue;
      }
      byte[] oldScore = memberAdd(member, score);
      if (options.isCH() && oldScore != null && !Arrays.equals(oldScore, score)) {
        changesCount++;
      }

      if (deltaInfo == null) {
        deltaInfo = new AddsDeltaInfo(new ArrayList<>());
      }
      deltaInfo.add(member);
      deltaInfo.add(score);
    }

    storeChanges(region, key, deltaInfo);
    return getSortedSetSize() - initialSize + changesCount;
  }

  private void validateScoreIsDouble(byte[] score) {
    String scoreString = Coder.bytesToString(score).toLowerCase();
    switch (scoreString) {
      case "inf":
      case "infinity":
      case "+inf":
      case "+infinity":
      case "-inf":
      case "-infinity":
        return;
      default:
        if (!Coder.bytesToString(score).matches("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$")) {
          throw new NumberFormatException(ERROR_NOT_A_VALID_FLOAT);
        }
    }
  }

  private byte[] zaddIncr(Region<RedisKey, RedisData> region, RedisKey key,
      List<byte[]> membersToAdd, ZAddOptions options) {
    // for zadd incr option, only one incrementing element pair is allowed to get here.
    byte[] increment = membersToAdd.get(0);
    byte[] member = membersToAdd.get(1);
    if (options.isNX() && members.containsKey(member)) {
      return null;
    }
    if (options.isXX() && !members.containsKey(member)) {
      return null;
    }
    return zincrby(region, key, increment, member);
  }

  byte[] zscore(byte[] member) {
    return members.get(member);
  }

  byte[] zincrby(Region<RedisKey, RedisData> region, RedisKey key, byte[] increment,
      byte[] member) {
    byte[] byteScore = members.get(member);
    double incr = processByteArrayAsDouble(increment);

    if (byteScore != null) {
      incr += Coder.bytesToDouble(byteScore);

      if (Double.isNaN(incr)) {
        throw new NumberFormatException(RedisConstants.ERROR_OPERATION_PRODUCED_NAN);
      }
    }

    byte[] byteIncr = Coder.doubleToBytes(incr);
    memberAdd(member, byteIncr);

    AddsDeltaInfo deltaInfo = new AddsDeltaInfo(new ArrayList<>());
    deltaInfo.add(member);
    deltaInfo.add(byteIncr);

    storeChanges(region, key, deltaInfo);

    return byteIncr;
  }

  int zrank(byte[] member) {
    byte[] score = members.get(member);
    if (score == null) {
      return -1;
    }
    return scoreSet.indexOf(new OrderedSetEntry(member, score));
  }

  long zrem(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToRemove) {
    int membersRemoved = 0;
    RemsDeltaInfo deltaInfo = null;
    for (byte[] memberToRemove : membersToRemove) {
      if (memberRemove(memberToRemove) != null) {
        if (deltaInfo == null) {
          deltaInfo = new RemsDeltaInfo();
        }
        deltaInfo.add(memberToRemove);
        membersRemoved++;
      }
    }
    storeChanges(region, key, deltaInfo);
    return membersRemoved;
  }

  synchronized byte[] memberRemove(byte[] member) {
    byte[] oldValue = members.remove(member);
    if (oldValue != null) {
      sizeInBytes -= calculateSizeOfFieldValuePair(member, oldValue);
    }

    if (members.isEmpty()) {
      sizeInBytes = BASE_REDIS_SORTED_SET_OVERHEAD;
    }

    return oldValue;
  }

  long zcard() {
    return members.size();
  }

  @Override
  public RedisDataType getType() {
    return REDIS_SORTED_SET;
  }

  @Override
  protected boolean removeFromRegion() {
    return members.isEmpty();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), members);
  }

  @Override
  public String toString() {
    return "RedisSortedSet{" + super.toString() + ", " + "size=" + members.size() + '}';
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  private double processByteArrayAsDouble(byte[] value) {
    String stringValue = bytesToString(value).toLowerCase();
    double processedDouble;
    switch (stringValue) {
      case "inf":
      case "+inf":
      case "infinity":
      case "+infinity":
        processedDouble = POSITIVE_INFINITY;
        break;
      case "-inf":
      case "-infinity":
        processedDouble = NEGATIVE_INFINITY;
        break;
      default:
        try {
          processedDouble = Double.parseDouble(stringValue);
        } catch (NumberFormatException nfe) {
          throw new NumberFormatException(ERROR_NOT_A_VALID_FLOAT);
        }
        break;
    }
    return processedDouble;
  }

  static class OrderedSetEntry implements Comparable {
    public byte[] member;
    public Double score;

    @Override
    public int compareTo(Object o) {
      int comparison = score.compareTo(((OrderedSetEntry) o).score);
      if (comparison == 0) {
        return javaImplementationOfAnsiCMemCmp((OrderedSetEntry) o);
      }
      return comparison;
    }

    private int javaImplementationOfAnsiCMemCmp(OrderedSetEntry o) {
      // Scores equal, try lexical ordering
      byte[] otherMember = o.member;
      int shortestLength = Math.min(member.length, otherMember.length);
      for (int i = 0; i < shortestLength; i++) {
        int localComp = Byte.toUnsignedInt(member[i]) - Byte.toUnsignedInt(otherMember[i]);
        if (localComp != 0) {
          return localComp;
        }
      }
      // shorter array whose items are all equal to the first items of a longer array is
      // considered 'less than'
      if (member.length < otherMember.length) {
        return -1; // member < other
      } else if (member.length > otherMember.length) {
        return 1; // other < member
      }
      return 0; // totally equal - should never happen, because you can't have two members
      // with same name...
    }

    OrderedSetEntry(byte[] member, byte[] score) {
      this.member = member;
      this.score = makeDoubleWhileHandlingInfinity(score);
    }

    private Double makeDoubleWhileHandlingInfinity(byte[] score) {
      String scoreString = Coder.bytesToString(score).toLowerCase();
      switch (scoreString) {
        case "inf":
        case "infinity":
        case "+inf":
        case "+infinity":
          return Double.POSITIVE_INFINITY;
        case "-inf":
        case "-infinity":
          return Double.NEGATIVE_INFINITY;
        default:
          return Coder.bytesToDouble(score);
      }
    }
  }
}
