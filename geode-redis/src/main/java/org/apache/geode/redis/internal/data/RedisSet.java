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

import static java.util.Collections.emptyList;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisSet extends AbstractRedisData {

  public static final RedisSet EMPTY = new EmptyRedisSet();
  private HashSet<ByteArrayWrapper> members;

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

  List<Object> sscan(Pattern matchPattern, int count, int cursor) {

    List<Object> returnList = new ArrayList<>();
    int size = members.size();
    int beforeCursor = 0;
    int numElements = 0;
    int i = -1;
    for (ByteArrayWrapper value : members) {
      i++;
      if (beforeCursor < cursor) {
        beforeCursor++;
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

  Collection<ByteArrayWrapper> spop(
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
      storeChanges(region, key, new RemsDeltaInfo(popped));
    }
    return popped;
  }

  Collection<ByteArrayWrapper> srandmember(int count) {
    int membersSize = members.size();
    boolean duplicatesAllowed = count < 0;
    if (duplicatesAllowed) {
      count = -count;
    }

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
    if (duplicatesAllowed) {
      ArrayList<ByteArrayWrapper> result = new ArrayList<>(count);
      while (count > 0) {
        result.add(entries[rand.nextInt(entries.length)]);
        count--;
      }
      return result;
    } else {
      Set<ByteArrayWrapper> result = new HashSet<>();
      // Note that rand.nextInt can return duplicates when "count" is high
      // so we need to use a Set to collect the results.
      while (result.size() < count) {
        ByteArrayWrapper s = entries[rand.nextInt(entries.length)];
        result.add(s);
      }
      return result;
    }
  }

  public boolean sismember(ByteArrayWrapper member) {
    return members.contains(member);
  }

  public int scard() {
    return members.size();
  }


  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
    if (deltaInfo instanceof AddsDeltaInfo) {
      AddsDeltaInfo addsDeltaInfo = (AddsDeltaInfo) deltaInfo;
      members.addAll(addsDeltaInfo.getAdds());
    } else {
      RemsDeltaInfo remsDeltaInfo = (RemsDeltaInfo) deltaInfo;
      members.removeAll(remsDeltaInfo.getRemoves());
    }
  }

  // DATA SERIALIZABLE
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeHashSet(members, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    members = DataSerializer.readHashSet(in);
  }

  /**
   * @param membersToAdd members to add to this set; NOTE this list may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @return the number of members actually added
   */
  long sadd(ArrayList<ByteArrayWrapper> membersToAdd,
      Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key) {

    membersToAdd.removeIf(memberToAdd -> !members.add(memberToAdd));
    int membersAdded = membersToAdd.size();
    if (membersAdded != 0) {
      storeChanges(region, key, new AddsDeltaInfo(membersToAdd));
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
  long srem(ArrayList<ByteArrayWrapper> membersToRemove,
      Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key) {

    membersToRemove.removeIf(memberToRemove -> !members.remove(memberToRemove));
    int membersRemoved = membersToRemove.size();
    if (membersRemoved != 0) {
      storeChanges(region, key, new RemsDeltaInfo(membersToRemove));
    }
    return membersRemoved;
  }

  /**
   * The returned set is a copy and will not be changed
   * by future changes to this instance.
   *
   * @return a set containing all the members in this set
   */
  Set<ByteArrayWrapper> smembers() {
    return new HashSet<>(members);
  }

  @Override
  public RedisDataType getType() {
    return REDIS_SET;
  }

  @Override
  protected boolean removeFromRegion() {
    return members.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisSet)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RedisSet redisSet = (RedisSet) o;
    return Objects.equals(members, redisSet.members);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), members);
  }

  @Override
  public String toString() {
    return "RedisSet{" +
        super.toString() + ", " +
        "members=" + members +
        '}';
  }
}
