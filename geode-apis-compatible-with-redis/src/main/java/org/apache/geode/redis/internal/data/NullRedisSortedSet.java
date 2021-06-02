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

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.executor.sortedset.ZAddOptions;
import org.apache.geode.redis.internal.netty.Coder;

class NullRedisSortedSet extends RedisSortedSet {

  NullRedisSortedSet() {
    super(new ArrayList<>());
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  long zadd(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToAdd,
      ZAddOptions options) {
    if (options.isXX()) {
      return 0;
    }
    RedisSortedSet sortedSet = new RedisSortedSet(membersToAdd);
    region.create(key, sortedSet);
    return sortedSet.getSortedSetSize();
  }

  @Override
  byte[] zscore(byte[] member) {
    return null;
  }

  @Override
  byte[] zincrby(Region<RedisKey, RedisData> region, RedisKey key, byte[] increment,
      byte[] member) {
    byte[] incr = processIncrement(Coder.bytesToString(increment));

    List<byte[]> valuesToAdd = new ArrayList<>();
    valuesToAdd.add(incr);
    valuesToAdd.add(member);

    RedisSortedSet sortedSet = new RedisSortedSet(valuesToAdd);
    region.create(key, sortedSet);

    return incr;
  }

  private byte[] processIncrement(String stringIncr) {
    switch (stringIncr) {
      case "inf":
      case "+inf":
      case "infinity":
      case "+infinity":
        stringIncr = Coder.doubleToString(POSITIVE_INFINITY);
        break;
      case "-inf":
      case "-infinity":
        stringIncr = Coder.doubleToString(NEGATIVE_INFINITY);
        break;
      default:
        try {
          Double.parseDouble(stringIncr);
        } catch (NumberFormatException nfe) {
          throw new NumberFormatException(ERROR_NOT_A_VALID_FLOAT);
        }
        break;
    }
    return Coder.stringToBytes(stringIncr);
  }
}
