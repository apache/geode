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


import org.apache.geode.Delta;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.size.Sizeable;

public interface RedisData extends Delta, DataSerializableFixedID, Sizeable {


  /**
   * Returns true if this instance does not exist.
   */
  default boolean isNull() {
    return false;
  }

  default boolean exists() {
    return !isNull();
  }

  RedisDataType getType();

  void setExpirationTimestamp(Region<RedisKey, RedisData> region, RedisKey key, long value);

  long getExpirationTimestamp();

  int persist(Region<RedisKey, RedisData> region, RedisKey key);

  boolean hasExpired();

  boolean hasExpired(long now);

  long pttl(Region<RedisKey, RedisData> region, RedisKey key);

  int pexpireat(CommandHelper helper, RedisKey key, long timestamp);

  void doExpiration(CommandHelper helper, RedisKey key);

  String type();

  boolean rename(Region<RedisKey, RedisData> region, RedisKey oldKey, RedisKey newKey);

  default boolean getForceRecalculateSize() {
    return true;
  }

}
