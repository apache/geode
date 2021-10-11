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


import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_DATA;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_HASH;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SET;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SORTED_SET;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_STRING;

public enum RedisDataType {

  REDIS_DATA("data", NULL_REDIS_DATA),
  REDIS_STRING("string", NULL_REDIS_STRING),
  REDIS_HASH("hash", NULL_REDIS_HASH),
  REDIS_SET("set", NULL_REDIS_SET),
  REDIS_SORTED_SET("sortedset", NULL_REDIS_SORTED_SET);

  private final String toStringValue;
  private final RedisData nullType;

  RedisDataType(String toString, RedisData nullType) {
    toStringValue = toString;
    this.nullType = nullType;
  }

  @Override
  public String toString() {
    return toStringValue;
  }

  public RedisData getNullType() {
    return nullType;
  }
}
