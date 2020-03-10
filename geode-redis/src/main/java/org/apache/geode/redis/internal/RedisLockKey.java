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

package org.apache.geode.redis.internal;

import org.apache.commons.codec.digest.MurmurHash3;

public class RedisLockKey {

  private final long hashCode;

  private final byte[] value;

  public RedisLockKey(byte[] value) {
    this.value = value;
    long[] murmur = MurmurHash3.hash128(value);
    this.hashCode = murmur[0];
  }

  @Override
  public int hashCode() {
    return (int) hashCode;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof RedisLockKey)) {
      return false;
    }

    RedisLockKey otherKey = (RedisLockKey) other;
    return this.hashCode == otherKey.hashCode;
  }

}
