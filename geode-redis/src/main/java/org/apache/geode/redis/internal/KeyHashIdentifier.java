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

/**
 * Key object specifically for use by the RedisLockService. This object has various unique
 * properties:
 * <ul>
 * <li>
 * The hashcode is calculated using the low 64 bits of a 128 bit Murmur3 hash
 * </li>
 * <li>
 * Equality between keys is determined exclusively using the hashcode. This is done in order
 * to speed up equality checks.
 * </li>
 * </ul>
 * <p/>
 * This implies that keys may exhibit equality even when their values are actually different.
 * However, since these keys are only used to identify a lock, the worst that may happen is that
 * a request for a lock will identify an already existing lock and have to wait on that lock to
 * become free.
 */
public class KeyHashIdentifier {

  private final long hashCode;

  /**
   * Since the key is used by the RedisLockService in a WeakHashMap, keep a reference to it so that
   * it is not removed prematurely (before the lock is actually released).
   */
  private final byte[] key;

  public KeyHashIdentifier(byte[] key) {
    this.key = key;
    long[] murmur = MurmurHash3.hash128(key);
    this.hashCode = murmur[0];
  }

  @Override
  public int hashCode() {
    return (int) hashCode;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KeyHashIdentifier)) {
      return false;
    }

    KeyHashIdentifier otherKey = (KeyHashIdentifier) other;
    return this.hashCode == otherKey.hashCode;
  }

}
