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
package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS;

import org.apache.geode.redis.internal.executor.cluster.CRC16;

/**
 * Utility for parsing the hashtags in a redis key and computing the redis slot of the key.
 */
public class KeyHashUtil {

  /**
   * Compute the slot of a given redis key
   *
   * @return the slot, a value between 0 and 16K.
   */
  public static short slotForKey(byte[] key) {
    int startHashtag;
    int endHashtag;

    for (startHashtag = 0; startHashtag < key.length; startHashtag++) {
      if (key[startHashtag] == (byte) '{') {
        break;
      }
    }

    // No starting hashtag, hash the whole thing
    if (startHashtag == key.length) {
      return crc16(key, 0, key.length);
    }

    for (endHashtag = startHashtag + 1; endHashtag < key.length; endHashtag++) {
      if (key[endHashtag] == (byte) '}') {
        break;
      }
    }

    // No ending hashtag, or empty hashtag. Hash the whole thing.
    if (endHashtag == key.length || endHashtag == startHashtag + 1) {
      return crc16(key, 0, key.length);
    }

    return crc16(key, startHashtag + 1, endHashtag);
  }

  private static short crc16(byte[] key, int start, int end) {
    return (short) (CRC16.calculate(key, start, end) & (REDIS_SLOTS - 1));
  }
}
