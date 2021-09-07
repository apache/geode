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
  public static int slotForKey(byte[] value) {
    int startHashtag = Integer.MAX_VALUE;
    int endHashtag = 0;

    for (int i = 0; i < value.length; i++) {
      if (value[i] == '{' && startHashtag == Integer.MAX_VALUE) {
        startHashtag = i;
      } else if (value[i] == '}') {
        endHashtag = i;
        break;
      }
    }

    if (endHashtag - startHashtag <= 1) {
      startHashtag = -1;
      endHashtag = value.length;
    }

    return CRC16.calculate(value, startHashtag + 1, endHashtag) & (REDIS_SLOTS - 1);
  }
}
