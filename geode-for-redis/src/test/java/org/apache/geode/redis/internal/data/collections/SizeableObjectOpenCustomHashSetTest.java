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
package org.apache.geode.redis.internal.data.collections;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.junit.Test;

import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.redis.internal.data.RedisSet;

public class SizeableObjectOpenCustomHashSetTest {
  private final ObjectSizer sizer = ReflectionObjectSizer.getInstance();

  private int expectedSize(RedisSet.MemberSet set) {
    return sizer.sizeof(set) - sizer.sizeof(ByteArrays.HASH_STRATEGY);
  }

  @Test
  public void getSizeInBytesIsAccurateForByteArrays() {
    List<byte[]> initialElements = new ArrayList<>();
    int initialNumberOfElements = 20;
    int elementsToAdd = 100;
    for (int i = 0; i < initialNumberOfElements; ++i) {
      initialElements.add(new byte[] {(byte) i});
    }
    // Create a set with an initial size and confirm that it correctly reports its size
    RedisSet.MemberSet set = new RedisSet.MemberSet(initialElements);
    assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));

    // Add enough members to force a resize and assert that the size is correct after each add
    for (int i = initialNumberOfElements; i < initialNumberOfElements + elementsToAdd; ++i) {
      set.add(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
    }
    assertThat(set.size()).isEqualTo(initialNumberOfElements + elementsToAdd);

    // Remove all the members and assert that the size is correct after each remove
    for (int i = 0; i < initialNumberOfElements + elementsToAdd; ++i) {
      set.remove(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
    }
    assertThat(set.size()).isEqualTo(0);
  }
}
