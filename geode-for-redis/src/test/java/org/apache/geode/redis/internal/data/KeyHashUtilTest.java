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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;
import redis.clients.jedis.util.JedisClusterCRC16;

import org.apache.geode.redis.internal.netty.Coder;

@RunWith(JUnitQuickcheck.class)
public class KeyHashUtilTest {

  @Property
  public void geodeAndJedisGenerateSameSlotWithHardcodedExamples() {
    compareHashes("nohash");
    compareHashes("with{hash}");
    compareHashes("with{two}{hashes}");
    compareHashes("with{borked{hashes}");
    compareHashes("with{unmatched");
    compareHashes("aaa}bbb{tag}ccc");
    compareHashes("withunmatchedright}");
    compareHashes("empty{}hash");
    compareHashes("nested{some{hash}or}hash");

    compareHashes("name{tag1000}");
    compareHashes("{tag1000");
    compareHashes("}tag1000{");
    compareHashes("tag{}1000");
    compareHashes("tag}{1000");
    compareHashes("{tag1000}}bar");
    compareHashes("foo{tag1000}{bar}");
    compareHashes("foo{}{tag1000}");
    compareHashes("{}{tag1000}");
    compareHashes("foo{{tag1000}}bar");
    compareHashes("");
  }

  @Property
  public void geodeAndJedisGenerateSameSlot(String key,
      @Size(min = 0, max = 2) Set<@InRange(minInt = 0, maxInt = 10) Integer> leftParenLocations,
      @Size(min = 0, max = 2) Set<@InRange(minInt = 0, maxInt = 10) Integer> rightParenLocations) {
    byte[] keyBytes = Coder.stringToBytes(key);
    leftParenLocations.stream()
        .filter(location -> location < keyBytes.length)
        .forEach(location -> keyBytes[location] = '{');
    rightParenLocations.stream()
        .filter(location -> location < keyBytes.length)
        .forEach(location -> keyBytes[location] = '}');

    compareHashes(Coder.bytesToString(keyBytes));
  }

  private static void compareHashes(String key) {
    int jedisSlot = JedisClusterCRC16.getSlot(key);
    short geodeSlot = KeyHashUtil.slotForKey(Coder.stringToBytes(key));
    assertThat(geodeSlot)
        .withFailMessage("Slot did not match for key %s", key)
        .isEqualTo((short) jedisSlot);
  }

}
