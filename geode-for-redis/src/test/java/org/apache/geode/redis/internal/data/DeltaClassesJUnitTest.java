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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.redis.internal.data.delta.AddByteArrayDoublePairs;
import org.apache.geode.redis.internal.data.delta.AddByteArrayPairs;
import org.apache.geode.redis.internal.data.delta.AddByteArrays;
import org.apache.geode.redis.internal.data.delta.AddByteArraysTail;
import org.apache.geode.redis.internal.data.delta.AppendByteArray;
import org.apache.geode.redis.internal.data.delta.InsertByteArray;
import org.apache.geode.redis.internal.data.delta.RemoveByteArrays;
import org.apache.geode.redis.internal.data.delta.RemoveElementsByIndex;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrayAtOffset;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrayDoublePairs;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrays;
import org.apache.geode.redis.internal.data.delta.ReplaceByteAtOffset;
import org.apache.geode.redis.internal.data.delta.RetainElementsByIndexRange;
import org.apache.geode.redis.internal.data.delta.SetByteArray;
import org.apache.geode.redis.internal.data.delta.SetByteArrayAndTimestamp;
import org.apache.geode.redis.internal.data.delta.SetTimestamp;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class DeltaClassesJUnitTest {
  @Test
  public void testAddByteArraysDeltaForRedisList() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<byte[]> deltaList = new ArrayList<>();
    deltaList.add("secondNew".getBytes());
    deltaList.add("firstNew".getBytes());
    RedisList redisList = makeRedisList();
    AddByteArrays source = new AddByteArrays(deltaList, (byte) (redisList.getVersion() + 1));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(5);
    assertThat(redisList.lindex(0)).isEqualTo("firstNew".getBytes());
    assertThat(redisList.lindex(1)).isEqualTo("secondNew".getBytes());
    assertThat(redisList.lindex(2)).isEqualTo("zero".getBytes());
  }

  @Test
  public void testAddByteArraysDeltaForRedisSet() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<byte[]> deltaList = new ArrayList<>();
    deltaList.add("secondNew".getBytes());
    deltaList.add("firstNew".getBytes());
    RedisSet redisSet = makeRedisSet();
    AddByteArrays source = new AddByteArrays(deltaList, (byte) (redisSet.getVersion() + 1));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisSet.fromDelta(dis);

    assertThat(redisSet.scard()).isEqualTo(5);
    assertThat(redisSet.sismember("firstNew".getBytes())).isTrue();
    assertThat(redisSet.sismember("secondNew".getBytes())).isTrue();
  }

  @Test
  public void testAddByteArraysTailDelta() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<byte[]> deltaList = new ArrayList<>();
    deltaList.add("firstNew".getBytes());
    deltaList.add("secondNew".getBytes());
    RedisList redisList = makeRedisList();
    AddByteArraysTail source =
        new AddByteArraysTail((byte) (redisList.getVersion() + 1), deltaList);

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(5);
    assertThat(redisList.lindex(2)).isEqualTo("two".getBytes());
    assertThat(redisList.lindex(3)).isEqualTo("firstNew".getBytes());
    assertThat(redisList.lindex(4)).isEqualTo("secondNew".getBytes());
  }

  @Test
  public void testAddByteArrayPairs() throws Exception {
    String original = "0123456789";
    String payload = "something amazing I guess";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    AddByteArrayPairs source = new AddByteArrayPairs(original.getBytes(), payload.getBytes());

    source.serializeTo(dos);

    RedisHash redisHash = makeRedisHash();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisHash.fromDelta(dis);

    assertThat(redisHash.hlen()).isEqualTo(4);
    assertThat(redisHash.hget(original.getBytes())).isEqualTo(payload.getBytes());
  }

  @Test
  public void testAddByteArrayDoublePairs() throws Exception {
    String original = "omega";
    double score = 3.0;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    AddByteArrayDoublePairs source = new AddByteArrayDoublePairs(1);
    source.add(original.getBytes(), score);

    source.serializeTo(dos);

    RedisSortedSet redisSortedSet = makeRedisSortedSet();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisSortedSet.fromDelta(dis);

    assertThat(redisSortedSet.zcard()).isEqualTo(4);
    assertThat(redisSortedSet.zrank(original.getBytes())).isEqualTo(2L);
  }

  @Test
  public void testAppendByteArray() throws Exception {
    String original = "0123456789";
    String payload = "something amazing I guess";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    AppendByteArray source = new AppendByteArray((byte) 3, payload.getBytes());

    source.serializeTo(dos);

    RedisString redisString = new RedisString(original.getBytes());

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisString.fromDelta(dis);

    assertThat(new String(redisString.get())).isEqualTo(original + payload);
  }

  @Test
  public void testInsertByteArrayDelta() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RedisList redisList = makeRedisList();
    InsertByteArray source =
        new InsertByteArray((byte) (redisList.getVersion() + 1), "newElement".getBytes(), 1);

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(4);
    assertThat(redisList.lindex(1)).isEqualTo("newElement".getBytes());
  }

  @Test
  public void testRemoveByteArraysDeltaForRedisSet() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RemoveByteArrays source =
        new RemoveByteArrays(Arrays.asList("zero".getBytes(), "two".getBytes()));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisSet redisSet = makeRedisSet();
    redisSet.fromDelta(dis);

    assertThat(redisSet.scard()).isEqualTo(1);
    assertThat(redisSet.sismember("one".getBytes())).isTrue();
  }

  @Test
  public void testRemoveByteArraysDeltaForRedisSortedSet() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RemoveByteArrays source =
        new RemoveByteArrays(Arrays.asList("alpha".getBytes(), "gamma".getBytes()));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisSortedSet redisSortedSet = makeRedisSortedSet();
    redisSortedSet.fromDelta(dis);

    assertThat(redisSortedSet.zcard()).isEqualTo(1);
    assertThat(redisSortedSet.zrank("beta".getBytes())).isEqualTo(0L);
  }

  @Test
  public void testRemoveByteArraysDeltaForRedisHash() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RemoveByteArrays source =
        new RemoveByteArrays(Arrays.asList("zero".getBytes()));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisHash redisHash = makeRedisHash();
    redisHash.fromDelta(dis);

    assertThat(redisHash.hlen()).isEqualTo(2);
    assertThat(redisHash.hget("zero".getBytes())).isNull();
  }

  @Test
  public void testReplaceByteArraysDelta() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    Set<byte[]> newSet = new RedisSet.MemberSet(1);
    newSet.add("alpha".getBytes());
    ReplaceByteArrays source = new ReplaceByteArrays(newSet);

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisSet redisSet = makeRedisSet();
    redisSet.fromDelta(dis);

    assertThat(redisSet.scard()).isEqualTo(1);
    assertThat(redisSet.sismember("alpha".getBytes())).isTrue();
  }

  @Test
  public void testReplaceByteArrayDoublePairsDelta() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    RedisSortedSet.MemberMap newMemberMap = new RedisSortedSet.MemberMap(1);
    byte[] deltaBytes = "delta".getBytes();
    RedisSortedSet.OrderedSetEntry entry = new RedisSortedSet.OrderedSetEntry(deltaBytes, 2.0);
    newMemberMap.put(deltaBytes, entry);
    ReplaceByteArrayDoublePairs source = new ReplaceByteArrayDoublePairs(newMemberMap);

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisSortedSet redisSortedSet = makeRedisSortedSet();
    redisSortedSet.fromDelta(dis);

    assertThat(redisSortedSet.zcard()).isEqualTo(1);
    assertThat(redisSortedSet.zrank(deltaBytes)).isEqualTo(0L);
  }

  @Test
  public void testReplaceByteAtOffsetDelta() throws Exception {
    String original = "0123456789";
    String expected = "012a456789";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    ReplaceByteAtOffset source = new ReplaceByteAtOffset(3, "a".getBytes()[0]);

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisString redisString = new RedisString(original.getBytes());
    redisString.fromDelta(dis);

    assertThat(redisString.get()).isEqualTo(expected.getBytes());
  }

  @Test
  public void testReplaceByteArrayAtOffsetForRedisString() throws Exception {
    String original = "0123456789";
    String payload = "something amazing I guess";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    ReplaceByteArrayAtOffset source = new ReplaceByteArrayAtOffset(3, payload.getBytes());

    source.serializeTo(dos);

    RedisString redisString = new RedisString(original.getBytes());

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisString.fromDelta(dis);

    assertThat(new String(redisString.get())).isEqualTo(original.substring(0, 3) + payload);
  }

  @Test
  public void testReplaceByteArrayAtOffsetForRedisList() throws Exception {
    String payload = "something amazing I guess";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    ReplaceByteArrayAtOffset source = new ReplaceByteArrayAtOffset(1, payload.getBytes());

    source.serializeTo(dos);

    RedisList redisList = makeRedisList();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(3);
    assertThat(redisList.lindex(0)).isEqualTo("zero".getBytes());
    assertThat(redisList.lindex(1)).isEqualTo(payload.getBytes());
    assertThat(redisList.lindex(2)).isEqualTo("two".getBytes());
  }

  @Test
  public void testSetByteArrayDelta() throws Exception {
    String original = "0123456789";
    String payload = "something amazing I guess";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    SetByteArray source = new SetByteArray(payload.getBytes());

    source.serializeTo(dos);

    RedisString redisString = new RedisString(original.getBytes());

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisString.fromDelta(dis);

    assertThat(new String(redisString.get())).isEqualTo(payload);
  }

  @Test
  public void testSetByteArrayAndTimestampDelta() throws Exception {
    String original = "0123456789";
    String payload = "something amazing I guess";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    SetByteArrayAndTimestamp source = new SetByteArrayAndTimestamp(payload.getBytes(), 2);

    source.serializeTo(dos);

    RedisString redisString = new RedisString(original.getBytes());

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisString.fromDelta(dis);

    assertThat(new String(redisString.get())).isEqualTo(payload);
    assertThat(redisString.getExpirationTimestamp()).isEqualTo(2);
  }

  @Test
  public void testSetTimestampDelta() throws Exception {
    String original = "0123456789";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    SetTimestamp source = new SetTimestamp(2);

    source.serializeTo(dos);

    RedisString redisString = new RedisString(original.getBytes());

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisString.fromDelta(dis);

    assertThat(new String(redisString.get())).isEqualTo(original);
    assertThat(redisString.getExpirationTimestamp()).isEqualTo(2);
  }

  @Test
  public void testRemoveElementsByIndex() throws Exception {
    List<Integer> payload = new ArrayList<>();
    payload.add(0);
    payload.add(2);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RedisList redisList = makeRedisList();

    RemoveElementsByIndex source =
        new RemoveElementsByIndex((byte) (redisList.getVersion() + 1), payload);

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(1);
    assertThat(redisList.lindex(0)).isEqualTo("one".getBytes());
  }

  @Test
  @Parameters(method = "getRetainElementsRanges")
  @TestCaseName("{method}: start:{0}, end:{1}, expected:{2}")
  public void testRetainElementsByIndexRangeDelta(int start, int end, byte[][] expected)
      throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RedisList redisList = makeRedisList();
    RetainElementsByIndexRange source =
        new RetainElementsByIndexRange((byte) (redisList.getVersion() + 1), start, end);

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisList.fromDelta(dis);

    assertThat(redisList.lrange(0, -1)).containsExactly(expected);
  }

  @SuppressWarnings("unused")
  private Object[] getRetainElementsRanges() {
    // Values are start, end, expected result
    // For initial list of {"zero", "one", "two"}
    return new Object[] {
        new Object[] {0, 0, new byte[][] {"zero".getBytes()}},
        new Object[] {0, 1, new byte[][] {"zero".getBytes(), "one".getBytes()}},
        new Object[] {0, 2, new byte[][] {"zero".getBytes(), "one".getBytes(), "two".getBytes()}},
        new Object[] {1, 2, new byte[][] {"one".getBytes(), "two".getBytes()}},
        new Object[] {2, 2, new byte[][] {"two".getBytes()}}
    };
  }

  private RedisList makeRedisList() {
    RedisList redisList = new RedisList();
    redisList.applyAddByteArrayTailDelta("zero".getBytes());
    redisList.applyAddByteArrayTailDelta("one".getBytes());
    redisList.applyAddByteArrayTailDelta("two".getBytes());
    return redisList;
  }

  private RedisSet makeRedisSet() {
    RedisSet redisSet = new RedisSet(5);
    redisSet.membersAdd("zero".getBytes());
    redisSet.membersAdd("one".getBytes());
    redisSet.membersAdd("two".getBytes());
    return redisSet;
  }

  private RedisSortedSet makeRedisSortedSet() {
    RedisSortedSet redisSortedSet = new RedisSortedSet(3);
    redisSortedSet.memberAdd("alpha".getBytes(), 1.0d);
    redisSortedSet.memberAdd("beta".getBytes(), 2.0d);
    redisSortedSet.memberAdd("gamma".getBytes(), 4.0d);
    return redisSortedSet;
  }

  private RedisHash makeRedisHash() {
    List<byte[]> pairList = new ArrayList<>();
    pairList.add("zero".getBytes());
    pairList.add("firstVal".getBytes());
    pairList.add("one".getBytes());
    pairList.add("secondVal".getBytes());
    pairList.add("two".getBytes());
    pairList.add("thirdVal".getBytes());
    RedisHash redisHash = new RedisHash(pairList);
    return redisHash;
  }
}
