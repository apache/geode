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
import java.util.List;

import org.junit.Test;

import org.apache.geode.redis.internal.data.delta.AddByteArrays;
import org.apache.geode.redis.internal.data.delta.AddByteArraysTail;
import org.apache.geode.redis.internal.data.delta.InsertByteArray;
import org.apache.geode.redis.internal.data.delta.RemoveElementsByIndex;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrayAtOffset;
import org.apache.geode.redis.internal.data.delta.RetainElementsByIndexRange;

public class DeltaClassesJUnitTest {

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
  public void testRetainElementsByIndexRangeDelta() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RedisList redisList = makeRedisList();
    RetainElementsByIndexRange source =
        new RetainElementsByIndexRange((byte) (redisList.getVersion() + 1), 0, 1);

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(2);
    assertThat(redisList.lindex(0)).isEqualTo("zero".getBytes());
    assertThat(redisList.lindex(1)).isEqualTo("one".getBytes());
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
}
