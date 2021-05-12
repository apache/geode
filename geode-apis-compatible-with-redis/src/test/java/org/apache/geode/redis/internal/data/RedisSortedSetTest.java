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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisSortedSetTest {

  @BeforeClass
  public static void beforeClass() {
    InternalDataSerializer
        .getDSFIDSerializer()
        .registerDSFID(DataSerializableFixedID.REDIS_BYTE_ARRAY_WRAPPER, ByteArrayWrapper.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_SORTED_SET_ID,
        RedisSortedSet.class);
  }

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier.isSynchronized(RedisSortedSet.class
        .getMethod("toData", DataOutput.class, SerializationContext.class).getModifiers()))
            .isTrue();
  }

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    RedisSortedSet o1 = createRedisSortedSet("k1", "v1", "k2", "v2");
    o1.setExpirationTimestampNoDelta(1000);
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(o1, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSortedSet o2 = DataSerializer.readObject(in);
    assertThat(o2.equals(o1)).isTrue();
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    RedisSortedSet o1 = createRedisSortedSet("k1", "v1", "k2", "v2");
    o1.setExpirationTimestampNoDelta(1000);
    RedisSortedSet o2 = createRedisSortedSet("k1", "v1", "k2", "v2");
    o2.setExpirationTimestampNoDelta(999);
    assertThat(o1).isNotEqualTo(o2);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    RedisSortedSet o1 = createRedisSortedSet("k1", "v1", "k2", "v2");
    o1.setExpirationTimestampNoDelta(1000);
    RedisSortedSet o2 = createRedisSortedSet("k1", "v1", "k2", "v3");
    o2.setExpirationTimestampNoDelta(1000);
    assertThat(o1).isNotEqualTo(o2);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    RedisSortedSet o1 = createRedisSortedSet("k1", "v1", "k2", "v2");
    o1.setExpirationTimestampNoDelta(1000);
    RedisSortedSet o2 = createRedisSortedSet("k1", "v1", "k2", "v2");
    o2.setExpirationTimestampNoDelta(1000);
    assertThat(o1).isEqualTo(o2);
  }

  @Test
  public void equals_returnsTrue_givenDifferentEmptySortedSets() {
    RedisSortedSet o1 = new RedisSortedSet(Collections.emptyList());
    RedisSortedSet o2 = NullRedisDataStructures.NULL_REDIS_SORTED_SET;
    assertThat(o1).isEqualTo(o2);
    assertThat(o2).isEqualTo(o1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void zadd_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = Mockito.mock(Region.class);
    RedisSortedSet o1 = createRedisSortedSet("k1", "v1", "k2", "v2");

    ArrayList<byte[]> adds = new ArrayList<>();
    adds.add("k3".getBytes());
    adds.add("v3".getBytes());

    o1.zadd(region, null, adds, null);
    assertThat(o1.hasDelta()).isTrue();

    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();

    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSortedSet o2 = createRedisSortedSet("k1", "v1", "k2", "v2");
    assertThat(o2).isNotEqualTo(o1);

    o2.fromDelta(in);
    assertThat(o2).isEqualTo(o1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = mock(Region.class);
    RedisSortedSet o1 = createRedisSortedSet("k1", "v1", "k2", "v2");
    o1.setExpirationTimestamp(region, null, 999);
    assertThat(o1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSortedSet o2 = createRedisSortedSet("k1", "v1", "k2", "v2");
    assertThat(o2).isNotEqualTo(o1);
    o2.fromDelta(in);
    assertThat(o2).isEqualTo(o1);
  }


  private RedisSortedSet createRedisSortedSet(String k1, String v1, String k2, String v2) {
    List<byte[]> elements = new ArrayList<>();
    elements.add(Coder.stringToBytes(k1));
    elements.add(Coder.stringToBytes(v1));
    elements.add(Coder.stringToBytes(k2));
    elements.add(Coder.stringToBytes(v2));
    return new RedisSortedSet(elements);
  }

}
