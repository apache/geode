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

import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_LIST;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.SerializationContext;

public class RedisListTest {

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    RedisList list1 = createRedisList(1, 2);
    int expirationTimestamp = 1000;
    list1.setExpirationTimestampNoDelta(expirationTimestamp);
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(list1, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisList list2 = DataSerializer.readObject(in);
    assertThat(list2.getExpirationTimestamp())
        .isEqualTo(list1.getExpirationTimestamp())
        .isEqualTo(expirationTimestamp);
    assertThat(list2).isEqualTo(list1);
  }

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier
        .isSynchronized(RedisList.class
            .getMethod("toData", DataOutput.class, SerializationContext.class).getModifiers()))
                .isTrue();
  }

  @Test
  public void hashcode_returnsSameValue_forEqualLists() {
    RedisList list1 = createRedisList(1, 2);
    RedisList list2 = createRedisList(1, 2);
    assertThat(list1).isEqualTo(list2);
    assertThat(list1.hashCode()).isEqualTo(list2.hashCode());
  }

  @Test
  public void hashcode_returnsDifferentValue_forDifferentLists() {
    RedisList list1 = createRedisList(1, 2);
    RedisList list2 = createRedisList(2, 1);
    assertThat(list1).isNotEqualTo(list2);
    assertThat(list1.hashCode()).isNotEqualTo(list2.hashCode());
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    RedisList list1 = createRedisList(1, 2);
    list1.setExpirationTimestampNoDelta(1000);
    RedisList list2 = createRedisList(1, 2);
    list2.setExpirationTimestampNoDelta(999);
    assertThat(list1).isNotEqualTo(list2);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    RedisList list1 = createRedisList(1, 2);
    list1.setExpirationTimestampNoDelta(1000);
    RedisList list2 = createRedisList(1, 3);
    list2.setExpirationTimestampNoDelta(1000);
    assertThat(list1).isNotEqualTo(list2);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    RedisList list1 = createRedisList(1, 2);
    int expirationTimestamp = 1000;
    list1.setExpirationTimestampNoDelta(expirationTimestamp);
    RedisList list2 = createRedisList(1, 2);
    list2.setExpirationTimestampNoDelta(expirationTimestamp);
    assertThat(list1).isEqualTo(list2);
    assertThat(list2.getExpirationTimestamp())
        .isEqualTo(list1.getExpirationTimestamp())
        .isEqualTo(expirationTimestamp);
  }

  @Test
  public void equals_returnsTrue_givenDifferentEmptyLists() {
    RedisList list1 = new RedisList();
    RedisList list2 = NULL_REDIS_LIST;
    assertThat(list1).isEqualTo(list2);
    assertThat(list2).isEqualTo(list1);
  }

  @Test
  public void removeIndexes_storesStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    when(region.put(any(), any())).thenAnswer(this::validateDeltaSerialization);

    byte[] element = new byte[] {1};
    RedisList list = createRedisListWithDuplicateElements();

    list.lrem(2, element, region, null);

    verify(region).put(any(), any());
    assertThat(list.hasDelta()).isFalse();
  }

  private Object validateDeltaSerialization(InvocationOnMock invocation) throws IOException {
    RedisList value = invocation.getArgument(1, RedisList.class);
    assertThat(value.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    value.toDelta(out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisList list2 = createRedisListWithDuplicateElements();
    assertThat(list2).isNotEqualTo(value);
    list2.fromDelta(in);
    assertThat(list2).isEqualTo(value);
    return null;
  }


  private RedisList createRedisListWithDuplicateElements() {
    RedisList newList = new RedisList();
    newList.elementPushHead(new byte[] {1});
    newList.elementPushHead(new byte[] {2});
    newList.elementPushHead(new byte[] {1});
    newList.elementPushHead(new byte[] {1});
    newList.elementPushHead(new byte[] {3});
    return newList;
  }

  private RedisList createRedisList(int e1, int e2) {
    RedisList newList = new RedisList();
    newList.elementPushHead(new byte[] {(byte) e1});
    newList.elementPushHead(new byte[] {(byte) e2});
    return newList;
  }
}
