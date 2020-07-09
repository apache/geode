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

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;

public class RedisStringTest {

  @BeforeClass
  public static void beforeClass() {
    InternalDataSerializer
        .getDSFIDSerializer()
        .registerDSFID(DataSerializableFixedID.REDIS_BYTE_ARRAY_WRAPPER, ByteArrayWrapper.class);
  }

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    RedisString o1 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1, 2, 3}));
    o1.setExpirationTimestampNoDelta(1000);
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(o1, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisString o2 = DataSerializer.readObject(in);
    assertThat(o2).isEqualTo(o1);
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    RedisString o1 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1, 2, 3}));
    o1.setExpirationTimestampNoDelta(1000);
    RedisString o2 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1, 2, 3}));
    o2.setExpirationTimestampNoDelta(999);
    assertThat(o1).isNotEqualTo(o2);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    RedisString o1 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1, 2, 3}));
    o1.setExpirationTimestampNoDelta(1000);
    RedisString o2 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1, 2, 2}));
    o2.setExpirationTimestampNoDelta(1000);
    assertThat(o1).isNotEqualTo(o2);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    RedisString o1 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1, 2, 3}));
    o1.setExpirationTimestampNoDelta(1000);
    RedisString o2 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1, 2, 3}));
    o2.setExpirationTimestampNoDelta(1000);
    assertThat(o1).isEqualTo(o2);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void append_stores_delta_that_is_stable() throws IOException {
    Region<ByteArrayWrapper, RedisData> region = mock(Region.class);
    RedisString o1 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1}));
    ByteArrayWrapper part2 = new ByteArrayWrapper(new byte[] {2, 3});
    o1.append(part2, region, null);
    assertThat(o1.hasDelta()).isTrue();
    assertThat(o1.get()).isEqualTo(new ByteArrayWrapper(new byte[] {0, 1, 2, 3}));
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisString o2 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1}));
    assertThat(o2).isNotEqualTo(o1);
    o2.fromDelta(in);
    assertThat(o2.get()).isEqualTo(new ByteArrayWrapper(new byte[] {0, 1, 2, 3}));
    assertThat(o2).isEqualTo(o1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() throws IOException {
    Region<ByteArrayWrapper, RedisData> region = mock(Region.class);
    RedisString o1 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1}));
    o1.setExpirationTimestamp(region, null, 999);
    assertThat(o1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisString o2 = new RedisString(new ByteArrayWrapper(new byte[] {0, 1}));
    assertThat(o2).isNotEqualTo(o1);
    o2.fromDelta(in);
    assertThat(o2).isEqualTo(o1);
  }
}
