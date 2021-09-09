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

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;

public class RedisKeyJUnitTest {

  @BeforeClass
  public static void classSetup() {
    InternalDataSerializer.getDSFIDSerializer()
        .registerDSFID(DataSerializableFixedID.REDIS_KEY, RedisKey.class);
  }

  @Test
  public void testSerialization() throws Exception {
    RedisKey keyOut = new RedisKey(stringToBytes("012345"));

    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(keyOut, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());

    RedisKey keyIn = DataSerializer.readObject(in);
    assertThat(keyIn).isEqualTo(keyOut);
  }
}
