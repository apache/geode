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
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS_PER_BUCKET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.executor.cluster.CRC16;
import org.apache.geode.redis.internal.executor.cluster.RedisPartitionResolver;

public class RedisKey extends ByteArrayWrapper
    implements DataSerializableFixedID, Serializable {

  private Integer routingId;

  public RedisKey() {}

  public RedisKey(byte[] value) {
    super(value);
    routingId = (CRC16.calculate(value) % REDIS_SLOTS) / REDIS_SLOTS_PER_BUCKET;
  }

  /**
   * Used by the {@link RedisPartitionResolver} to map slots to buckets
   */
  public Object getRoutingId() {
    return routingId;
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.REDIS_BYTE_ARRAY_WRAPPER_KEY;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    DataSerializer.writeInteger(routingId, out);
    super.toData(out, context);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    routingId = DataSerializer.readInteger(in);
    super.fromData(in, context);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

}
