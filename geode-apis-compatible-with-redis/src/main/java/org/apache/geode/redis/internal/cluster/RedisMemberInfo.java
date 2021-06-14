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

package org.apache.geode.redis.internal.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

public class RedisMemberInfo implements DataSerializableFixedID, Serializable {

  private static final long serialVersionUID = -10228877687322470L;

  private DistributedMember member;
  private String hostAddress;
  private int redisPort;

  // For serialization
  public RedisMemberInfo() {}

  public RedisMemberInfo(DistributedMember member, String hostAddress, int redisPort) {
    this.member = member;
    this.hostAddress = hostAddress;
    this.redisPort = redisPort;
  }

  public DistributedMember getMember() {
    return member;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public int getRedisPort() {
    return redisPort;
  }

  @Override
  public int getDSFID() {
    return REDIS_MEMBER_INFO_ID;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    DataSerializer.writeObject(member, out);
    DataSerializer.writeString(hostAddress, out);
    out.writeInt(redisPort);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    member = DataSerializer.readObject(in);
    hostAddress = DataSerializer.readString(in);
    redisPort = DataSerializer.readPrimitiveInt(in);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public String toString() {
    return member.toString() + " hostAddress: " + hostAddress + " redisPort: " + redisPort;
  }
}
