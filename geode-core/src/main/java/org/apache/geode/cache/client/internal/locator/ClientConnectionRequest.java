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

package org.apache.geode.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A request from a client to the locator asking for a server to connect to for client to server
 * traffic.
 *
 */
public class ClientConnectionRequest extends ServerLocationRequest {
  private Set<ServerLocation> excludedServers;

  public ClientConnectionRequest() {

  }

  public ClientConnectionRequest(Set<ServerLocation> excludedServers, String serverGroup) {
    super(serverGroup);
    this.excludedServers = excludedServers;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.excludedServers = SerializationHelper.readServerLocationSet(in, context);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    SerializationHelper.writeServerLocationSet(this.excludedServers, out, context);
  }

  public Set getExcludedServers() {
    return excludedServers;
  }

  @Override
  public String toString() {
    return "ClientConnectionRequest{group=" + getServerGroup() + ", excluded="
        + getExcludedServers() + "}";
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CLIENT_CONNECTION_REQUEST;
  }
}
