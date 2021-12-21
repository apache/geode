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

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A response from a locator to a client Indicating which server to connect to for client to server
 * traffic.
 *
 */
public class ClientConnectionResponse extends ServerLocationResponse {

  private ServerLocation server;

  private boolean serverFound = false;

  /** For data serializer */
  public ClientConnectionResponse() {
    super();
  }

  public ClientConnectionResponse(ServerLocation server) {
    this.server = server;
    if (server != null) {
      serverFound = true;
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    serverFound = DataSerializer.readPrimitiveBoolean(in);
    if (serverFound) {
      server = new ServerLocation();
      server.fromData(in);
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    boolean serverFound = server != null;
    DataSerializer.writePrimitiveBoolean(serverFound, out);
    if (serverFound) {
      server.toData(out);
    }
  }

  public ServerLocation getServer() {
    return server;
  }

  @Override
  public String toString() {
    return "ClientConnectionResponse{server=" + getServer() + "}";
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CLIENT_CONNECTION_RESPONSE;
  }

  @Override
  public boolean hasResult() {
    return serverFound;
  }

}
