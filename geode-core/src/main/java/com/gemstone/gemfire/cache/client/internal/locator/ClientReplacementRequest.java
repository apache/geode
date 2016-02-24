/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;

/**
 * A request from a client to the locator asking for a
 * server to connect to for client to server traffic.
 *
 */
public class ClientReplacementRequest extends ClientConnectionRequest {
  private ServerLocation currentServer;
  
  public ClientReplacementRequest() {
    
  }

  public ClientReplacementRequest(ServerLocation currentServer, Set/*<ServerLocation>*/ excludedServers, String serverGroup) {
    super(excludedServers, serverGroup);
    this.currentServer = currentServer;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.currentServer = new ServerLocation();
    InternalDataSerializer.invokeFromData(this.currentServer, in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    InternalDataSerializer.invokeToData(this.currentServer, out);
  }

  public ServerLocation getCurrentServer() {
    return this.currentServer;
  }
  
  @Override
  public String toString() {
    return "ClientReplacementRequest{group=" + getServerGroup()
      + ", excluded=" + getExcludedServers()
      + ", currentServer=" + getCurrentServer()
      + "}";
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CLIENT_REPLACEMENT_REQUEST;
  }
}
