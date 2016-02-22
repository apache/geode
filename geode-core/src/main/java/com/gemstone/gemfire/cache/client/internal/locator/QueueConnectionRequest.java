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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * A request from a client to locator asking for a server
 * to host a queue. If the durable client Id is specified, the locator
 * will attempt to discover a pre-existing queue.
 *
 */
public class QueueConnectionRequest extends ServerLocationRequest {
  private ClientProxyMembershipID proxyId;
  private Set excludedServers;
  private int redundantCopies;
  private boolean findDurable = false;
  
  public QueueConnectionRequest() {
    super();
  }

  public QueueConnectionRequest(ClientProxyMembershipID proxyId, int redundantCopies, Set excludedServers, String serverGroup,boolean findDurable) {
    super(serverGroup);
    this.proxyId = proxyId;
    this.excludedServers = excludedServers;
    this.redundantCopies = redundantCopies;
    this.findDurable = findDurable;
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    
    proxyId = ClientProxyMembershipID.readCanonicalized(in);
    redundantCopies = DataSerializer.readPrimitiveInt(in);
    this.excludedServers = SerializationHelper.readServerLocationSet(in);
    this.findDurable = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(proxyId, out);
    DataSerializer.writePrimitiveInt(redundantCopies, out);
    SerializationHelper.writeServerLocationSet(this.excludedServers, out);
    out.writeBoolean(this.findDurable);
  }
  
  public Set getExcludedServers() {
    return excludedServers;
  }

  public ClientProxyMembershipID getProxyId() {
    return proxyId;
  }

  public int getRedundantCopies() {
    return redundantCopies;
  }
  
  public boolean isFindDurable() {
    return this.findDurable;
  }
  
  @Override
  public String toString() {
    return "QueueConnectionRequest{group=" + getServerGroup() + ", excluded="
        + getExcludedServers() + ", redundant= " + redundantCopies
        + ",findDurable=" + findDurable + ",proxyId=" + proxyId + "}";
  }
  
  public int getDSFID() {
    return DataSerializableFixedID.QUEUE_CONNECTION_REQUEST;
  }
}
