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
package org.apache.geode.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;

/**
 * A response from locator to client indicating the servers
 * to use to host the clients queue. The servers already
 * contain the queue if the durableQueueFound flag is true.
 *
 */
public class QueueConnectionResponse extends ServerLocationResponse {
  
  private boolean durableQueueFound;
  private List servers;
  private boolean serversFound = false;

  public QueueConnectionResponse() {
  }

  public QueueConnectionResponse(boolean durableQueueFound, List servers) {
    this.durableQueueFound = durableQueueFound;
    this.servers = servers;
    if (servers != null && !servers.isEmpty()) {
      this.serversFound = true;
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    durableQueueFound = DataSerializer.readPrimitiveBoolean(in);
    servers = SerializationHelper.readServerLocationList(in);
    if (this.servers != null && !this.servers.isEmpty()) {
      this.serversFound = true;
    }
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveBoolean(durableQueueFound, out);
    SerializationHelper.writeServerLocationList(servers, out);
  }

  public boolean isDurableQueueFound() {
    return durableQueueFound;
  }

  public List getServers() {
    return servers;
  }
  
  @Override
  public String toString() {
    return "QueueConnectionResponse{durableQueueFound=" + durableQueueFound + ", servers="
        + servers + "}";
  }
  
  public int getDSFID() {
    return DataSerializableFixedID.QUEUE_CONNECTION_RESPONSE;
  }

  @Override
  public boolean hasResult() {
    return this.serversFound;
  }
  
}
