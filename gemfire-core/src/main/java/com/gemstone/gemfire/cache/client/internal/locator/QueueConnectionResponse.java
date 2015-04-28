/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;

/**
 * A response from locator to client indicating the servers
 * to use to host the clients queue. The servers already
 * contain the queue if the durableQueueFound flag is true.
 * @author dsmith
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
