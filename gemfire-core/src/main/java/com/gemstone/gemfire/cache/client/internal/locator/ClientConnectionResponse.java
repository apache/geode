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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.DataSerializableFixedID;

/**
 * A response from a locator to a client
 * Indicating which server to connect to for client to server traffic.
 * @author dsmith
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
      this.serverFound = true;
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.serverFound = DataSerializer.readPrimitiveBoolean(in);
    if (this.serverFound) {
      server = new ServerLocation();
      server.fromData(in);
    }
  }

  public void toData(DataOutput out) throws IOException {
    boolean serverFound = server != null;
    DataSerializer.writePrimitiveBoolean(serverFound, out);
    if(serverFound) {
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

  public int getDSFID() {
    return DataSerializableFixedID.CLIENT_CONNECTION_RESPONSE;
  }
  
  @Override
  public boolean hasResult() {
    return this.serverFound;
  }
  
}
