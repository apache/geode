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
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;

/**
 * A request from a client to the locator asking for a
 * server to connect to for client to server traffic.
 * @author dsmith
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
