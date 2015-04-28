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

import com.gemstone.gemfire.internal.DataSerializableFixedID;

/**
 * A request from a client to the locator asking for a
 * server to connect to for client to server traffic.
 * @author dsmith
 *
 */
public class ClientConnectionRequest extends ServerLocationRequest {
  Set/*<ServerLocation>*/ excludedServers;
  
  public ClientConnectionRequest() {
    
  }

  public ClientConnectionRequest(Set/*<ServerLocation>*/ excludedServers, String serverGroup) {
    super(serverGroup);
    this.excludedServers = excludedServers;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.excludedServers = SerializationHelper.readServerLocationSet(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    SerializationHelper.writeServerLocationSet(this.excludedServers, out);
  }

  public Set getExcludedServers() {
    return excludedServers;
  }
  
  @Override
  public String toString() {
    return "ClientConnectionRequest{group=" + getServerGroup() + ", excluded=" + getExcludedServers() + "}";
  }

  public int getDSFID() {
    return DataSerializableFixedID.CLIENT_CONNECTION_REQUEST;
  }
}
