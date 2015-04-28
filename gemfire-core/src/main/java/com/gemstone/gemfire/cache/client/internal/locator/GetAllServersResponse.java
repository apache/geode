/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
/**
 * 
 * @author ymahajan
 *
 */
public class GetAllServersResponse extends ServerLocationResponse {

  private ArrayList servers;

  private boolean serversFound = false;

  /** For data serializer */
  public GetAllServersResponse() {
    super();
  }

  public GetAllServersResponse(ArrayList servers) {
    this.servers = servers;
    if (servers != null && !servers.isEmpty()) {
      this.serversFound = true;
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.servers = SerializationHelper.readServerLocationList(in);
    if (this.servers != null && !this.servers.isEmpty()) {
      this.serversFound = true;
    }
  }

  public void toData(DataOutput out) throws IOException {
    SerializationHelper.writeServerLocationList(servers, out);
  }

  public ArrayList getServers() {
    return servers;
  }

  @Override
  public String toString() {
    return "GetAllServersResponse{servers=" + getServers() + "}";
  }

  public int getDSFID() {
    return DataSerializableFixedID.GET_ALL_SERVRES_RESPONSE;
  }

  @Override
  public boolean hasResult() {
    return this.serversFound;
  }
  
}
