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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * A request from a client to locator asking for a server
 * to host a queue. If the durable client Id is specified, the locator
 * will attempt to discover a pre-existing queue.
 * @author dsmith
 * @author gregp
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
