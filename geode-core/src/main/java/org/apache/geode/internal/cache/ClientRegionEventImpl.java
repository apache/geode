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

package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;


/**
 * Class <code>ClientRegionEventImpl</code> is a region event with the client's host and port for
 * notification purposes.
 *
 *
 * @since GemFire 5.1
 */
public class ClientRegionEventImpl extends RegionEventImpl {

  /**
   * The originating membershipId of this event.
   */
  private ClientProxyMembershipID context;

  public ClientRegionEventImpl() {}

  /**
   * To be called from the Distributed Message without setting EventID
   *
   */
  public ClientRegionEventImpl(LocalRegion region, Operation op, Object callbackArgument,
      boolean originRemote, DistributedMember distributedMember, ClientProxyMembershipID contx) {
    super(region, op, callbackArgument, originRemote, distributedMember);
    setContext(contx);
  }

  public ClientRegionEventImpl(LocalRegion region, Operation op, Object callbackArgument,
      boolean originRemote, DistributedMember distributedMember, ClientProxyMembershipID contx,
      EventID eventId) {
    super(region, op, callbackArgument, originRemote, distributedMember, eventId);
    setContext(contx);
  }


  /**
   * sets The membershipId originating this event
   *
   */
  protected void setContext(ClientProxyMembershipID contx) {
    context = contx;
  }

  /**
   * Returns The context originating this event
   *
   * @return The context originating this event
   */
  @Override
  public ClientProxyMembershipID getContext() {
    return context;
  }

  @Override
  public String toString() {
    String superStr = super.toString();
    StringBuilder buffer = new StringBuilder();
    String str = superStr.substring(0, superStr.length() - 1);
    buffer.append(str).append(";context=").append(getContext()).append(']');
    return buffer.toString();
  }

  @Override
  public int getDSFID() {
    return CLIENT_REGION_EVENT;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(getContext(), out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    setContext(ClientProxyMembershipID.readCanonicalized(in));
  }
}
