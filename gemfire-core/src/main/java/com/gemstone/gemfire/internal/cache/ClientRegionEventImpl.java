/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

//import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * Class <code>ClientRegionEventImpl</code> is a
 * region event with the client's
 * host and port for notification purposes.
 * 
 * @author Girish Thombare
 * 
 * @since 5.1
 */
public final class ClientRegionEventImpl extends RegionEventImpl
  {

  /**
   * The originating membershipId of this event.
   */
  private  ClientProxyMembershipID context;

  public ClientRegionEventImpl() {
  }
  
  /**
   * To be called from the Distributed Message without setting EventID
   * @param region
   * @param op
   * @param callbackArgument
   * @param originRemote
   * @param distributedMember
   */
  public ClientRegionEventImpl(LocalRegion region, Operation op, Object callbackArgument,boolean originRemote, DistributedMember distributedMember,ClientProxyMembershipID contx) {
    super(region, op,callbackArgument, originRemote,distributedMember);
    setContext(contx);
  }

  public ClientRegionEventImpl(LocalRegion region, Operation op, Object callbackArgument,boolean originRemote, DistributedMember distributedMember,ClientProxyMembershipID contx,EventID eventId) {
      super(region, op,callbackArgument, originRemote,distributedMember, eventId);
      setContext(contx);
  }


  /**
   * sets The membershipId originating this event
   *  
   */
  protected void setContext(ClientProxyMembershipID contx)
  {
    this.context = contx;
  }

  /**
   * Returns The context originating this event
   * 
   * @return The context originating this event
   */
  @Override
  public ClientProxyMembershipID getContext()
  {
    return this.context;
  }

  @Override
  public String toString()
  {
    String superStr = super.toString();
    StringBuffer buffer = new StringBuffer();
    String str = superStr.substring(0, superStr.length() - 1);
    buffer.append(str).append(";context=").append(getContext()).append(']');
    return buffer.toString();
  }

  @Override
  public int getDSFID() {
    return CLIENT_REGION_EVENT;
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeObject(getContext(), out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    setContext(ClientProxyMembershipID.readCanonicalized(in));
  }
}
