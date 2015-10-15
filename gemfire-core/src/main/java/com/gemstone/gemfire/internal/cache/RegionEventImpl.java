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
import java.lang.ClassNotFoundException;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo.FilterInfo;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * Implementation of a region event
 * 
 * @author Sudhir Menon
 *  
 */
// must be public for DataSerializable to work
public class RegionEventImpl 
  implements RegionEvent, InternalCacheEvent, Cloneable, DataSerializableFixedID
{

  transient LocalRegion region;

  String regionPath;

  Object callbackArgument = null;

  boolean originRemote = false;

  Operation op;

  DistributedMember distributedMember;

  private EventID eventId = null;

  /* To store the operation/modification type */
  private EnumListenerEvent eventType;

  private FilterInfo filterInfo;
  
  private VersionTag<?> versionTag;
  
  public RegionEventImpl() {
    // public zero-argument constructor required by DataSerializable
  }

  /**
   * Constructor which does not generate EventID
   * 
   * @param region
   * @param op
   * @param callbackArgument
   * @param originRemote
   * @param distributedMember
   */
  public RegionEventImpl(Region region, Operation op, Object callbackArgument,
      boolean originRemote, DistributedMember distributedMember) {
    this(region, op, callbackArgument, originRemote, distributedMember, false);
  }

  public RegionEventImpl(Region region, Operation op, Object callbackArgument,
      boolean originRemote, DistributedMember distributedMember,
      boolean generateEventID) {
    this.region = (LocalRegion)region;
    this.regionPath = region.getFullPath();
    this.op = op;
    this.callbackArgument = callbackArgument;
    this.originRemote = originRemote;
    this.distributedMember = distributedMember;
    DistributedSystem sys = ((LocalRegion)region).getCache().getDistributedSystem();
    if (generateEventID) {
      this.eventId = new EventID(sys);
    }
  }

  /**
   * Constructor which uses the eventID passed
   * 
   * @param region
   * @param op
   * @param callbackArgument
   * @param originRemote
   * @param distributedMember
   * @param eventID
   *          EventID used to create the RegionEvent
   */
  public RegionEventImpl(Region region, Operation op, Object callbackArgument,
      boolean originRemote, DistributedMember distributedMember, EventID eventID) {
    this.region = (LocalRegion)region;
    this.regionPath = region.getFullPath();
    this.op = op;
    this.callbackArgument = callbackArgument;
    this.originRemote = originRemote;
    this.distributedMember = distributedMember;
    //TODO:ASIF: Remove this Assert from production env.
    Assert.assertTrue(eventID != null);
    this.eventId = eventID;
  }

  /**
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.cache.CacheEvent#getRegion()
   */
  public Region getRegion()
  {
    return region;
  }

  public Operation getOperation()
  {
    return this.op;
  }

  public void setOperation(Operation newOp)
  {
    this.op = newOp;
  }
  
  public void setVersionTag(VersionTag<?>tag) {
    this.versionTag = tag;
  }
  
  public VersionTag getVersionTag() {
    return this.versionTag;
  }

  /**
   * @see com.gemstone.gemfire.cache.CacheEvent#getCallbackArgument()
   */
  public Object getCallbackArgument()
  {
    Object result = this.callbackArgument;
    while (result instanceof WrappedCallbackArgument) {
      WrappedCallbackArgument wca = (WrappedCallbackArgument)result;
      result = wca.getOriginalCallbackArg();
    }
    if (result == Token.NOT_AVAILABLE) {
      result = null;
    }
    return result;
  }
  public boolean isCallbackArgumentAvailable() {
    return this.callbackArgument != Token.NOT_AVAILABLE;
  }

  /**
   * Returns the value of the RegionEventImpl field.
   * This is for internal use only. Customers should always call
   * {@link #getCallbackArgument}
   * @since 5.7 
   */
  public Object getRawCallbackArgument() {
    return this.callbackArgument;
  }

  /**
   * @see com.gemstone.gemfire.cache.CacheEvent#isOriginRemote()
   */
  public boolean isOriginRemote()
  {
    return originRemote;
  }

  public DistributedMember getDistributedMember()
  {
    return this.distributedMember;
  }

  public boolean isGenerateCallbacks()
  {
    return true;
  }

  /**
   * @see com.gemstone.gemfire.cache.CacheEvent#isExpiration()
   */
  public boolean isExpiration()
  {
    return this.op.isExpiration();
  }

  /**
   * @see com.gemstone.gemfire.cache.CacheEvent#isDistributed()
   */
  public boolean isDistributed()
  {
    return this.op.isDistributed();
  }

  @Override
  public Object clone()
  {
    try {
      return super.clone();
    }
    catch (CloneNotSupportedException e) {
      throw new Error(LocalizedStrings.RegionEventImpl_CLONE_IS_SUPPORTED.toLocalizedString());
    }
  }

  public int getDSFID() {
    return REGION_EVENT;
  }

  /**
   * Writes the contents of this message to the given output.
   */
  public void toData(DataOutput out) throws IOException
  {
    DataSerializer.writeString(this.regionPath, out);
    DataSerializer.writeObject(this.callbackArgument, out);
    out.writeByte(this.op.ordinal);
    out.writeBoolean(this.originRemote);
    InternalDataSerializer.invokeToData(((InternalDistributedMember)this.distributedMember), out);
  }

  /**
   * Reads the contents of this message from the given input.
   */
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    this.regionPath = DataSerializer.readString(in);
    this.callbackArgument = DataSerializer.readObject(in);
    this.op = Operation.fromOrdinal(in.readByte());
    this.originRemote = in.readBoolean();
    this.distributedMember = DSFIDFactory.readInternalDistributedMember(in);
  }

  public boolean isReinitializing()
  {
    return this.op == Operation.REGION_LOAD_SNAPSHOT
        || this.op == Operation.REGION_REINITIALIZE;
  }

  public EventID getEventId()
  {
    return this.eventId;
  }
  
  public void setEventID(EventID eventID) {
    this.eventId = eventID;
  }

  public void setCallbackArgument(Object callbackArgument)
  {
    this.callbackArgument = callbackArgument;
  }

  /**
   * Returns the Operation type.
   * @return eventType
   */
  public EnumListenerEvent getEventType() {
    return this.eventType; 
  }

  /**
   * Sets the operation type.
   * @param eventType
   */
  public void setEventType(EnumListenerEvent eventType) {
    this.eventType = eventType; 
  }
  
  public ClientProxyMembershipID getContext() {
    // regular region events do not have a context - see ClientRegionEventImpl
    return null;
  }
  
  /**
   * sets the routing information for cache clients
   */
  public void setLocalFilterInfo(FilterInfo info) {
    this.filterInfo = info;
  }
  
  /**
   * retrieves the routing information for cache clients in this VM
   */
  public FilterInfo getLocalFilterInfo() {
    return this.filterInfo;
  }

  public boolean isBridgeEvent() {
    return hasClientOrigin();
  }
  public boolean hasClientOrigin() {
    return this.getContext() != null;
  }

  String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length()+1);
  }

  @Override
  public String toString() {
    return new StringBuffer()
      .append(getShortClassName())
      .append("[")
      .append("region=")
      .append(getRegion())
      .append(";op=")
      .append(getOperation())
      .append(";isReinitializing=")
      .append(isReinitializing())
      .append(";callbackArg=")
      .append(getCallbackArgument())
      .append(";originRemote=")
      .append(isOriginRemote())
      .append(";originMember=")
      .append(getDistributedMember())
      .append(";tag=")
      .append(this.versionTag)
      .append("]")
      .toString();
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
