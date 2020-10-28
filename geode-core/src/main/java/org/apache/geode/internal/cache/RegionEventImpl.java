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
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Implementation of a region event
 *
 *
 */
// must be public for DataSerializable to work
public class RegionEventImpl
    implements RegionEvent, InternalCacheEvent, Cloneable, DataSerializableFixedID {

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
   */
  public RegionEventImpl(Region region, Operation op, Object callbackArgument, boolean originRemote,
      DistributedMember distributedMember) {
    this(region, op, callbackArgument, originRemote, distributedMember, false);
  }

  public RegionEventImpl(Region region, Operation op, Object callbackArgument, boolean originRemote,
      DistributedMember distributedMember, boolean generateEventID) {
    this.region = (LocalRegion) region;
    this.regionPath = region.getFullPath();
    this.op = op;
    this.callbackArgument = callbackArgument;
    this.originRemote = originRemote;
    this.distributedMember = distributedMember;
    DistributedSystem sys = ((LocalRegion) region).getCache().getDistributedSystem();
    if (generateEventID) {
      this.eventId = new EventID(sys);
    }
  }

  /**
   * Constructor which uses the eventID passed
   *
   * @param eventID EventID used to create the RegionEvent
   */
  public RegionEventImpl(Region region, Operation op, Object callbackArgument, boolean originRemote,
      DistributedMember distributedMember, EventID eventID) {
    this.region = (LocalRegion) region;
    this.regionPath = region.getFullPath();
    this.op = op;
    this.callbackArgument = callbackArgument;
    this.originRemote = originRemote;
    this.distributedMember = distributedMember;
    this.eventId = eventID;
  }

  /**
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.CacheEvent#getRegion()
   */
  @Override
  public Region getRegion() {
    return region;
  }

  public void setRegion(LocalRegion region) {
    this.region = region;
    this.distributedMember = region.getMyId();
  }

  @Override
  public Operation getOperation() {
    return this.op;
  }

  public void setOperation(Operation newOp) {
    this.op = newOp;
  }

  public void setVersionTag(VersionTag<?> tag) {
    this.versionTag = tag;
  }

  @Override
  public VersionTag getVersionTag() {
    return this.versionTag;
  }

  /**
   * @see org.apache.geode.cache.CacheEvent#getCallbackArgument()
   */
  @Override
  public Object getCallbackArgument() {
    Object result = this.callbackArgument;
    while (result instanceof WrappedCallbackArgument) {
      WrappedCallbackArgument wca = (WrappedCallbackArgument) result;
      result = wca.getOriginalCallbackArg();
    }
    if (result == Token.NOT_AVAILABLE) {
      result = null;
    }
    return result;
  }

  @Override
  public boolean isCallbackArgumentAvailable() {
    return this.callbackArgument != Token.NOT_AVAILABLE;
  }

  /**
   * Returns the value of the RegionEventImpl field. This is for internal use only. Customers should
   * always call {@link #getCallbackArgument}
   *
   * @since GemFire 5.7
   */
  public Object getRawCallbackArgument() {
    return this.callbackArgument;
  }

  /**
   * @see org.apache.geode.cache.CacheEvent#isOriginRemote()
   */
  @Override
  public boolean isOriginRemote() {
    return originRemote;
  }

  @Override
  public DistributedMember getDistributedMember() {
    return this.distributedMember;
  }

  @Override
  public boolean isGenerateCallbacks() {
    return true;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new Error("clone IS supported");
    }
  }

  @Override
  public int getDSFID() {
    return REGION_EVENT;
  }

  /**
   * Writes the contents of this message to the given output.
   */
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeString(this.regionPath, out);
    context.getSerializer().writeObject(this.callbackArgument, out);
    out.writeByte(this.op.ordinal);
    out.writeBoolean(this.originRemote);
    InternalDataSerializer.invokeToData(((InternalDistributedMember) this.distributedMember), out);
  }

  /**
   * Reads the contents of this message from the given input.
   */
  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.regionPath = DataSerializer.readString(in);
    this.callbackArgument = context.getDeserializer().readObject(in);
    this.op = Operation.fromOrdinal(in.readByte());
    this.originRemote = in.readBoolean();
    this.distributedMember = DSFIDFactory.readInternalDistributedMember(in);
  }

  @Override
  public boolean isReinitializing() {
    return this.op == Operation.REGION_LOAD_SNAPSHOT || this.op == Operation.REGION_REINITIALIZE;
  }

  @Override
  public EventID getEventId() {
    return this.eventId;
  }

  public void setEventID(EventID eventID) {
    this.eventId = eventID;
  }

  public void setCallbackArgument(Object callbackArgument) {
    this.callbackArgument = callbackArgument;
  }

  /**
   * Returns the Operation type.
   *
   */
  @Override
  public EnumListenerEvent getEventType() {
    return this.eventType;
  }

  /**
   * Sets the operation type.
   *
   */
  @Override
  public void setEventType(EnumListenerEvent eventType) {
    this.eventType = eventType;
  }

  @Override
  public ClientProxyMembershipID getContext() {
    // regular region events do not have a context - see ClientRegionEventImpl
    return null;
  }

  /**
   * sets the routing information for cache clients
   */
  @Override
  public void setLocalFilterInfo(FilterInfo info) {
    this.filterInfo = info;
  }

  /**
   * retrieves the routing information for cache clients in this VM
   */
  @Override
  public FilterInfo getLocalFilterInfo() {
    return this.filterInfo;
  }

  @Override
  public boolean isBridgeEvent() {
    return hasClientOrigin();
  }

  @Override
  public boolean hasClientOrigin() {
    return this.getContext() != null;
  }

  String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length() + 1);
  }

  @Override
  public String toString() {
    return new StringBuffer().append(getShortClassName()).append("[").append("region=")
        .append(getRegion()).append(";op=").append(getOperation()).append(";isReinitializing=")
        .append(isReinitializing()).append(";callbackArg=").append(getCallbackArgument())
        .append(";originRemote=").append(isOriginRemote()).append(";originMember=")
        .append(getDistributedMember()).append(";tag=").append(this.versionTag).append("]")
        .toString();
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
