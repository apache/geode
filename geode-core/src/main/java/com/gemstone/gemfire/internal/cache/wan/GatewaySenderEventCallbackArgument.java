/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.cache.wan;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.WrappedCallbackArgument;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class <code>GatewayEventCallbackArgument</code> is a wrapper on a callback
 * arg plus the id of the <code>GatewaySender</code> making the request. It is
 * created during a batch update request so that the hub id is passed to the
 * <code>GatewayReceiver</code> so that events are not re-distributed back to
 * the originating <code>GatewayReceiver</code>, but are distributed to other
 * <code>GatewayReceiver</code>s. The original callback arg is wrapped by this
 * one and replaced in the event sent to CacheListener, CacheWriter and
 * CacheLoader.
 * <p>
 * This class used to be in package <code>com.gemstone.gemfire.util</code>.
 * 
 * @since GemFire 7.0
 */
public final class GatewaySenderEventCallbackArgument extends
WrappedCallbackArgument implements DataSerializableFixedID {
  /**
   * The id of the originating <code>GatewayReceiver</code> making the request
   */
  private int originatingDSId = GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;

  /**
   * The set of <code>GatewayReceiver</code> s to which the event has been sent. This
   * set keeps track of the <code>GatewayReceiver</code> s to which the event has been
   * sent so that downstream <code>GatewayReceiver</code> s don't resend the event to
   * the same <code>GatewayReceiver</code>s.
   */
  private Set<Integer> receipientDSIds;

  /**
   * No arg constructor for DataSerializable.
   */
  public GatewaySenderEventCallbackArgument() {
  }

  public GatewaySenderEventCallbackArgument(Object originalCallbackArg) {
    super(originalCallbackArg);
  }
  
  /**
   * Constructor.
   * @param geca
   *          The original callback argument set by the caller or null if there
   *          was not callback arg
   */
  public GatewaySenderEventCallbackArgument(GatewaySenderEventCallbackArgument geca) {
    super(geca.getOriginalCallbackArg());
    //  _originalEventId = geca._originalEventId;
    originatingDSId = geca.originatingDSId;
    if (geca.receipientDSIds != null) {
      receipientDSIds = new HashSet<Integer>(geca.receipientDSIds);
    }
  }
  
  /**
   * Constructor.
   * 
   * @param originalCallbackArg
   *          The original callback argument set by the caller or null if there
   *          was not callback arg
   * @param originatingDSId
   *          The id of the originating <code>GatewayReceiver</code> making the
   *          request
   * @param originalReceivers
   *          The list of <code>Gateway</code> s to which the event has been
   *          originally sent
   * @param serializeCBArg
   *          boolean indicating whether to serialize callback argument
   * 
   */
  public GatewaySenderEventCallbackArgument(Object originalCallbackArg,
      int originatingDSId, List<Integer> originalReceivers,
      boolean serializeCBArg) {
    super(originalCallbackArg, serializeCBArg);
    this.originatingDSId = originatingDSId;
    initializeReceipientDSIds(originalReceivers);
  }

  /**
   * Returns the id of the originating <code>GatewayReceiver</code> making the
   * request.
   * 
   * @return the id of the originating <code>GatewayReceiver</code> making the
   *         request
   */
  public int getOriginatingDSId() {
    return this.originatingDSId;
  }

  /**
   * Sets the originating <code>SenderId</code> id
   * 
   * @param originatingDSId
   *          The originating <code>SenderId</code> id
   */
  public void setOriginatingDSId(int originatingDSId) {
    this.originatingDSId = originatingDSId;
  }

  /**
   * Returns the list of <code>Gateway</code> s to which the event has been
   * sent.
   * 
   * @return the list of <code>Gateway</code> s to which the event has been sent
   */
  public Set<Integer> getRecipientDSIds() {
    return this.receipientDSIds;
  }

  /**
   * Initialize the original set of recipient <code>Gateway</code>s.
   * 
   * @param originalGatewaysReceivers
   *          The original recipient <code>Gateway</code>s.
   */
  public void initializeReceipientDSIds(List<Integer> originalGatewaysReceivers) {
    this.receipientDSIds = new HashSet<Integer>();
    for (Integer id:originalGatewaysReceivers) {
      this.receipientDSIds.add(id);
    }
  }

  public int getDSFID() {
    return GATEWAY_SENDER_EVENT_CALLBACK_ARGUMENT;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeInteger(this.originatingDSId, out);
    if (this.receipientDSIds != null) {
      out.writeInt(this.receipientDSIds.size());
      for (Integer gateway : this.receipientDSIds) {
        out.writeInt(gateway);
      }
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.originatingDSId = DataSerializer.readInteger(in);
    this.receipientDSIds = new HashSet<Integer>();
    int numberOfRecipientGateways = in.readInt();
    for (int i = 0; i < numberOfRecipientGateways; i++) {
      this.receipientDSIds.add(in.readInt());
    }
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("GatewaySenderEventCallbackArgument [")
        .append("originalCallbackArg=").append(getOriginalCallbackArg())
        .append(";originatingSenderId=").append(this.originatingDSId)
        .append(";recipientGatewayReceivers=").append(this.receipientDSIds)
        .append("]");
    return buffer.toString();
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
