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

package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a particular distribution manager to get information about a durable
 * client's proxy in the bridge-servers of its current cache.
 *
 * @since GemFire 5.6
 *
 */
public class DurableClientInfoRequest extends AdminRequest {
  static final int HAS_DURABLE_CLIENT_REQUEST = 10;

  static final int IS_PRIMARY_FOR_DURABLE_CLIENT_REQUEST = 11;

  // ///////////////// Instance Fields ////////////////////
  String durableId;

  /** The action to be taken by this request */
  int action = 0;

  /**
   * Returns a <code>DurableClientInfoRequest</code>.
   */
  public static DurableClientInfoRequest create(String id, int operation) {
    DurableClientInfoRequest m = new DurableClientInfoRequest();
    m.durableId = id;
    m.action = operation;
    setFriendlyName(m);
    return m;
  }

  public DurableClientInfoRequest() {
    setFriendlyName(this);
  }

  /**
   * Must return a proper response to this request.
   *
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return DurableClientInfoResponse.create(dm, getSender(), this);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeString(durableId, out);
    out.writeInt(action);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    durableId = DataSerializer.readString(in);
    action = in.readInt();
    setFriendlyName(this);
  }

  public String toString() {
    return "DurableClientInfoRequest from " + getSender();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.serialization.DataSerializableFixedID#getDSFID()
   */
  @Override
  public int getDSFID() {
    return DURABLE_CLIENT_INFO_REQUEST;
  }

  private static void setFriendlyName(DurableClientInfoRequest o) {
    // TODO MGh - these should be localized?
    switch (o.action) {
      case HAS_DURABLE_CLIENT_REQUEST:
        o.friendlyName = "Find whether the server has durable-queue for this client";
        break;
      case IS_PRIMARY_FOR_DURABLE_CLIENT_REQUEST:
        o.friendlyName = "Find whether the server is primary for this durable-client";
        break;
      default:
        o.friendlyName = "Unknown operation " + o.action;
        break;
    }
  }
}
