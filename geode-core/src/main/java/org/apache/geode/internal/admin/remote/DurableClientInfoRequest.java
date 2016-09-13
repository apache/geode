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

package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.*; // import
import java.io.*;

/**
 * A message that is sent to a particular distribution manager to get
 * information about a durable client's proxy in the bridge-servers of its
 * current cache.
 * 
 * @since GemFire 5.6
 * 
 */
public class DurableClientInfoRequest extends AdminRequest
{
  static final int HAS_DURABLE_CLIENT_REQUEST = 10;

  static final int IS_PRIMARY_FOR_DURABLE_CLIENT_REQUEST = 11;

  // ///////////////// Instance Fields ////////////////////
  String durableId;

  /** The action to be taken by this request */
  int action = 0;

  /**
   * Returns a <code>DurableClientInfoRequest</code>.
   */
  public static DurableClientInfoRequest create(String id, int operation)
  {
    DurableClientInfoRequest m = new DurableClientInfoRequest();
    m.durableId = id;
    m.action = operation;
    setFriendlyName( m );
    return m;
  }

  public DurableClientInfoRequest() {
    setFriendlyName( this );
  }

  /**
   * Must return a proper response to this request.
   */
  protected AdminResponse createResponse(DistributionManager dm)
  {
    return DurableClientInfoResponse.create(dm, this.getSender(), this);
  }

  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeString(this.durableId, out);
    out.writeInt(this.action);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.durableId = DataSerializer.readString(in);
    this.action = in.readInt();
    setFriendlyName( this );
  }

  public String toString()
  {
    return "DurableClientInfoRequest from " + this.getSender();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return DURABLE_CLIENT_INFO_REQUEST;
  }
  
  private static void setFriendlyName( DurableClientInfoRequest o ) {
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
