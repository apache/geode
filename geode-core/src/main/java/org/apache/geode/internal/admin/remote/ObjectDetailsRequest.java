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

/**
 * A message that is sent to a particular app vm to request the value, stats, and attributes of a
 * given region entry.
 */
public class ObjectDetailsRequest extends RegionAdminRequest implements Cancellable {
  // instance variables
  private Object objName;
  private int inspectionType;
  private transient boolean cancelled;
  private ObjectDetailsResponse resp;

  /**
   * Returns a <code>ObjectValueRequest</code> to be sent to the specified recipient.
   */
  public static ObjectDetailsRequest create(Object objName, int inspectionType) {
    ObjectDetailsRequest m = new ObjectDetailsRequest();
    m.objName = objName;
    m.inspectionType = inspectionType;
    return m;
  }

  public ObjectDetailsRequest() {
    friendlyName = "Inspect cached object";
  }

  public synchronized void cancel() {
    cancelled = true;
    if (resp != null) {
      resp.cancel();
    }
  }

  /**
   * Must return a proper response to this request.
   *
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    CancellationRegistry.getInstance().registerMessage(this);
    resp = ObjectDetailsResponse.create(dm, this.getSender());
    if (cancelled) {
      return null;
    }
    resp.buildDetails(this.getRegion(dm.getSystem()), this.objName, this.inspectionType);
    if (cancelled) {
      return null;
    }
    CancellationRegistry.getInstance().deregisterMessage(this);
    return resp;
  }

  public int getDSFID() {
    return OBJECT_DETAILS_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.objName, out);
    out.writeInt(inspectionType);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.objName = DataSerializer.readObject(in);
    this.inspectionType = in.readInt();
  }

  @Override
  public String toString() {
    return "ObjectDetailsRequest from " + getRecipient() + " region=" + getRegionName() + " object="
        + this.objName;
  }
}
