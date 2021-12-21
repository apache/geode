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

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Admin request to transfer region info for a member
 *
 */
public class RegionSubRegionSizeRequest extends AdminRequest implements Cancellable {
  public RegionSubRegionSizeRequest() {
    friendlyName = "Refresh the Member's Regions' Statuses";
  }

  /**
   * Returns a <code>RegionSubRegionSizeRequest</code> to be sent to the specified recipient.
   */
  public static RegionSubRegionSizeRequest create() {
    RegionSubRegionSizeRequest m = new RegionSubRegionSizeRequest();
    return m;
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    CancellationRegistry.getInstance().registerMessage(this);

    resp = RegionSubRegionsSizeResponse.create(dm, getSender());
    if (cancelled) {
      return null;
    }

    resp.populateSnapshot(dm);

    CancellationRegistry.getInstance().deregisterMessage(this);
    return resp;
  }

  @Override
  public synchronized void cancel() {
    cancelled = true;
    if (resp != null) {
      resp.cancel();
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  @Override
  public int getDSFID() {
    return REGION_SUB_SIZE_REQUEST;
  }


  @Override
  public String toString() {
    return "RegionSubRegionSizeRequest sent to " + getRecipient() + " from "
        + getSender();
  }

  private transient boolean cancelled = false;

  private transient RegionSubRegionsSizeResponse resp = null;
}
