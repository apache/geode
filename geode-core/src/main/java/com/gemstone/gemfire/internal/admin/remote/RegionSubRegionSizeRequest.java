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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;

/**
 * Admin request to transfer region info for a member
 * 
 */
public class RegionSubRegionSizeRequest extends AdminRequest implements
    Cancellable {
  public RegionSubRegionSizeRequest() {
    friendlyName = "Refresh the Member's Regions' Statuses";
  }

  /**
   * Returns a <code>RegionSubRegionSizeRequest</code> to be sent to the
   * specified recipient.
   */
  public static RegionSubRegionSizeRequest create() {
    RegionSubRegionSizeRequest m = new RegionSubRegionSizeRequest();
    return m;
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    CancellationRegistry.getInstance().registerMessage(this);

    resp = RegionSubRegionsSizeResponse.create(dm, this.getSender());
    if (cancelled) {
      return null;
    }

    resp.populateSnapshot(dm);

    CancellationRegistry.getInstance().deregisterMessage(this);
    return resp;
  }

  public synchronized void cancel() {
    cancelled = true;
    if (resp != null) {
      resp.cancel();
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  public int getDSFID() {
    return REGION_SUB_SIZE_REQUEST;
  }


  @Override
  public String toString() {
    return "RegionSubRegionSizeRequest sent to " + this.getRecipient()
        + " from " + this.getSender();
  }

  private transient boolean cancelled = false;

  private transient RegionSubRegionsSizeResponse resp = null;
}
