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
import org.apache.geode.internal.Assert;

/**
 * A message that is sent to a particular app vm to request all the subregions of a given parent
 * region.
 */
public class RegionSizeRequest extends RegionAdminRequest implements Cancellable {
  private transient boolean cancelled;
  private transient RegionSizeResponse resp;

  /**
   * Returns a <code>ObjectNamesRequest</code> to be sent to the specified recipient.
   */
  public static RegionSizeRequest create() {
    RegionSizeRequest m = new RegionSizeRequest();
    return m;
  }

  /**
   * Must return a proper response to this request.
   *
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    Assert.assertTrue(this.getSender() != null);
    CancellationRegistry.getInstance().registerMessage(this);
    resp = RegionSizeResponse.create(dm, this.getSender());
    if (cancelled) {
      return null;
    }
    resp.calcSize(this.getRegion(dm.getSystem()));
    if (cancelled) {
      return null;
    }
    CancellationRegistry.getInstance().deregisterMessage(this);
    return resp;
  }

  public RegionSizeRequest() {
    friendlyName = "Fetch region size";
  }

  public synchronized void cancel() {
    cancelled = true;
    if (resp != null) {
      resp.cancel();
    }
  }

  public int getDSFID() {
    return REGION_SIZE_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return "RegionSizeRequest from " + getRecipient() + " region=" + getRegionName();
  }
}
