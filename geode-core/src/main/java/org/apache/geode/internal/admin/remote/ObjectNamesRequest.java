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

/**
 * A message that is sent to a particular app vm to request all the subregions of a given parent
 * region.
 */
public class ObjectNamesRequest extends RegionAdminRequest implements Cancellable {
  // instance variables
  private transient boolean cancelled;
  private transient ObjectNamesResponse resp;

  /**
   * Returns a <code>ObjectNamesRequest</code> to be sent to the specified recipient.
   */
  public static ObjectNamesRequest create() {
    ObjectNamesRequest m = new ObjectNamesRequest();
    return m;
  }

  public ObjectNamesRequest() {
    friendlyName = "List cached objects";
  }

  /**
   * Must return a proper response to this request.
   *
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    CancellationRegistry.getInstance().registerMessage(this);
    resp = ObjectNamesResponse.create(dm, this.getSender());
    if (cancelled) {
      return null;
    }
    resp.buildNames(this.getRegion(dm.getSystem()));
    if (cancelled) {
      return null;
    }
    CancellationRegistry.getInstance().deregisterMessage(this);
    return resp;
  }

  public synchronized void cancel() {
    cancelled = true;
    if (resp != null) {
      resp.cancel();
    }
  }

  public int getDSFID() {
    return OBJECT_NAMES_REQUEST;
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
    return "ObjectNamesRequest from " + getRecipient() + " region=" + getRegionName();
  }
}
