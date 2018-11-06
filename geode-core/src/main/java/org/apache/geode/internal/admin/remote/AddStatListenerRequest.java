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
import org.apache.geode.internal.admin.Stat;
import org.apache.geode.internal.admin.StatResource;

/**
 * A message that is sent to a particular distribution manager to add a statistic listener.
 */
public class AddStatListenerRequest extends AdminRequest {
  // instance variables
  private long resourceId;
  private String statName;

  /**
   * Returns a <code>AddStatListenerRequest</code> to be sent to the specified recipient.
   */
  public static AddStatListenerRequest create(StatResource observedResource, Stat observedStat) {
    AddStatListenerRequest m = new AddStatListenerRequest();
    m.resourceId = observedResource.getResourceUniqueID();
    m.statName = observedStat.getName();
    return m;
  }

  public AddStatListenerRequest() {
    friendlyName =
        "Add statistic resource listener";
  }

  /**
   * Must return a proper response to this request.
   *
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return AddStatListenerResponse.create(dm, this.getSender(), this.resourceId, this.statName);
  }

  public int getDSFID() {
    return ADD_STAT_LISTENER_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(this.resourceId);
    out.writeUTF(this.statName);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.resourceId = in.readLong();
    this.statName = in.readUTF();
  }

  @Override
  public String toString() {
    return "AddStatListenerRequest from " + this.getRecipient() + " for " + this.resourceId + " "
        + this.statName;
  }
}
