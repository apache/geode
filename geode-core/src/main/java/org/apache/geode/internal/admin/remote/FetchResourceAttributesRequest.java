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

public class FetchResourceAttributesRequest extends AdminRequest {

  // instance variables
  private long resourceUniqueId;

  public static FetchResourceAttributesRequest create(long id) {
    FetchResourceAttributesRequest m = new FetchResourceAttributesRequest();
    m.resourceUniqueId = id;
    return m;
  }

  public FetchResourceAttributesRequest() {
    friendlyName = "Fetch statistics for resource";
  }

  @Override
  public AdminResponse createResponse(DistributionManager dm) {
    return FetchResourceAttributesResponse.create(dm, this.getSender(), resourceUniqueId);
  }

  public int getDSFID() {
    return FETCH_RESOURCE_ATTRIBUTES_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(resourceUniqueId);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    resourceUniqueId = in.readLong();
  }

  @Override
  public String toString() {
    return String.format("Fetch statistics for %s",
        this.getRecipient());
  }

}
