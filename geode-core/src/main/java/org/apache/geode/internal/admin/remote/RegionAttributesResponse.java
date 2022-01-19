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
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Responds to {@link RegionAttributesResponse}.
 */
public class RegionAttributesResponse extends AdminResponse {
  // instance variables
  private RemoteRegionAttributes attributes;

  /**
   * Returns a <code>RegionAttributesResponse</code> that will be returned to the specified
   * recipient. The message will contains a copy of the local manager's system config.
   */
  public static RegionAttributesResponse create(DistributionManager dm,
      InternalDistributedMember recipient, Region r) {
    RegionAttributesResponse m = new RegionAttributesResponse();
    m.setRecipient(recipient);
    m.attributes = new RemoteRegionAttributes(r.getAttributes());
    return m;
  }

  // instance methods
  public RegionAttributes getRegionAttributes() {
    return attributes;
  }

  @Override
  public int getDSFID() {
    return REGION_ATTRIBUTES_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(attributes, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    attributes = DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "RegionAttributesResponse from " + getRecipient();
  }
}
