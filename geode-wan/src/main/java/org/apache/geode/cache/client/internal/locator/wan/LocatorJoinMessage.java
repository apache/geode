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
package org.apache.geode.cache.client.internal.locator.wan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.cache.client.internal.locator.ServerLocationRequest;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class LocatorJoinMessage extends ServerLocationRequest {

  private DistributionLocatorId locator;

  private int distributedSystemId;

  private DistributionLocatorId sourceLocator;

  public LocatorJoinMessage() {
    super();
  }

  public LocatorJoinMessage(int distributedSystemId, DistributionLocatorId locator,
      DistributionLocatorId sourceLocator, String serverGroup) {
    super(serverGroup);
    this.locator = locator;
    this.distributedSystemId = distributedSystemId;
    this.sourceLocator = sourceLocator;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    locator = context.getDeserializer().readObject(in);
    distributedSystemId = in.readInt();
    sourceLocator = context.getDeserializer().readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    context.getSerializer().writeObject(locator, out);
    out.writeInt(distributedSystemId);
    context.getSerializer().writeObject(sourceLocator, out);
  }

  public DistributionLocatorId getLocator() {
    return locator;
  }

  public int getDistributedSystemId() {
    return distributedSystemId;
  }

  public DistributionLocatorId getSourceLocator() {
    return sourceLocator;
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.LOCATOR_JOIN_MESSAGE;
  }

  @Override
  public String toString() {
    return "LocatorJoinMessage{distributedSystemId=" + distributedSystemId + " locators=" + locator
        + " Source Locator : " + sourceLocator + "}";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof LocatorJoinMessage)) {
      return false;
    }
    LocatorJoinMessage myObject = (LocatorJoinMessage) obj;
    return (distributedSystemId == myObject.getDistributedSystemId())
        && locator.equals(myObject.getLocator());
  }

  @Override
  public int hashCode() {
    // it is sufficient for all messages having the same locator to hash to the same bucket
    if (locator == null) {
      return 0;
    } else {
      return locator.hashCode();
    }
  }


}
