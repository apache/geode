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
package org.apache.geode.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;


public class LocatorListRequest extends ServerLocationRequest {

  private boolean requestInternalAddress;

  public LocatorListRequest(boolean requestInternal) {
    super();
    this.requestInternalAddress = requestInternal;
  }

  public LocatorListRequest() {
    super();
    this.requestInternalAddress = false;
  }

  public boolean getRequestInternalAddress() {
    return requestInternalAddress;
  }

  @Override
  public String toString() {
    return "LocatorListRequest{group=" + getServerGroup() + ",requestInternalAddress="
        + getRequestInternalAddress() + "}";
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.LOCATOR_LIST_REQUEST;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    if (StaticSerialization.getVersionForDataStream(in).isNotOlderThan(KnownVersion.GEODE_1_14_0)) {
      requestInternalAddress = DataSerializer.readPrimitiveBoolean(in);
    } else {
      requestInternalAddress = false;
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writePrimitiveBoolean(requestInternalAddress, out);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
