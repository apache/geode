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

import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

/**
 * 
 *
 */
public class RemoteLocatorRequest implements DataSerializableFixedID {
  private int distributedSystemId;

  public RemoteLocatorRequest() {
    super();
  }

  public RemoteLocatorRequest(int dsId, String serverGroup) {
    this.distributedSystemId = dsId;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.distributedSystemId = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.distributedSystemId);
  }

  public int getDsId() {
    return this.distributedSystemId;
  }

  public int getDSFID() {
    return DataSerializableFixedID.REMOTE_LOCATOR_REQUEST;
  }

  @Override
  public String toString() {
    return "RemoteLocatorRequest{dsName=" + distributedSystemId + "}";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
