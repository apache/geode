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
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent in response to a {@link VersionInfoRequest}.
 *
 * @since GemFire 3.5
 */
public class VersionInfoResponse extends AdminResponse {
  // instance variables
  private String verInfo;


  /**
   * Returns a <code>VersionInfoResponse</code> that will be returned to the specified recipient.
   */
  public static VersionInfoResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    VersionInfoResponse m = new VersionInfoResponse();
    m.setRecipient(recipient);
    m.verInfo = GemFireVersion.asString();
    return m;
  }

  public String getVersionInfo() {
    return this.verInfo;
  }

  @Override
  public int getDSFID() {
    return VERSION_INFO_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeString(this.verInfo, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.verInfo = DataSerializer.readString(in);
  }

  @Override
  public String toString() {
    return "VersionInfoResponse from " + this.getSender();
  }
}
