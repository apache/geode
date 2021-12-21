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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ServerLocation;

/**
 * Represents the {@link ServerLocation} of a {@link BucketRegion}
 *
 *
 * @since GemFire 6.5
 */
@SuppressWarnings("serial")
public class BucketServerLocation66 extends ServerLocation {

  private byte version;

  private int bucketId;

  private boolean isPrimary;

  private String[] serverGroups;

  private byte numServerGroups;

  public BucketServerLocation66() {}

  public BucketServerLocation66(int bucketId, int port, String host, boolean isPrimary,
      byte version, String[] groups) {
    super(host, port);
    this.bucketId = bucketId;
    this.isPrimary = isPrimary;
    this.version = version;

    serverGroups = new String[groups.length];
    System.arraycopy(groups, 0, serverGroups, 0, groups.length);
    numServerGroups = (byte) serverGroups.length;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    bucketId = DataSerializer.readInteger(in);
    isPrimary = DataSerializer.readBoolean(in);
    version = DataSerializer.readByte(in);
    numServerGroups = in.readByte();
    serverGroups = new String[numServerGroups];
    if (numServerGroups > 0) {
      for (int i = 0; i < numServerGroups; i++) {
        serverGroups[i] = DataSerializer.readString(in);
      }
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeInteger(bucketId, out);
    DataSerializer.writeBoolean(isPrimary, out);
    DataSerializer.writeByte(version, out);
    out.writeByte(numServerGroups);
    if (numServerGroups > 0) {
      for (String s : serverGroups) {
        DataSerializer.writeString(s, out);
      }
    }
  }

  public boolean isPrimary() {
    return isPrimary;
  }

  public byte getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "BucketServerLocation{bucketId=" + bucketId + ",host=" + getHostName() + ",port="
        + getPort() + ",isPrimary=" + isPrimary + ",version=" + version + "}";
  }

  public int getBucketId() {
    return bucketId;
  }

  public String[] getServerGroups() {
    return serverGroups;
  }
}
