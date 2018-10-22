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
public class BucketServerLocation extends ServerLocation {

  private byte version;

  private int bucketId;

  private boolean isPrimary;

  public BucketServerLocation() {}

  public BucketServerLocation(int bucketId, int port, String host, boolean isPrimary,
      byte version) {
    super(host, port);
    this.bucketId = bucketId;
    this.isPrimary = isPrimary;
    this.version = version;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketId = DataSerializer.readInteger(in);
    this.isPrimary = DataSerializer.readBoolean(in);
    this.version = DataSerializer.readByte(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeInteger(this.bucketId, out);
    DataSerializer.writeBoolean(this.isPrimary, out);
    DataSerializer.writeByte(this.version, out);
  }

  public boolean isPrimary() {
    return this.isPrimary;
  }

  public byte getVersion() {
    return this.version;
  }

  public int getBucketId() {
    return bucketId;
  }
}
