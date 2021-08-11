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
 *
 */

package org.apache.geode.redis.internal.executor.string;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.executor.BaseSetOptions;

/**
 * Class representing different options that can be used with Redis string SET command.
 */
public class SetOptions extends BaseSetOptions {

  private long expirationMillis;
  private boolean keepTTL;

  public SetOptions(Exists exists, long expiration, boolean keepTTL) {
    super(exists);
    this.expirationMillis = expiration;
    this.keepTTL = keepTTL;
  }

  public SetOptions() {}

  public long getExpiration() {
    return expirationMillis;
  }

  public boolean isKeepTTL() {
    return keepTTL;
  }

  @Override
  public int getDSFID() {
    return REDIS_SET_OPTIONS_ID;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeLong(expirationMillis);
    out.writeBoolean(keepTTL);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context) throws IOException {
    super.fromData(in, context);
    expirationMillis = in.readLong();
    keepTTL = in.readBoolean();
  }

}
