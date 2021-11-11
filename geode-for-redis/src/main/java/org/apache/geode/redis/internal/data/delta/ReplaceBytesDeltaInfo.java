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

package org.apache.geode.redis.internal.data.delta;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;

/**
 * This delta info simply replaces a RedisData's current
 * byte array with another one.
 */
public class ReplaceBytesDeltaInfo implements DeltaInfo {
  private final byte[] replaceBytes;

  public ReplaceBytesDeltaInfo(byte[] value) {
    replaceBytes = value;
  }

  public void serializeTo(DataOutput out) throws IOException {
    DataSerializer.writeEnum(DeltaType.REPLACE_BYTES, out);
    DataSerializer.writeByteArray(replaceBytes, out);
  }
}
