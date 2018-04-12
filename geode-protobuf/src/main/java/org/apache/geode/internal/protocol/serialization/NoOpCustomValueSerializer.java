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
package org.apache.geode.internal.protocol.serialization;

import com.google.protobuf.ByteString;

import org.apache.geode.cache.Cache;
import org.apache.geode.protocol.serialization.ValueSerializer;

public class NoOpCustomValueSerializer implements ValueSerializer {
  @Override
  public void init(Cache cache) {
    // Do nothing
  }

  @Override
  public String getID() {
    throw new IllegalStateException("The no op serializer is not loaded using the service loader");
  }

  @Override
  public ByteString serialize(Object object) {
    return null;
  }

  @Override
  public Object deserialize(ByteString bytes) {
    throw new IllegalStateException(
        "Received a custom value, but did not set the valueFormat in the handshakeMessage");
  }
}
