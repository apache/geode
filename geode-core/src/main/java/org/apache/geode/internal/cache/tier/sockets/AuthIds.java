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

package org.apache.geode.internal.cache.tier.sockets;


import org.apache.geode.internal.serialization.ByteArrayDataInput;

public class AuthIds {
  private long connectionId;
  private long uniqueId;

  public AuthIds(byte[] bytes) throws Exception {
    try (ByteArrayDataInput dis = new ByteArrayDataInput(bytes)) {
      if (bytes.length == 8) {
        // only connectionid
        connectionId = dis.readLong();
      } else if (bytes.length == 16) {
        // first connectionId and then uniqueID
        connectionId = dis.readLong();
        uniqueId = dis.readLong();
      } else {
        throw new Exception("Auth ids are not in right form");
      }
    }
  }


  public long getConnectionId() {
    return connectionId;
  }

  public long getUniqueId() {
    return this.uniqueId;
  }
}
