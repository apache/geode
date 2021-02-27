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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

public class SocketMessageWriterTest {

  @Test
  public void writeHandshakeMessageWithClientVersionNull() throws IOException {
    final SocketMessageWriter writer = new SocketMessageWriter();
    // can't mock DataOutputStream
    final DataOutputStream outputStream = new DataOutputStream(new ByteArrayOutputStream());
    writer.writeHandshakeMessage(outputStream, (byte) 1, "", null, (byte) 0, 0, null, null);
  }
}
