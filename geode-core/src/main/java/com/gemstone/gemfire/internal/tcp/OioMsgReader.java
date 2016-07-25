/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.tcp;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.Version;

/**
 * A message reader which reads from the socket using
 * the old io.
 *
 */
public class OioMsgReader extends MsgReader {

  public OioMsgReader(Connection conn, Version version) {
    super(conn, version);
  }

  @Override
  public ByteBuffer readAtLeast(int bytes) throws IOException {
    byte[] buffer = new byte[bytes];
    conn.readFully(conn.getSocket().getInputStream(), buffer, bytes);
    return ByteBuffer.wrap(buffer);
  }

}
