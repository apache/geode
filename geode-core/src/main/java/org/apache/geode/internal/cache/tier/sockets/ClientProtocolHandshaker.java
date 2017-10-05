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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.security.server.Authenticator;

/**
 * ClientProtocolHandshaker provides the handshake for the protocol implemented by
 * {@link ClientProtocolMessageHandler}. It is responsible for checking version compatibility
 * information as well as authentication mode.
 */
public interface ClientProtocolHandshaker {
  /**
   * Read a handshake message from the input stream and write a response to output.
   *
   * @return an authenticator from those available. These are currently only set in one place, see
   *         {@link ServerConnectionFactory}.
   */
  Authenticator handshake(InputStream inputStream, OutputStream outputStream)
      throws IOException, IncompatibleVersionException;

  /**
   * @return false until handshake is complete, then true afterwards.
   */
  boolean handshakeComplete();
}
