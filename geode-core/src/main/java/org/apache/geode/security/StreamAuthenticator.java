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
package org.apache.geode.security;

import org.apache.geode.security.SecurityManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Implementers of this interface do some message passing over a socket to authenticate a client,
 * then hand off the connection to the protocol that will talk on the socket.
 *
 * If authentication fails, an implementor may continue to wait for another valid authentication
 * exchange.
 */
public interface StreamAuthenticator {
  /**
   *
   * @param inputStream to read auth messages from.
   * @param outputStream to send messages to.
   * @param securityManager can be used for validating credentials against.
   * @throws IOException if EOF or if invalid input is received.
   */
  void receiveMessage(InputStream inputStream, OutputStream outputStream,
      SecurityManager securityManager) throws IOException;

  /**
   * Until authentication is complete, isAuthenticated() must return false, and the socket will
   * always be passed to the StreamAuthenticator. Once authentication succeeds, calls to this
   * function must always return true.
   */
  boolean isAuthenticated();

  /**
   * @return a unique identifier for this particular implementation (NOOP, PASSTHROUGH, etc.)
   */
  String implementationID();
}
