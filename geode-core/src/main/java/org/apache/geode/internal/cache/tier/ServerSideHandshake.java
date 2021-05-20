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

package org.apache.geode.internal.cache.tier;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Principal;

import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;

/**
 * <code>ClientHandShake</code> represents a handshake from the client.
 *
 */
public interface ServerSideHandshake {
  boolean isOK();

  ClientProxyMembershipID getMembershipId();

  int getClientReadTimeout();

  KnownVersion getVersion();

  Object verifyCredentials() throws AuthenticationRequiredException, AuthenticationFailedException;

  void setClientReadTimeout(int clientReadTimeout);

  @Deprecated
  Encryptor getEncryptor();

  void handshakeWithClient(OutputStream out, InputStream in, byte endpointType, int queueSize,
      CommunicationMode communicationMode, Principal principal) throws IOException;
}
