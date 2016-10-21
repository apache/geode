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

import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * <code>ClientHandShake</code> represents a handshake from the client.
 * 
 * @since GemFire 5.7
 */
public interface ClientHandShake {
  public boolean isOK();

  public byte getCode();

  public ClientProxyMembershipID getMembership();

  public int getClientReadTimeout();

  public Version getVersion();

  public void accept(OutputStream out, InputStream in, byte epType, int qSize,
      byte communicationMode, Principal principal) throws IOException;
}
