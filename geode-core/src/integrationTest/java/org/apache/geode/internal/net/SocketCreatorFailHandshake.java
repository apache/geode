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
package org.apache.geode.internal.net;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import javax.net.ssl.SSLException;

import org.apache.geode.internal.admin.SSLConfig;

/*
 * This test class will fail the TLS handshake with an SSLException, by default.
 */
public class SocketCreatorFailHandshake extends SocketCreator {
  private final List<Integer> socketSoTimeouts;
  private boolean failTLSHandshake;

  public SocketCreatorFailHandshake(SSLConfig sslConfig, List<Integer> socketTimeouts) {
    super(sslConfig);
    this.socketSoTimeouts = socketTimeouts;
    failTLSHandshake = true;
  }

  /**
   * @param failTLSHandshake false will cause the next handshake to NOT throw SSLException
   */
  public void setFailTLSHandshake(final boolean failTLSHandshake) {
    this.failTLSHandshake = failTLSHandshake;
  }

  @Override
  public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    this.socketSoTimeouts.add(timeout);
    if (failTLSHandshake) {
      throw new SSLException("This is a test SSLException");
    } else {
      super.handshakeIfSocketIsSSL(socket, timeout);
    }
  }
}
