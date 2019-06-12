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

package org.apache.geode.cache.client.internal;

import java.net.Socket;

/**
 * Derivatives of this class can have an alternative socket timeout from the default connection
 * timeout.
 */
public abstract class AbstractOpWithTimeout extends AbstractOp {

  private final int timeoutMs;

  AbstractOpWithTimeout(int msgType, int msgParts, int timeoutMs) {
    super(msgType, msgParts);

    this.timeoutMs = timeoutMs;
  }


  @Override
  public Object attempt(final Connection connection) throws Exception {
    final Socket socket = connection.getSocket();
    final int previousTimeoutMs = socket.getSoTimeout();
    final int timeoutMs = getTimeoutMs();
    final boolean changeTimeout = previousTimeoutMs != timeoutMs;
    if (changeTimeout) {
      socket.setSoTimeout(timeoutMs);
    }
    try {
      return super.attempt(connection);
    } finally {
      if (changeTimeout) {
        socket.setSoTimeout(previousTimeoutMs);
      }
    }
  }

  final int getTimeoutMs() {
    return timeoutMs;
  }
}
