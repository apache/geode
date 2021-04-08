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
package org.apache.geode.distributed.internal;

import java.io.DataInputStream;
import java.net.Socket;

import org.apache.geode.distributed.internal.tcpserver.ProtocolChecker;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.Handshake;

public class ProtocolCheckerImpl implements ProtocolChecker {

  @Override
  public boolean checkProtocol(final Socket socket, final DataInputStream input,
      final int firstByte) throws Exception {
    if (CommunicationMode.isValidMode(firstByte)) {
      socket.getOutputStream().write(Handshake.REPLY_SERVER_IS_LOCATOR);
      throw new Exception("Improperly configured client detected - use addPoolLocator to "
          + "configure its locators instead of addPoolServer.");
    }
    return false;
  }
}
