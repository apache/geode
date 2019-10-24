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
import java.io.IOException;
import java.net.Socket;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.distributed.internal.tcpserver.ProtocolChecker;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolService;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolServiceLoader;
import org.apache.geode.internal.cache.client.protocol.exception.ServiceLoadingFailureException;
import org.apache.geode.internal.cache.client.protocol.exception.ServiceVersionNotFoundException;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.Handshake;
import org.apache.geode.internal.logging.LogService;

public class ProtocolCheckerImpl implements ProtocolChecker {
  private static final Logger logger = LogService.getLogger();
  public final InternalLocator internalLocator;
  public final ClientProtocolServiceLoader clientProtocolServiceLoader;

  public ProtocolCheckerImpl(
      final InternalLocator internalLocator,
      final ClientProtocolServiceLoader clientProtocolServiceLoader) {
    this.internalLocator = internalLocator;
    this.clientProtocolServiceLoader = clientProtocolServiceLoader;
  }


  @Override
  public boolean checkProtocol(final Socket socket, final DataInputStream input,
      final int firstByte) throws Exception {
    boolean handled = false;
    if (firstByte == CommunicationMode.ProtobufClientServerProtocol.getModeNumber()) {
      handleProtobufConnection(socket, input);
      handled = true;
    } else if (CommunicationMode.isValidMode(firstByte)) {
      socket.getOutputStream().write(Handshake.REPLY_SERVER_IS_LOCATOR);
      throw new Exception("Improperly configured client detected - use addPoolLocator to "
          + "configure its locators instead of addPoolServer.");
    }
    return handled;
  }

  public void handleProtobufConnection(Socket socket, DataInputStream input) throws Exception {
    if (!Boolean.getBoolean("geode.feature-protobuf-protocol")) {
      logger.warn("Incoming protobuf connection, but protobuf not enabled on this locator.");
      socket.close();
      return;
    }

    try {
      ClientProtocolService clientProtocolService = clientProtocolServiceLoader.lookupService();
      clientProtocolService.initializeStatistics("LocatorStats",
          internalLocator.getDistributedSystem());
      try (ClientProtocolProcessor pipeline = clientProtocolService.createProcessorForLocator(
          internalLocator, internalLocator.getCache().getSecurityService())) {
        while (!pipeline.socketProcessingIsFinished()) {
          pipeline.processMessage(input, socket.getOutputStream());
        }
      } catch (IncompatibleVersionException e) {
        // should not happen on the locator as there is no handshake.
        logger.error("Unexpected exception in client message processing", e);
      }
    } catch (ServiceLoadingFailureException e) {
      logger.error("There was an error looking up the client protocol service", e);
      socket.close();
      throw new IOException("There was an error looking up the client protocol service", e);
    } catch (ServiceVersionNotFoundException e) {
      logger.error("Unable to find service matching the client protocol version byte", e);
      socket.close();
      throw new IOException("Unable to find service matching the client protocol version byte", e);
    }
  }
}
