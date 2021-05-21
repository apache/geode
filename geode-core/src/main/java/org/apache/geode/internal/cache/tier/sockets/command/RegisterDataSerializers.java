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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

public class RegisterDataSerializers extends BaseCommand {

  @Immutable
  private static final RegisterDataSerializers singleton = new RegisterDataSerializers();

  public static Command getCommand() {
    return singleton;
  }

  private RegisterDataSerializers() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start)
      throws IOException, ClassNotFoundException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received register dataserializer request ({} parts) from {}",
          serverConnection.getName(), clientMessage.getNumberOfParts(),
          serverConnection.getSocketString());
    }

    if (!ServerConnection.allowInternalMessagesWithoutCredentials) {
      serverConnection.getAuthzRequest();
    }

    int noOfParts = clientMessage.getNumberOfParts();

    // 2 parts per instantiator and one eventId part
    int noOfDataSerializers = (noOfParts - 1) / 2;

    // retrieve eventID from the last Part
    ByteBuffer eventIdPartsBuffer =
        ByteBuffer.wrap(clientMessage.getPart(noOfParts - 1).getSerializedForm());
    long threadId = EventID.readEventIdPartsFromOptimizedByteArray(eventIdPartsBuffer);
    long sequenceId = EventID.readEventIdPartsFromOptimizedByteArray(eventIdPartsBuffer);
    EventID eventId =
        new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId);

    byte[][] serializedDataSerializers = new byte[noOfDataSerializers * 2][];
    boolean caughtCNFE = false;
    Exception cnfe = null;
    try {
      for (int i = 0; i < noOfParts - 1; i = i + 2) {

        Part dataSerializerClassNamePart = clientMessage.getPart(i);
        serializedDataSerializers[i] = dataSerializerClassNamePart.getSerializedForm();
        String dataSerializerClassName =
            (String) CacheServerHelper.deserialize(serializedDataSerializers[i]);

        Part idPart = clientMessage.getPart(i + 1);
        serializedDataSerializers[i + 1] = idPart.getSerializedForm();
        int id = idPart.getInt();

        Class dataSerializerClass = null;
        try {
          dataSerializerClass = InternalDataSerializer.getCachedClass(dataSerializerClassName);
          InternalDataSerializer.register(dataSerializerClass, true, eventId,
              serverConnection.getProxyID());
        } catch (ClassNotFoundException e) {
          // If a ClassNotFoundException is caught, store it, but continue
          // processing other instantiators
          caughtCNFE = true;
          cnfe = e;
        }
      }
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    }

    // If a ClassNotFoundException was caught while processing the
    // instantiators, send it back to the client. Note: This only sends
    // the last CNFE.
    if (caughtCNFE) {
      writeException(clientMessage, cnfe, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    }

    // Send reply to client if necessary. If an exception occurs in the above
    // code, then the reply has already been sent.
    if (!serverConnection.getTransientFlag(RESPONDED)) {
      writeReply(clientMessage, serverConnection);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Registered dataserializer for MembershipId = {}",
          serverConnection.getMembershipID());
    }
  }
}
