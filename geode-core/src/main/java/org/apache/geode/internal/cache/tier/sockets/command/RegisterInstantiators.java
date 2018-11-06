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

import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
import org.apache.geode.internal.cache.tier.sockets.ClientInstantiatorMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;


public class RegisterInstantiators extends BaseCommand {

  private static final RegisterInstantiators singleton = new RegisterInstantiators();

  public static Command getCommand() {
    return singleton;
  }

  private RegisterInstantiators() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start)
      throws IOException, ClassNotFoundException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received register instantiator request ({} parts) from {}",
          serverConnection.getName(), clientMessage.getNumberOfParts(),
          serverConnection.getSocketString());
    }

    int noOfParts = clientMessage.getNumberOfParts();
    // Assert parts
    Assert.assertTrue((noOfParts - 1) % 3 == 0);
    // 3 parts per instantiator and one eventId part
    int noOfInstantiators = (noOfParts - 1) / 3;

    // retrieve eventID from the last Part
    ByteBuffer eventIdPartsBuffer =
        ByteBuffer.wrap(clientMessage.getPart(noOfParts - 1).getSerializedForm());
    long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    EventID eventId =
        new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId);

    byte[][] serializedInstantiators = new byte[noOfInstantiators * 3][];
    boolean caughtCNFE = false;
    Exception cnfe = null;
    try {
      for (int i = 0; i < noOfParts - 1; i = i + 3) {

        Part instantiatorPart = clientMessage.getPart(i);
        serializedInstantiators[i] = instantiatorPart.getSerializedForm();
        String instantiatorClassName =
            (String) CacheServerHelper.deserialize(serializedInstantiators[i]);

        Part instantiatedPart = clientMessage.getPart(i + 1);
        serializedInstantiators[i + 1] = instantiatedPart.getSerializedForm();
        String instantiatedClassName =
            (String) CacheServerHelper.deserialize(serializedInstantiators[i + 1]);

        Part idPart = clientMessage.getPart(i + 2);
        serializedInstantiators[i + 2] = idPart.getSerializedForm();
        int id = idPart.getInt();

        Class instantiatorClass = null, instantiatedClass = null;
        try {
          instantiatorClass = InternalDataSerializer.getCachedClass(instantiatorClassName);
          instantiatedClass = InternalDataSerializer.getCachedClass(instantiatedClassName);
          InternalInstantiator.register(instantiatorClass, instantiatedClass, id, true, eventId,
              serverConnection.getProxyID());
        } catch (ClassNotFoundException e) {
          // If a ClassNotFoundException is caught, store it, but continue
          // processing other instantiators
          caughtCNFE = true;
          cnfe = e;
        }
      }
    } catch (Exception e) {
      logger.warn("Client {} failed to register instantiators: {}",
          new Object[] {serverConnection.getMembershipID(), e.getLocalizedMessage()});
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    }

    // If a ClassNotFoundException was caught while processing the
    // instantiators, send it back to the client. Note: This only sends
    // the last CNFE.
    if (caughtCNFE) {
      writeException(clientMessage, cnfe, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);

      // Send the instantiators on to other clients if we hit an error
      // due to a missing class, because they were not distributed
      // in InternalInstantiator.register. Otherwise they will have
      // been distributed if successfully registered.
      ClientInstantiatorMessage clientInstantiatorMessage =
          new ClientInstantiatorMessage(EnumListenerEvent.AFTER_REGISTER_INSTANTIATOR,
              serializedInstantiators, serverConnection.getProxyID(), eventId);

      // Notify other clients
      CacheClientNotifier.routeClientMessage(clientInstantiatorMessage);

    }

    // Send reply to client if necessary. If an exception occurs in the above
    // code, then the reply has already been sent.
    if (!serverConnection.getTransientFlag(RESPONDED)) {
      writeReply(clientMessage, serverConnection);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Registered instantiators for MembershipId = {}",
          serverConnection.getMembershipID());
    }
  }

}
