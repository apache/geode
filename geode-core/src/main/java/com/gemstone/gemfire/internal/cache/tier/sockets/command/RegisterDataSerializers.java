/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;

public class RegisterDataSerializers extends BaseCommand {

  private final static RegisterDataSerializers singleton = new RegisterDataSerializers();

  public static Command getCommand() {
    return singleton;
  }

  private RegisterDataSerializers() {
  }

  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received register dataserializer request ({} parts) from {}", servConn.getName(), msg.getNumberOfParts(), servConn.getSocketString());
    }
    int noOfParts = msg.getNumberOfParts();
    
    // 2 parts per instantiator and one eventId part
    int noOfDataSerializers = (noOfParts - 1) / 2;

    // retrieve eventID from the last Part
    ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(msg.getPart(noOfParts - 1)
        .getSerializedForm());
    long threadId = EventID
        .readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    long sequenceId = EventID
        .readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    EventID eventId = new EventID(servConn.getEventMemberIDByteArray(), threadId,
        sequenceId);

    byte[][] serializedDataSerializers = new byte[noOfDataSerializers * 2][];
    boolean caughtCNFE = false;
    Exception cnfe = null;
    try {
      for (int i = 0; i < noOfParts - 1; i = i + 2) {

        Part dataSerializerClassNamePart = msg.getPart(i);
        serializedDataSerializers[i] = dataSerializerClassNamePart.getSerializedForm();
        String dataSerializerClassName = (String)CacheServerHelper
            .deserialize(serializedDataSerializers[i]);

        Part idPart = msg.getPart(i + 1);
        serializedDataSerializers[i + 1] = idPart.getSerializedForm();
        int id = idPart.getInt();

        Class dataSerializerClass = null;
        try {
          dataSerializerClass = InternalDataSerializer.getCachedClass(dataSerializerClassName);
          InternalDataSerializer.register(dataSerializerClass, true, eventId, servConn.getProxyID());
        }
        catch (ClassNotFoundException e) {
          // If a ClassNotFoundException is caught, store it, but continue
          // processing other instantiators
          caughtCNFE = true;
          cnfe = e;
        }
      }
    }
    catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
    }

    // If a ClassNotFoundException was caught while processing the
    // instantiators, send it back to the client. Note: This only sends
    // the last CNFE.
    if (caughtCNFE) {
      writeException(msg, cnfe, false, servConn);
      servConn.setAsTrue(RESPONDED);
    }

    // Send reply to client if necessary. If an exception occurs in the above
    // code, then the reply has already been sent.
    if (!servConn.getTransientFlag(RESPONDED)) {
      writeReply(msg, servConn);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Registered dataserializer for MembershipId = {}", servConn.getMembershipID());
    }
  }
}
