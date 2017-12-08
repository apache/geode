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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.protocol.protobuf.Handshake;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;

public class HandshakeHandler {
  private static final Logger logger = LogManager.getLogger();
  private static final VersionValidator validator = new VersionValidator();

  public static boolean handleHandshake(InputStream inputStream, OutputStream outputStream,
      ProtocolClientStatistics statistics) throws IOException {
    Handshake.NewConnectionHandshake handshakeRequest =
        Handshake.NewConnectionHandshake.parseDelimitedFrom(inputStream);

    statistics.messageReceived(handshakeRequest.getSerializedSize());

    final boolean handshakeSucceeded =
        validator.isValid(handshakeRequest.getMajorVersion(), handshakeRequest.getMinorVersion());

    Handshake.HandshakeAcknowledgement handshakeResponse = Handshake.HandshakeAcknowledgement
        .newBuilder().setServerMajorVersion(Handshake.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
        .setServerMinorVersion(Handshake.MinorVersions.CURRENT_MINOR_VERSION_VALUE)
        .setHandshakePassed(handshakeSucceeded).build();

    handshakeResponse.writeDelimitedTo(outputStream);
    statistics.messageSent(handshakeResponse.getSerializedSize());
    if (!handshakeSucceeded) {
      throw new IOException("Incompatible protobuf version.");
    }

    return handshakeSucceeded;
  }
}
