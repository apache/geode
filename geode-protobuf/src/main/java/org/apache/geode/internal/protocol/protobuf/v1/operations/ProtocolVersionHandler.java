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

import org.apache.geode.internal.protocol.protobuf.ProtocolVersion;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;

public class ProtocolVersionHandler {
  private static final VersionValidator validator = new VersionValidator();

  public static boolean handleVersionMessage(InputStream inputStream, OutputStream outputStream,
      ClientStatistics statistics) throws IOException {
    ProtocolVersion.NewConnectionClientVersion versionMessage =
        ProtocolVersion.NewConnectionClientVersion.parseDelimitedFrom(inputStream);

    statistics.messageReceived(versionMessage.getSerializedSize());

    final boolean versionAccepted =
        validator.isValid(versionMessage.getMajorVersion(), versionMessage.getMinorVersion());

    ProtocolVersion.VersionAcknowledgement handshakeResponse =
        ProtocolVersion.VersionAcknowledgement.newBuilder()
            .setServerMajorVersion(ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
            .setServerMinorVersion(ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE)
            .setVersionAccepted(versionAccepted).build();

    handshakeResponse.writeDelimitedTo(outputStream);
    statistics.messageSent(handshakeResponse.getSerializedSize());
    if (!versionAccepted) {
      throw new IOException("Incompatible protobuf version.");
    }

    return versionAccepted;
  }
}
