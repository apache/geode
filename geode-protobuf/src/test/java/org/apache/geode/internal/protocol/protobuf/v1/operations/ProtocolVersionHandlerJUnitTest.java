package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.protocol.protobuf.ProtocolVersion;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.MessageUtil;
import org.apache.geode.test.junit.categories.ClientServerTest;

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

@Category({ClientServerTest.class})
public class ProtocolVersionHandlerJUnitTest {
  private static final int INVALID_MAJOR_VERSION = 67;
  private static final int INVALID_MINOR_VERSION = 92347;

  private ProtocolVersionHandler protocolVersionHandler = new ProtocolVersionHandler();

  @Test
  public void testCurrentVersionVersionMessageSucceeds() throws Exception {
    ProtocolVersion.NewConnectionClientVersion versionRequest =
        generateVersionMessageRequest(ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
            ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    ByteArrayInputStream inputStream =
        MessageUtil.writeMessageDelimitedToInputStream(versionRequest);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    assertTrue(protocolVersionHandler.handleVersionMessage(inputStream, outputStream,
        mock(ClientStatistics.class)));

    ProtocolVersion.VersionAcknowledgement versionResponse = ProtocolVersion.VersionAcknowledgement
        .parseDelimitedFrom(new ByteArrayInputStream(outputStream.toByteArray()));
    assertTrue(versionResponse.getVersionAccepted());
    assertEquals(ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
        versionResponse.getServerMajorVersion());
    assertEquals(ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE,
        versionResponse.getServerMinorVersion());
  }

  @Test
  public void testInvalidMajorVersionFails() throws Exception {
    assertNotEquals(INVALID_MAJOR_VERSION,
        ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE);

    ProtocolVersion.NewConnectionClientVersion versionRequest = generateVersionMessageRequest(
        INVALID_MAJOR_VERSION, ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    verifyVersionMessageFails(versionRequest);

    // Also validate the protobuf INVALID_MAJOR_VERSION_VALUE constant fails
    versionRequest =
        generateVersionMessageRequest(ProtocolVersion.MajorVersions.INVALID_MAJOR_VERSION_VALUE,
            ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
    verifyVersionMessageFails(versionRequest);
  }

  private void verifyVersionMessageFails(ProtocolVersion.NewConnectionClientVersion versionRequest)
      throws Exception {
    ByteArrayInputStream inputStream =
        MessageUtil.writeMessageDelimitedToInputStream(versionRequest);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    try {
      protocolVersionHandler.handleVersionMessage(inputStream, outputStream,
          mock(ClientStatistics.class));
      fail("Invalid version should throw IOException");
    } catch (IOException e) {
      // expected if version verification fails
    }

    ProtocolVersion.VersionAcknowledgement versionResponse = ProtocolVersion.VersionAcknowledgement
        .parseDelimitedFrom(new ByteArrayInputStream(outputStream.toByteArray()));

    assertFalse(versionResponse.getVersionAccepted());
  }

  @Test
  public void testInvalidMinorVersionFails() throws Exception {
    assertNotEquals(INVALID_MINOR_VERSION,
        ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    ProtocolVersion.NewConnectionClientVersion versionRequest = generateVersionMessageRequest(
        ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE, INVALID_MINOR_VERSION);

    verifyVersionMessageFails(versionRequest);

    // Also validate the protobuf INVALID_MINOR_VERSION_VALUE constant fails
    versionRequest =
        generateVersionMessageRequest(ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
            ProtocolVersion.MinorVersions.INVALID_MINOR_VERSION_VALUE);
    verifyVersionMessageFails(versionRequest);
  }

  private ProtocolVersion.NewConnectionClientVersion generateVersionMessageRequest(int majorVersion,
      int minorVersion) {
    return ProtocolVersion.NewConnectionClientVersion.newBuilder().setMajorVersion(majorVersion)
        .setMinorVersion(minorVersion).build();
  }
}
