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

import org.apache.geode.internal.protocol.protobuf.Version;
import org.apache.geode.internal.protocol.protobuf.v1.MessageUtil;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;
import org.apache.geode.test.junit.categories.UnitTest;

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

@Category(UnitTest.class)
public class HandshakeHandlerJUnitTest {
  private static final int INVALID_MAJOR_VERSION = 67;
  private static final int INVALID_MINOR_VERSION = 92347;

  private VersionHandler handshakeHandler = new VersionHandler();

  @Test
  public void testCurrentVersionHandshakeSucceeds() throws Exception {
    Version.NewConnectionClientVersion handshakeRequest =
        generateHandshakeRequest(Version.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
            Version.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    ByteArrayInputStream inputStream =
        MessageUtil.writeMessageDelimitedToInputStream(handshakeRequest);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    assertTrue(handshakeHandler.handleClientVersion(inputStream, outputStream,
        mock(ProtocolClientStatistics.class)));

    Version.VersionAcknowledgement handshakeResponse = Version.VersionAcknowledgement
        .parseDelimitedFrom(new ByteArrayInputStream(outputStream.toByteArray()));
    assertTrue(handshakeResponse.getHandshakePassed());
    assertEquals(Version.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
        handshakeResponse.getServerMajorVersion());
    assertEquals(Version.MinorVersions.CURRENT_MINOR_VERSION_VALUE,
        handshakeResponse.getServerMinorVersion());
  }

  @Test
  public void testInvalidMajorVersionFails() throws Exception {
    assertNotEquals(INVALID_MAJOR_VERSION, Version.MajorVersions.CURRENT_MAJOR_VERSION_VALUE);

    Version.NewConnectionClientVersion handshakeRequest = generateHandshakeRequest(
        INVALID_MAJOR_VERSION, Version.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    verifyHandshakeFails(handshakeRequest);

    // Also validate the protobuf INVALID_MAJOR_VERSION_VALUE constant fails
    handshakeRequest = generateHandshakeRequest(Version.MajorVersions.INVALID_MAJOR_VERSION_VALUE,
        Version.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
    verifyHandshakeFails(handshakeRequest);
  }

  private void verifyHandshakeFails(Version.NewConnectionClientVersion handshakeRequest)
      throws Exception {
    ByteArrayInputStream inputStream =
        MessageUtil.writeMessageDelimitedToInputStream(handshakeRequest);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    try {
      handshakeHandler.handleClientVersion(inputStream, outputStream,
          mock(ProtocolClientStatistics.class));
      fail("Invalid handshake should throw IOException");
    } catch (IOException e) {
      // expected if handshake verification fails
    }

    Version.VersionAcknowledgement handshakeResponse = Version.VersionAcknowledgement
        .parseDelimitedFrom(new ByteArrayInputStream(outputStream.toByteArray()));

    assertFalse(handshakeResponse.getHandshakePassed());
  }

  @Test
  public void testInvalidMinorVersionFails() throws Exception {
    assertNotEquals(INVALID_MINOR_VERSION, Version.MinorVersions.CURRENT_MINOR_VERSION_VALUE);

    Version.NewConnectionClientVersion handshakeRequest = generateHandshakeRequest(
        Version.MajorVersions.CURRENT_MAJOR_VERSION_VALUE, INVALID_MINOR_VERSION);

    verifyHandshakeFails(handshakeRequest);

    // Also validate the protobuf INVALID_MINOR_VERSION_VALUE constant fails
    handshakeRequest = generateHandshakeRequest(Version.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
        Version.MinorVersions.INVALID_MINOR_VERSION_VALUE);
    verifyHandshakeFails(handshakeRequest);
  }

  private Version.NewConnectionClientVersion generateHandshakeRequest(int majorVersion,
                                                                      int minorVersion) {
    return Version.NewConnectionClientVersion.newBuilder().setMajorVersion(majorVersion)
        .setMinorVersion(minorVersion).build();
  }
}
