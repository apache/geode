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
package org.apache.geode.internal.tcp;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.net.SocketCloser;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class ConnectionDUnitTest {

  @Rule
  public final DistributedRule distributedRule = DistributedRule.builder().withVMCount(0).build();

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File logFile;

  private DistributedSystem distributedSystem;

  @After
  public void teardown() {
    if (distributedSystem != null) {
      distributedSystem.disconnect();
    }
  }

  @Test
  public void badHeaderMessageIsCorrectlyLogged() throws Exception {
    Properties properties = DUnitLauncher.getDistributedSystemProperties();
    logFile = temporaryFolder.newFile();
    properties.put(ConfigurationProperties.LOG_FILE, logFile.getAbsolutePath());
    distributedSystem = DistributedSystem.connect(properties);

    final String expectedException = "Unknown handshake reply code: 99 messageLength: 0";

    IgnoredException.addIgnoredException(expectedException);

    ConnectionTable connectionTable = mock(ConnectionTable.class);
    TCPConduit tcpConduit = mock(TCPConduit.class);
    when(connectionTable.getConduit()).thenReturn(tcpConduit);
    when(tcpConduit.getSocketId()).thenReturn(new InetSocketAddress("localhost", 1234));
    DistributionConfig config = mock(DistributionConfig.class);
    when(config.getEnableNetworkPartitionDetection()).thenReturn(false);
    when(tcpConduit.getConfig()).thenReturn(config);
    when(tcpConduit.getMemberId()).thenReturn(new InternalDistributedMember("localhost", 2345));
    when(connectionTable.getSocketCloser()).thenReturn(mock(SocketCloser.class));
    Socket socket = mock(Socket.class);
    Connection connection = new Connection(connectionTable, socket);
    ByteBuffer peerDataBuffer = ByteBuffer.allocate(100);
    byte[] bytes = new byte[] {99};
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
    DataInputStream inputStream = new DataInputStream(byteArrayInputStream);
    connection.readHandshakeForSender(inputStream, peerDataBuffer);
    distributedSystem.disconnect();
    distributedSystem = null;
    LogFileAssert.assertThat(logFile).contains(expectedException);
    logFile.delete();
  }
}
