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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.test.assertj.LogFileAssert.assertThat;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.net.SocketCloser;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category(MembershipTest.class)
public class ConnectionIntegrationTest {

  private static final String EXPECTED_EXCEPTION_MESSAGE =
      "Unknown handshake reply code: 99 messageLength: 0";

  private File logFile;
  private Cache cache;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() throws Exception {
    logFile = temporaryFolder.newFile();

    Properties properties = new Properties();
    properties.setProperty(LOCATORS, "");
    properties.setProperty(LOG_FILE, logFile.getAbsolutePath());

    cache = cacheRule.getOrCreateCache(properties);

    addIgnoredException(EXPECTED_EXCEPTION_MESSAGE);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void badHeaderMessageIsCorrectlyLogged() {
    ConnectionTable connectionTable = mock(ConnectionTable.class);
    DistributionConfig config = mock(DistributionConfig.class);
    Socket socket = mock(Socket.class);
    TCPConduit tcpConduit = mock(TCPConduit.class);

    when(connectionTable.getConduit()).thenReturn(tcpConduit);
    when(tcpConduit.getSocketId()).thenReturn(new InetSocketAddress("localhost", 1234));
    when(config.getEnableNetworkPartitionDetection()).thenReturn(false);
    when(tcpConduit.getConfig()).thenReturn(config);
    when(tcpConduit.getMemberId()).thenReturn(new InternalDistributedMember("localhost", 2345));
    when(connectionTable.getSocketCloser()).thenReturn(mock(SocketCloser.class));

    ClusterConnection connection = new ClusterConnection(connectionTable, socket);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[] {99});
    DataInputStream inputStream = new DataInputStream(byteArrayInputStream);

    connection.readHandshakeForSender(inputStream, ByteBuffer.allocate(100));

    assertThat(logFile).contains(EXPECTED_EXCEPTION_MESSAGE);
  }
}
