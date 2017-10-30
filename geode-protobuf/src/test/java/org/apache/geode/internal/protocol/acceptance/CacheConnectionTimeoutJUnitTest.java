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

package org.apache.geode.internal.protocol.acceptance;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.protocol.MessageUtil;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Test the new protocol correctly times out connections
 */
@Category(IntegrationTest.class)
public class CacheConnectionTimeoutJUnitTest {
  private final String TEST_KEY = "testKey";
  private final String TEST_VALUE = "testValue";
  private final String TEST_REGION = "testRegion";

  private Cache cache;
  private SerializationService serializationService;
  private Socket socket;
  private OutputStream outputStream;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName testName = new TestName();
  private long monitorInterval;
  private int maximumTimeBetweenPings;

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    System.setProperty(ClientHealthMonitor.CLIENT_HEALTH_MONITOR_INTERVAL_PROPERTY, "25");

    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.setSecurityManager(null);

    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    int cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.setMaximumTimeBetweenPings(100);
    cacheServer.start();

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.create(TEST_REGION);

    System.setProperty("geode.feature-protobuf-protocol", "true");

    socket = new Socket("localhost", cacheServerPort);

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    outputStream = socket.getOutputStream();
    outputStream.write(110);

    serializationService = new ProtobufSerializationService();

    monitorInterval = ClientHealthMonitor.getInstance().getMonitorInterval();
    maximumTimeBetweenPings = ClientHealthMonitor.getInstance().getMaximumTimeBetweenPings();

    // sanity check to keep us from tweaking the test to the point where it gets brittle
    assertTrue("monitor time: " + monitorInterval + " less than half of maximumTimeBetweenPings",
        monitorInterval * 2 < maximumTimeBetweenPings);
  }

  @After
  public void cleanup() throws IOException {
    cache.close();
    socket.close();
    SocketCreatorFactory.close();
    ClientHealthMonitor.shutdownInstance();
  }

  @Test
  public void testUnresponsiveClientsGetDisconnected() throws Exception {
    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message putMessage =
        MessageUtil.makePutRequestMessage(serializationService, TEST_KEY, TEST_VALUE, TEST_REGION);

    InputStream inputStream = socket.getInputStream();

    // Better to wait a bit later for jitter and pass the test, than to have intermittent failures.
    long delay = maximumTimeBetweenPings + (maximumTimeBetweenPings / 2) + monitorInterval;

    protobufProtocolSerializer.serialize(putMessage, outputStream);
    protobufProtocolSerializer.deserialize(inputStream);

    // In this case, I think Thread.sleep is actually the right choice. This is
    // not right for Awaitility because it is not a condition that will be
    // eventually satisfied; rather, it is a condition that we expect to be met
    // after a specific amount of time.
    Thread.sleep(delay);

    // should be disconnected, expecting EOF.
    assertEquals(-1, inputStream.read());
  }

  @Test
  public void testResponsiveClientsStaysConnected() throws Exception {
    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message putMessage =
        MessageUtil.makePutRequestMessage(serializationService, TEST_KEY, TEST_VALUE, TEST_REGION);

    int timeout = maximumTimeBetweenPings * 4;
    int interval = maximumTimeBetweenPings / 4;
    for (int i = 0; i < timeout; i += interval) {
      // send a PUT message
      protobufProtocolSerializer.serialize(putMessage, outputStream);
      protobufProtocolSerializer.deserialize(socket.getInputStream());
      Thread.sleep(interval);
    }
  }
}
