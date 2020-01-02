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

package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.internal.membership.adapter.TcpSocketCreatorAdapter.asTcpSocketCreator;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.ProtocolCheckerImpl;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolServiceLoader;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category({MembershipTest.class})
public class TcpServerFactoryTest {
  @Before
  public void before() {
    SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));
  }

  @After
  public void teardown() {
    SocketCreatorFactory.close();
  }

  @Test
  public void createsATcpServer() {
    TcpServerFactory factory = new TcpServerFactory();

    TcpServer server = new TcpServer(80, null, null,
        null, new ProtocolCheckerImpl(null, new ClientProtocolServiceLoader()),
        DistributionStats::getStatTime, TcpServerFactory.createExecutorServiceSupplier(null),
        asTcpSocketCreator(
            SocketCreatorFactory
                .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR)),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        GeodeGlossary.GEMFIRE_PREFIX + "TcpServer.READ_TIMEOUT",
        GeodeGlossary.GEMFIRE_PREFIX + "TcpServer.BACKLOG");
    assertTrue(server != null);
  }
}
