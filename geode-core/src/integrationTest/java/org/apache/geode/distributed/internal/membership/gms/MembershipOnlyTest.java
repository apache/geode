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
package org.apache.geode.distributed.internal.membership.gms;

import static org.apache.geode.distributed.internal.membership.adapter.TcpSocketCreatorAdapter.asTcpSocketCreator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.DSFIDSerializer;

public class MembershipOnlyTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private InternalLocator locator;
  private InetAddress localHost;

  @Before
  public void before() throws IOException {
    localHost = InetAddress.getLocalHost();

    // TODO - using a geode-core locator
    locator = InternalLocator.startLocator(0, new File(""), null, null, localHost, false,
        new Properties(), null, temporaryFolder.getRoot().toPath());
  }

  @After
  public void after() {
    locator.stop();
  }

  @Test
  public void locatorStarts() {
    assertThat(locator.getPort()).isGreaterThan(0);
  }

  @Test
  public void memberCanConnectToLocator() throws MemberStartupException {

    final MembershipConfig config = new MembershipConfig() {
      public String getLocators() {
        return localHost.getHostName() + '[' + locator.getPort() + ']';
      }
    };

    // TODO - using geode-core InternalDistributedMember
    MemberIdentifierFactoryImpl memberIdFactory = new MemberIdentifierFactoryImpl();

    // TODO - using geode-core serializer. This is needed to have be able to
    // read InternalDistributedMember.
    DSFIDSerializer dsfidSerializer = InternalDataSerializer.getDSFIDSerializer();

    // TODO - using geode-core socket creator
    final TcpSocketCreator socketCreator =
        asTcpSocketCreator(new SocketCreator(new SSLConfig.Builder().build()));

    TcpClient client = new TcpClient(socketCreator, dsfidSerializer.getObjectSerializer(),
        dsfidSerializer.getObjectDeserializer());

    LifecycleListener<InternalDistributedMember> lifeCycleListener = mock(LifecycleListener.class);

    final Membership<InternalDistributedMember> membership =
        MembershipBuilder.<InternalDistributedMember>newMembershipBuilder()
            .setConfig(config)
            .setSerializer(dsfidSerializer)
            .setLifecycleListener(lifeCycleListener)
            .setMemberIDFactory(memberIdFactory)
            .setLocatorClient(client)
            .setSocketCreator(socketCreator)
            .create();


    // TODO - the membership *must* be installed in the locator at this special
    // point during membership startup for the start to succeed
    doAnswer(invocation -> {
      locator.getLocatorHandler().setMembership(membership);
      return null;
    }).when(lifeCycleListener).started();


    membership.start();
    assertThat(membership.getView().getMembers()).hasSize(1);
  }
}
