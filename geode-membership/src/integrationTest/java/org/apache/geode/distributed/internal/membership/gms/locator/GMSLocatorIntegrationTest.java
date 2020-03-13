/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.distributed.internal.membership.gms.locator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.MembershipLocatorStatisticsNoOp;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreatorImpl;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;

public class GMSLocatorIntegrationTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private TcpServer tcpServer;
  private GMSMembershipView view;
  private GMSLocator gmsLocator;
  Services services;
  private JoinLeave joinLeave;
  private Messenger messenger;

  @Before
  public void setUp() throws MembershipConfigurationException {

    tcpServer = mock(TcpServer.class);
    view = new GMSMembershipView();
    services = mock(Services.class);
    DSFIDSerializer serializer = new DSFIDSerializerImpl();
    Services.registerSerializables(serializer);
    when(services.getSerializer()).thenReturn(serializer);
    Version current = Version.CURRENT; // force Version static initialization to set
    // Version

    joinLeave = mock(JoinLeave.class);
    when(services.getJoinLeave()).thenReturn(joinLeave);
    messenger = mock(Messenger.class);
    when(services.getMessenger()).thenReturn(messenger);

    final MemberIdentifierFactoryImpl factory = new MemberIdentifierFactoryImpl();
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("localhost")
        .setMembershipPort(1234).build();
    MemberIdentifier memberId = factory.create(memberData);
    when(messenger.getMemberID()).thenReturn(memberId);

    gmsLocator =
        new GMSLocator(null, null, false, false, new MembershipLocatorStatisticsNoOp(), "",
            temporaryFolder.getRoot().toPath(), new TcpClient(new TcpSocketCreatorImpl(),
                services.getSerializer().getObjectSerializer(),
                services.getSerializer().getObjectDeserializer(), TcpSocketFactory.DEFAULT),
            services.getSerializer().getObjectSerializer(),
            services.getSerializer().getObjectDeserializer());

    final MembershipLocator membershipLocator = mock(MembershipLocator.class);
    gmsLocator.setServices(services);
  }

  @Test
  public void viewFileIsNullByDefault() {
    assertThat(gmsLocator.getViewFile()).isNull();
  }

  @Test
  public void initDefinesViewFileInSpecifiedDirectory() {
    gmsLocator.init(tcpServer);

    assertThat(gmsLocator.getViewFile()).isNotNull();
  }

  @Test
  public void installViewCreatesViewFileInSpecifiedDirectory() {
    gmsLocator.init(tcpServer);

    gmsLocator.installView(view);

    assertThat(gmsLocator.getViewFile()).exists();
  }
}
