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

import org.apache.geode.distributed.internal.LocatorStats;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.serialization.DSFIDSerializer;

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
  public void setUp() {
    tcpServer = mock(TcpServer.class);
    view = new GMSMembershipView();
    services = mock(Services.class);
    DSFIDSerializer serializer = new DSFIDSerializer();
    Services.registerSerializables(serializer);
    when(services.getSerializer()).thenReturn(serializer);
    Version current = Version.CURRENT; // force Version static initialization to set
    // SerializationVersion

    joinLeave = mock(JoinLeave.class);
    when(services.getJoinLeave()).thenReturn(joinLeave);
    messenger = mock(Messenger.class);
    when(services.getMessenger()).thenReturn(messenger);
    when(messenger.getMemberID()).thenReturn(new GMSMember("localhost", 8080));

    gmsLocator =
        new GMSLocator(null, null, false, false, new LocatorStats(), "",
            temporaryFolder.getRoot().toPath());
    gmsLocator.setServices(services);
  }

  @Test
  public void viewFileIsNullByDefault() {
    assertThat(gmsLocator.getViewFile()).isNull();
  }

  @Test
  public void initDefinesViewFileInSpecifiedDirectory() {
    gmsLocator.init(String.valueOf(tcpServer.getPort()));

    assertThat(gmsLocator.getViewFile()).isNotNull();
  }

  @Test
  public void installViewCreatesViewFileInSpecifiedDirectory() {
    gmsLocator.init(String.valueOf(tcpServer.getPort()));

    gmsLocator.installView(view);

    assertThat(gmsLocator.getViewFile()).exists();
  }
}
