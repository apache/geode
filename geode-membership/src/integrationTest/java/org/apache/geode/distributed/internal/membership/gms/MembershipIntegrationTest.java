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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorBuilder;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreatorImpl;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;
import org.apache.geode.logging.internal.executors.LoggingExecutors;

/**
 * Tests of using the membership APIs to make multiple Membership systems that communicate
 * with each other and form a group
 */
public class MembershipIntegrationTest {
  private InetAddress localHost;
  private DSFIDSerializer dsfidSerializer;
  private TcpSocketCreator socketCreator;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException, MembershipConfigurationException {
    localHost = LocalHostUtil.getLocalHost();
    dsfidSerializer = new DSFIDSerializerImpl();
    socketCreator = new TcpSocketCreatorImpl();
  }

  @Test
  public void oneMembershipCanStartWithALocator()
      throws IOException, MemberStartupException {
    final MembershipLocator<MemberIdentifier> locator = createLocator(0);
    locator.start();

    final Membership<MemberIdentifier> membership = createMembership(locator,
        locator.getPort());
    start(membership);

    assertThat(membership.getView().getMembers()).hasSize(1);
  }

  @Test
  public void twoMembershipsCanStartWithOneLocator()
      throws IOException, MemberStartupException {
    final MembershipLocator<MemberIdentifier> locator = createLocator(0);
    locator.start();
    final int locatorPort = locator.getPort();

    final Membership<MemberIdentifier> membership1 = createMembership(locator, locatorPort);
    start(membership1);

    final Membership<MemberIdentifier> membership2 = createMembership(null, locatorPort);
    start(membership2);

    assertThat(membership1.getView().getMembers()).hasSize(2);
    assertThat(membership2.getView().getMembers()).hasSize(2);
  }

  @Test
  public void twoLocatorsCanStartSequentially()
      throws IOException, MemberStartupException {

    final MembershipLocator<MemberIdentifier> locator1 = createLocator(0);
    locator1.start();
    final int locatorPort1 = locator1.getPort();

    Membership<MemberIdentifier> membership1 = createMembership(locator1, locatorPort1);
    start(membership1);

    final MembershipLocator<MemberIdentifier> locator2 = createLocator(0, locatorPort1);
    locator2.start();
    final int locatorPort2 = locator2.getPort();

    Membership<MemberIdentifier> membership2 =
        createMembership(locator2, locatorPort1, locatorPort2);
    start(membership2);

    assertThat(membership1.getView().getMembers()).hasSize(2);
    assertThat(membership2.getView().getMembers()).hasSize(2);
  }

  @Test
  public void secondMembershipCanJoinUsingTheSecondLocatorToStart()
      throws IOException, MemberStartupException {

    final MembershipLocator<MemberIdentifier> locator1 = createLocator(0);
    locator1.start();
    final int locatorPort1 = locator1.getPort();

    final Membership<MemberIdentifier> membership1 = createMembership(locator1, locatorPort1);
    start(membership1);

    final MembershipLocator<MemberIdentifier> locator2 = createLocator(0, locatorPort1);
    locator2.start();
    int locatorPort2 = locator2.getPort();

    // Force the next membership to use locator2 by stopping locator1
    locator1.stop();

    Membership<MemberIdentifier> membership2 =
        createMembership(locator2, locatorPort1, locatorPort2);
    start(membership2);

    assertThat(membership1.getView().getMembers()).hasSize(2);
    assertThat(membership2.getView().getMembers()).hasSize(2);
  }

  private void start(final Membership<MemberIdentifier> membership)
      throws MemberStartupException {
    membership.start();
    membership.startEventProcessing();
  }

  private Membership<MemberIdentifier> createMembership(
      final MembershipLocator<MemberIdentifier> embeddedLocator,
      final int... locatorPorts)
      throws MembershipConfigurationException {
    final boolean isALocator = embeddedLocator != null;
    final MembershipConfig config = createMembershipConfig(isALocator, locatorPorts);

    final MemberIdentifierFactoryImpl memberIdFactory = new MemberIdentifierFactoryImpl();

    final TcpClient locatorClient =
        new TcpClient(socketCreator, dsfidSerializer.getObjectSerializer(),
            dsfidSerializer.getObjectDeserializer(), TcpSocketFactory.DEFAULT);

    return MembershipBuilder.<MemberIdentifier>newMembershipBuilder(
        socketCreator, locatorClient, dsfidSerializer, memberIdFactory)
        .setMembershipLocator(embeddedLocator)
        .setConfig(config)
        .create();
  }

  private MembershipConfig createMembershipConfig(
      final boolean isALocator,
      final int[] locatorPorts) {
    return new MembershipConfig() {
      public String getLocators() {
        return getLocatorString(locatorPorts);
      }

      // TODO - the Membership system starting in the locator *MUST* be told that is
      // is a locator through this flag. Ideally it should be able to infer this from
      // being associated with a locator
      @Override
      public int getVmKind() {
        return isALocator ? MemberIdentifier.LOCATOR_DM_TYPE : MemberIdentifier.NORMAL_DM_TYPE;
      }
    };
  }

  private String getLocatorString(
      final int... locatorPorts) {
    final String hostName = localHost.getHostName();
    return Arrays.stream(locatorPorts)
        .mapToObj(port -> hostName + '[' + port + ']')
        .collect(Collectors.joining(","));
  }

  private MembershipLocator<MemberIdentifier> createLocator(
      final int localPort,
      final int... locatorPorts)
      throws MembershipConfigurationException,
      IOException {
    final Supplier<ExecutorService> executorServiceSupplier =
        () -> LoggingExecutors.newCachedThreadPool("membership", false);
    Path locatorDirectory = temporaryFolder.newFolder().toPath();

    final MembershipConfig config = createMembershipConfig(true, locatorPorts);

    return MembershipLocatorBuilder.<MemberIdentifier>newLocatorBuilder(
        socketCreator,
        dsfidSerializer,
        locatorDirectory,
        executorServiceSupplier)
        .setConfig(config)
        .setPort(localPort)
        .create();
  }

}
