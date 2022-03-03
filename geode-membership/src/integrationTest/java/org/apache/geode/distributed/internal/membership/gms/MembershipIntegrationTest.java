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

import static org.apache.geode.distributed.internal.membership.api.MembershipConfig.DEFAULT_LOCATOR_WAIT_TIME;
import static org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.FIND_LOCATOR_RETRY_SLEEP;
import static org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.JOIN_RETRY_SLEEP;
import static org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.getMinimumRetriesBeforeBecomingCoordinator;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

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

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void before() throws IOException, MembershipConfigurationException {
    localHost = LocalHostUtil.getLocalHost();
    dsfidSerializer = new DSFIDSerializerImpl();
    Services.registerSerializables(dsfidSerializer);
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

    stop(membership);
    stop(locator);
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

    await().untilAsserted(
        () -> assertThat(membership1.getView().getMembers()).hasSize(2));

    await().untilAsserted(
        () -> assertThat(membership2.getView().getMembers()).hasSize(2));

    stop(membership1, membership2);
    stop(locator);
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

    await().untilAsserted(
        () -> assertThat(membership1.getView().getMembers()).hasSize(2));
    await().untilAsserted(
        () -> assertThat(membership2.getView().getMembers()).hasSize(2));

    stop(membership2, membership1);
    stop(locator2, locator1);
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
    stop(locator1);

    Membership<MemberIdentifier> membership2 =
        createMembership(locator2, locatorPort1, locatorPort2);
    start(membership2);

    await().untilAsserted(
        () -> assertThat(membership1.getView().getMembers()).hasSize(2));
    await().untilAsserted(
        () -> assertThat(membership2.getView().getMembers()).hasSize(2));

    stop(membership2, membership1);
    stop(locator2, locator1);
  }

  @Test
  public void locatorWaitsForLocatorWaitTimeUntilAllLocatorsContacted()
      throws InterruptedException, TimeoutException, ExecutionException {

    final Supplier<ExecutorService> executorServiceSupplier =
        () -> LoggingExecutors.newCachedThreadPool("membership", false);

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    int locatorWaitTime = (int) Duration.ofMinutes(5).getSeconds();
    final MembershipConfig config =
        createMembershipConfig(true, locatorWaitTime, locatorPorts[0], locatorPorts[1]);

    /*
     * Start a locator trying to contact the locator that hasn't started it's port
     *
     * Set its locator-wait-time so it'll not become a coordinator right away, allowing time for the
     * other member to start and become a coordinator.
     */
    CompletableFuture<Membership<MemberIdentifier>> createMembership0 =
        launchLocator(executorServiceSupplier, locatorPorts[0], config);

    // minimum duration a locator waits to become the coordinator, regardless of locatorWaitTime
    final Duration minimumJoinWaitTime = Duration
        // amount of sleep time per retry in GMSJoinLeave.join()
        .ofMillis(JOIN_RETRY_SLEEP + FIND_LOCATOR_RETRY_SLEEP)
        // expected number of retries in GMSJoinLeave.join()
        .multipliedBy(getMinimumRetriesBeforeBecomingCoordinator(locatorPorts.length));

    /*
     * By sleeping for 2x the minimumJoinWaitTime, we are trying to make sure we sleep for
     * longer than the minimum but shorter than the locatorWaitTime so we can detect whether the
     * lateJoiningMembership is waiting for the full locatorWaitTime and not just the minimum
     * wait time.
     */
    Thread.sleep(2 * minimumJoinWaitTime.toMillis());

    assertThat(createMembership0.getNow(null)).isNull();

    /*
     * Now start the other locator, after waiting longer than the minimum wait time for
     * connecting to a locator but shorter than the locator-wait-time.
     */
    CompletableFuture<Membership<MemberIdentifier>> createMembership1 =
        launchLocator(executorServiceSupplier, locatorPorts[1], config);


    // Make sure the members are created in less than the locator-wait-time
    Membership<MemberIdentifier> membership0 = createMembership0.get(2, TimeUnit.MINUTES);
    Membership<MemberIdentifier> membership1 = createMembership1.get(2, TimeUnit.MINUTES);

    // Make sure the members see each other in the view
    await().untilAsserted(() -> assertThat(membership0.getView().getMembers()).hasSize(2));
    await().untilAsserted(() -> assertThat(membership1.getView().getMembers()).hasSize(2));

    stop(membership0, membership1);
  }


  private CompletableFuture<Membership<MemberIdentifier>> launchLocator(
      Supplier<ExecutorService> executorServiceSupplier, int locatorPort, MembershipConfig config) {
    return executorServiceRule.supplyAsync(() -> {
      try {
        Path locatorDirectory0 = temporaryFolder.newFolder().toPath();
        MembershipLocator<MemberIdentifier> locator0 =
            MembershipLocatorBuilder.newLocatorBuilder(
                socketCreator,
                dsfidSerializer,
                locatorDirectory0,
                executorServiceSupplier)
                .setConfig(config)
                .setPort(locatorPort)
                .create();
        locator0.start();

        Membership<MemberIdentifier> membership = createMembership(config, locator0);
        membership.start();
        membership.startEventProcessing();
        return membership;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
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
    final MembershipConfig config =
        createMembershipConfig(isALocator, DEFAULT_LOCATOR_WAIT_TIME, locatorPorts);
    return createMembership(config, embeddedLocator);
  }

  private Membership<MemberIdentifier> createMembership(
      final MembershipConfig config,
      final MembershipLocator<MemberIdentifier> embeddedLocator)
      throws MembershipConfigurationException {
    final MemberIdentifierFactoryImpl memberIdFactory = new MemberIdentifierFactoryImpl();

    final TcpClient locatorClient =
        new TcpClient(socketCreator, dsfidSerializer.getObjectSerializer(),
            dsfidSerializer.getObjectDeserializer(), TcpSocketFactory.DEFAULT);

    return MembershipBuilder.newMembershipBuilder(
        socketCreator, locatorClient, dsfidSerializer, memberIdFactory)
        .setMembershipLocator(embeddedLocator)
        .setConfig(config)
        .create();
  }

  private MembershipConfig createMembershipConfig(
      final boolean isALocator,
      final int locatorWaitTime,
      final int... locatorPorts) {
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

      @Override
      public int getLocatorWaitTime() {
        return locatorWaitTime;
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

    final MembershipConfig config =
        createMembershipConfig(true, DEFAULT_LOCATOR_WAIT_TIME, locatorPorts);

    return MembershipLocatorBuilder.newLocatorBuilder(
        socketCreator,
        dsfidSerializer,
        locatorDirectory,
        executorServiceSupplier)
        .setConfig(config)
        .setPort(localPort)
        .create();
  }

  private void stop(final Membership<MemberIdentifier>... memberships) {
    Arrays.stream(memberships).forEach(membership -> membership.disconnect(false));
  }

  private void stop(final MembershipLocator<MemberIdentifier>... locators) {
    Arrays.stream(locators).forEach(MembershipLocator::stop);
  }
}
