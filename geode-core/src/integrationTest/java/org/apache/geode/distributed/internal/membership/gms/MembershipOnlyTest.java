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

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.internal.membership.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierImpl;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorBuilder;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DSFIDSerializerFactory;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.logging.internal.executors.LoggingExecutors;

public class MembershipOnlyTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private InetAddress localHost;
  private DSFIDSerializer dsfidSerializer;
  private TcpSocketCreator socketCreator;
  private MembershipLocator membershipLocator;

  @Before
  public void before() throws IOException, MembershipConfigurationException {
    localHost = InetAddress.getLocalHost();

    dsfidSerializer = new DSFIDSerializerFactory().create();

    // TODO -if we don't do this, the serializer doesn't know how to deserialize
    // membership classes. Seems like this should happen during membership initialization
    // or using a ServiceLoader?
    Services.registerSerializables(dsfidSerializer);

    // TODO - MemberIdentiferImpl probably needs it's own DSFID, or it should be BasicSerializable
    // Or ... ? Right now it has the same ID as InternalDistributedMember
    dsfidSerializer.registerDSFID(DataSerializableFixedID.DISTRIBUTED_MEMBER,
        MemberIdentifierImpl.class);

    // TODO - using geode-core socket creator
    socketCreator = asTcpSocketCreator(new SocketCreator(new SSLConfig.Builder().build()));

    final Supplier<ExecutorService> executorServiceSupplier =
        () -> LoggingExecutors.newCachedThreadPool("membership", false);
    membershipLocator = MembershipLocatorBuilder.<MemberIdentifierImpl>newLocatorBuilder(
        socketCreator,
        dsfidSerializer.getObjectSerializer(),
        dsfidSerializer.getObjectDeserializer(),
        temporaryFolder.newFile("locator").toPath(),
        executorServiceSupplier)
        .create();

    membershipLocator.start();
  }

  @After
  public void after() {
    membershipLocator.stop();
  }

  @Test
  public void locatorStarts() {
    assertThat(membershipLocator.getPort()).isGreaterThan(0);
  }

  @Test
  public void memberCanConnectToSelfHostedLocator() throws MemberStartupException {

    MembershipConfig config = new MembershipConfig() {
      public String getLocators() {
        return localHost.getHostName() + '[' + membershipLocator.getPort() + ']';
      }

      // TODO - the Membership system starting in the locator *MUST* be told that is
      // is a locator through this flag. Ideally it should be able to infer this from
      // being associated with a locator
      @Override
      public int getVmKind() {
        return MemberIdentifier.LOCATOR_DM_TYPE;
      }
    };

    MemberIdentifierFactoryImpl memberIdFactory = new MemberIdentifierFactoryImpl();

    TcpClient locatorClient = new TcpClient(socketCreator, dsfidSerializer.getObjectSerializer(),
        dsfidSerializer.getObjectDeserializer());

    LifecycleListener<MemberIdentifierImpl> lifeCycleListener = mock(LifecycleListener.class);

    final Membership<MemberIdentifierImpl> membership =
        MembershipBuilder.<MemberIdentifierImpl>newMembershipBuilder(
            socketCreator, locatorClient, dsfidSerializer, memberIdFactory)
            .setConfig(config)
            .setLifecycleListener(lifeCycleListener)
            .create();


    // TODO - the membership *must* be installed in the locator at this special
    // point during membership startup for the start to succeed
    doAnswer(invocation -> {
      membershipLocator.setMembership(membership);
      return null;
    }).when(lifeCycleListener).started();


    membership.start();
    assertThat(membership.getView().getMembers()).hasSize(1);
  }
}
