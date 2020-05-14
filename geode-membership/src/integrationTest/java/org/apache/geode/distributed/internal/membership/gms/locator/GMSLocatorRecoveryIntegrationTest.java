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
package org.apache.geode.distributed.internal.membership.gms.locator;

import static org.apache.geode.distributed.internal.membership.gms.locator.GMSLocator.LOCATOR_FILE_STAMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorBuilder;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.MemberIdentifierImpl;
import org.apache.geode.distributed.internal.membership.gms.MembershipLocatorStatisticsNoOp;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.util.MemberIdentifierUtil;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreatorImpl;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category(MembershipTest.class)
public class GMSLocatorRecoveryIntegrationTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  private File stateFile;
  private GMSLocator gmsLocator;
  private MembershipLocator locator;
  private DSFIDSerializer serializer;

  @Before
  public void setUp() throws Exception {

    serializer = new DSFIDSerializerImpl();
    Services.registerSerializables(serializer);
    Version current = Version.CURRENT; // force version initialization

    stateFile = new File(temporaryFolder.getRoot(), getClass().getSimpleName() + "_locator.dat");

    gmsLocator = new GMSLocator(null, null, false,
        false, new MembershipLocatorStatisticsNoOp(), "",
        temporaryFolder.getRoot().toPath(), new TcpClient(new TcpSocketCreatorImpl(),
            serializer.getObjectSerializer(),
            serializer.getObjectDeserializer()),
        serializer.getObjectSerializer(),
        serializer.getObjectDeserializer());
    gmsLocator.setViewFile(stateFile);
  }

  @After
  public void tearDown() throws Exception {
    // TODO stop locator threads?
  }

  @Test
  public void testRecoverFromFileWithNonExistFile() {
    assertThat(stateFile).doesNotExist();

    assertThat(gmsLocator.recoverFromFile(stateFile)).isFalse();
  }

  @Test
  public void testRecoverFromFileWithNormalFile() throws Exception {
    GMSMembershipView view = new GMSMembershipView();
    populateStateFile(stateFile, LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL, view);

    assertThat(gmsLocator.recoverFromFile(stateFile)).isTrue();
  }

  @Test
  public void testRecoverFromFileWithWrongFileStamp() throws Exception {
    // add 1 to file stamp to make it invalid
    populateStateFile(stateFile, LOCATOR_FILE_STAMP + 1, Version.CURRENT_ORDINAL,
        MemberIdentifierUtil.createMemberID(2345));

    assertThat(gmsLocator.recoverFromFile(stateFile)).isFalse();
  }

  @Test
  public void testRecoverFromFileWithWrongOrdinal() throws Exception {
    // add 1 to ordinal to make it wrong
    populateStateFile(stateFile, LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL + 1,
        MemberIdentifierUtil.createMemberID(1234));

    boolean recovered = gmsLocator.recoverFromFile(stateFile);
    assertThat(recovered).isFalse();
  }

  @Test
  public void testRecoverFromFileWithInvalidViewObject() throws Exception {
    populateStateFile(stateFile, LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL,
        MemberIdentifierUtil.createMemberID(1234));

    Throwable thrown = catchThrowable(() -> gmsLocator.recoverFromFile(stateFile));

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Unable to recover previous membership view from");
  }

  @Test
  public void testRecoverFromOther() throws Exception {
    InetAddress localHost = LocalHostUtil.getLocalHost();

    final Supplier<ExecutorService> executorServiceSupplier =
        () -> LoggingExecutors.newCachedThreadPool("membership", false);
    final TcpSocketCreatorImpl socketCreator = new TcpSocketCreatorImpl();
    locator = MembershipLocatorBuilder.newLocatorBuilder(socketCreator, serializer,
        temporaryFolder.getRoot().toPath(), executorServiceSupplier)
        .setBindAddress(localHost).create();
    final int port = locator.start();

    try {
      final TcpClient locatorClient =
          new TcpClient(socketCreator, serializer.getObjectSerializer(),
              serializer.getObjectDeserializer());

      MembershipConfig membershipConfig = new MembershipConfig() {
        public String getLocators() {
          try {
            return LocalHostUtil.getLocalHostName() + "[" + port + "]";
          } catch (UnknownHostException e) {
            throw new RuntimeException("unable to locate localhost for this machine");
          }
        }
      };
      final Membership<MemberIdentifierImpl> membership =
          MembershipBuilder.newMembershipBuilder(socketCreator, locatorClient, serializer,
              new MemberIdentifierFactoryImpl()).setConfig(membershipConfig)
              .setMembershipLocator(locator)
              .create();
      membership.start();
      membership.startEventProcessing();
      assertThat(membership.getView().size()).isEqualTo(1);

      // now create a peer location handler that should recover from our real locator and know
      // that real locator's identifier
      GMSLocator gmsLocator = new GMSLocator(localHost,
          membership.getLocalMember().getHost() + "[" + port + "]", true, true,
          new MembershipLocatorStatisticsNoOp(), "", temporaryFolder.getRoot().toPath(),
          locatorClient,
          serializer.getObjectSerializer(),
          serializer.getObjectDeserializer());
      gmsLocator.setViewFile(new File(temporaryFolder.getRoot(), "locator2.dat"));
      TcpServer server = mock(TcpServer.class);
      gmsLocator.init(server);

      assertThat(gmsLocator.getMembers())
          .contains(membership.getLocalMember());
    } finally {
      locator.stop();
    }
  }

  @Test
  public void testViewFileNotFound() throws Exception {
    populateStateFile(stateFile, LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL,
        new GMSMembershipView());
    assertThat(stateFile).exists();

    File dir = temporaryFolder.newFolder(testName.getMethodName());
    File viewFileInNewDirectory = new File(dir, stateFile.getName());
    assertThat(viewFileInNewDirectory).doesNotExist();

    File locatorViewFile = gmsLocator.setViewFile(viewFileInNewDirectory);
    assertThat(gmsLocator.recoverFromFile(locatorViewFile)).isFalse();
  }

  private void populateStateFile(File file, int fileStamp, int ordinal, Object object)
      throws IOException {
    try (FileOutputStream fileStream = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(fileStream)) {
      oos.writeInt(fileStamp);
      oos.writeInt(ordinal);
      oos.flush();
      DataOutput dataOutput = new DataOutputStream(oos);
      serializer.getObjectSerializer().writeObject(object, dataOutput);
      fileStream.flush();
    }
  }
}
