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

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.LocatorStats;
import org.apache.geode.distributed.internal.membership.adapter.ServiceConfig;
import org.apache.geode.distributed.internal.membership.gms.api.Authenticator;
import org.apache.geode.distributed.internal.membership.gms.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.gms.api.MemberData;
import org.apache.geode.distributed.internal.membership.gms.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.gms.api.MemberShunnedException;
import org.apache.geode.distributed.internal.membership.gms.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.gms.api.Membership;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.api.MessageListener;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.locator.GMSLocator;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.distributed.internal.membership.gms.messages.AbstractGMSMessage;
import org.apache.geode.distributed.internal.tcpserver.ConnectionWatcher;
import org.apache.geode.distributed.internal.tcpserver.ProtocolChecker;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DSFIDSerializerFactory;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.Version;

@Category({MembershipOnlyJUnitTest.class})
public class MembershipOnlyJUnitTest {

  public static final int MemberID_DSFID = 2002;
  public static final int SerialMessage_DSFID = 2001;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private TcpClient locatorClient;

  @Test
  public void testMultipleManagersInSameProcess() throws Exception {
    Membership<MemberID> m1 = null, m2 = null;
    TcpServer tcpServer = null;

    final MemberIdentifierFactory memberFactory = mock(MemberIdentifierFactory.class);
    when(memberFactory.create(isA(GMSMemberData.class))).thenAnswer(new Answer<MemberIdentifier>() {
      @Override
      public MemberIdentifier answer(InvocationOnMock invocation) throws Throwable {
        return new MemberID((GMSMemberData) invocation.getArgument(0));
      }
    });

    try {
      final DSFIDSerializer dsfidSerializer = createDSFIDSerializer();
      TcpSocketCreator socketCreator = new TestTcpSocketCreator();
      locatorClient = new TcpClient(socketCreator, dsfidSerializer.getObjectSerializer(),
          dsfidSerializer.getObjectDeserializer());
      String locatorsString = "";
      GMSLocator locator = new GMSLocator(InetAddress.getLocalHost(), locatorsString,
          true, false, mock(
              LocatorStats.class),
          "", new File("").toPath(), locatorClient,
          dsfidSerializer.getObjectSerializer(), dsfidSerializer.getObjectDeserializer());
      int locationServicePort = AvailablePortHelper.getRandomAvailableTCPPort();
      tcpServer = new TcpServer(locationServicePort, InetAddress.getLocalHost(), locator,
          "testTcpServerThread", mock(ProtocolChecker.class),
          System::nanoTime, MembershipOnlyJUnitTest::newThreadPool, socketCreator,
          dsfidSerializer.getObjectSerializer(),
          dsfidSerializer.getObjectDeserializer(),
          "scooby_scooby_doo",
          "where_are_you");
      System.out.println("Test is starting a locator");
      tcpServer.start();

      // boot up a locator
      InetAddress localHost = InetAddress.getLocalHost();
      String locators = localHost.getHostName() + '[' + locationServicePort + ']';


      System.out.println("Test is creating the first membership manager");
      try {
        System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
        m1 = createMembershipManager(locators, memberFactory).getLeft();
      } finally {
        System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
      }
      locator.setMembership(m1);

      System.out.println("Test is creating the second membership manager");
      final Pair<Membership, MessageListener> pair =
          createMembershipManager(locators, memberFactory);
      m2 = pair.getLeft();
      final MessageListener listener2 = pair.getRight();

      // we have to check the views with JoinLeave because the membership
      // manager queues new views for processing through the DM listener,
      // which is a mock object in this test
      System.out.println("waiting for views to stabilize");
      JoinLeave jl1 = ((GMSMembership) m1).getServices().getJoinLeave();
      JoinLeave jl2 = ((GMSMembership) m2).getServices().getJoinLeave();
      long giveUp = System.currentTimeMillis() + 15000;
      for (;;) {
        try {
          assertTrue("view = " + jl2.getView(), jl2.getView().size() == 2);
          assertTrue("view = " + jl1.getView(), jl1.getView().size() == 2);
          assertTrue(jl1.getView().getCreator().equals(jl2.getView().getCreator()));
          assertTrue(jl1.getView().getViewId() == jl2.getView().getViewId());
          break;
        } catch (AssertionError e) {
          if (System.currentTimeMillis() > giveUp) {
            throw e;
          }
        }
      }

      GMSMembershipView<MemberID> view = jl1.getView();
      MemberIdentifier notCreator;
      if (view.getCreator().equals(jl1.getMemberID())) {
        notCreator = view.getMembers().get(1);
      } else {
        notCreator = view.getMembers().get(0);
      }
      List<String> result = notCreator.getGroups();

      System.out.println("sending SerialMessage from m1 to m2");
      SerialMessage msg = new SerialMessage();
      msg.setRecipient(m2.getLocalMember());
      msg.setMulticast(false);
      m1.send(new MemberID[] {m2.getLocalMember()}, msg);
      giveUp = System.currentTimeMillis() + 15000;
      boolean verified = false;
      Throwable problem = null;
      while (giveUp > System.currentTimeMillis()) {
        try {
          verify(listener2).messageReceived(isA(SerialMessage.class));
          verified = true;
          break;
        } catch (Error e) {
          problem = e;
          Thread.sleep(500);
        }
      }
      if (!verified) {
        AssertionError error = new AssertionError("Expected a message to be received");
        if (problem != null) {
          error.initCause(problem);
        }
        throw error;
      }

      // let the managers idle for a while and get used to each other
      // Thread.sleep(4000l);

      m2.disconnect(false);
      assertTrue(!m2.isConnected());

      System.out.println("view is " + m1.getView());
      final Membership<MemberID> waitingMember = m1;
      await().untilAsserted(() -> assertTrue(waitingMember.getView().size() == 1));
    } finally {

      if (m2 != null) {
        m2.shutdown();
      }
      if (m1 != null) {
        m1.shutdown();
      }
      if (tcpServer != null) {
        tcpServer.requestShutdown();
        tcpServer.join(300_000);
      }
    }
  }

  private DSFIDSerializer createDSFIDSerializer() {
    final DSFIDSerializer dsfidSerializer = new DSFIDSerializerFactory().create();
    Services.registerSerializables(dsfidSerializer);
    dsfidSerializer.registerDSFID(SerialMessage_DSFID, SerialMessage.class);
    dsfidSerializer.registerDSFID(MemberID_DSFID, MemberID.class);
    return dsfidSerializer;
  }

  private Pair<Membership, MessageListener> createMembershipManager(String locators,
      MemberIdentifierFactory memberIdentifierFactory)
      throws MemberStartupException, MemberShunnedException {
    // create configuration objects
    Properties nonDefault = new Properties();
    nonDefault.put(DISABLE_TCP, "true");
    nonDefault.put(MCAST_PORT, "0");
    nonDefault.put(LOG_FILE, "");
    nonDefault.put(LOG_LEVEL, "fine");
    nonDefault.put(MEMBER_TIMEOUT, "2000");
    nonDefault.put(LOCATORS, locators);
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    RemoteTransportConfig transport =
        new RemoteTransportConfig(config, GMSMemberData.NORMAL_DM_TYPE);

    final MembershipListener listener = mock(MembershipListener.class);
    final MessageListener messageListener = mock(MessageListener.class);
    DSFIDSerializer serializer = createDSFIDSerializer();

    LifecycleListener lifeCycleListener = mock(LifecycleListener.class);
    final Membership m1 =
        MembershipBuilder.<MemberID>newMembershipBuilder()
            .setAuthenticator(mock(Authenticator.class))
            .setStatistics(mock(MembershipStatistics.class))
            .setMessageListener(messageListener)
            .setMembershipListener(listener)
            .setConfig(new ServiceConfig(transport, config))
            .setSerializer(serializer)
            .setLifecycleListener(lifeCycleListener)
            .setMemberIDFactory(memberIdentifierFactory)
            .setLocatorClient(locatorClient)
            .setSocketCreator(new TestTcpSocketCreator())
            .create();
    // doAnswer(invocation -> {
    // DistributionImpl.connectLocatorToServices(m1);
    // return null;
    // }).when(lifeCycleListener).started();
    m1.start();
    m1.startEventProcessing();
    return Pair.of(m1, messageListener);
  }


  public static class MemberID implements MemberIdentifier {
    MemberData memberData;
    private boolean isPartialIdentifier;

    public MemberID() {} // constructor for deserialization

    public MemberID(GMSMemberData data) {
      memberData = data;
    }

    @Override
    public MemberData getMemberData() {
      return memberData;
    }

    public boolean equals(Object o) {
      return compareTo(o) == 0;
    }

    public int compareTo(Object o) {
      return compareTo(o, true, false);
    }

    public int compareTo(Object o, boolean compareViewIds, boolean compareMemberData) {
      if (this == o) {
        return 0;
      }
      // obligatory type check
      if (!(o instanceof MemberID))
        throw new ClassCastException(
            "MemberID.compareTo(): comparison between different classes");
      MemberID other = (MemberID) o;

      int myPort = getMembershipPort();
      int otherPort = other.getMembershipPort();
      if (myPort < otherPort)
        return -1;
      if (myPort > otherPort)
        return 1;


      InetAddress myAddr = getInetAddress();
      InetAddress otherAddr = other.getInetAddress();

      // Discard null cases
      if (myAddr == null && otherAddr == null) {
        return 0;
      } else if (myAddr == null) {
        return -1;
      } else if (otherAddr == null)
        return 1;

      byte[] myBytes = myAddr.getAddress();
      byte[] otherBytes = otherAddr.getAddress();

      if (myBytes != otherBytes) {
        for (int i = 0; i < myBytes.length; i++) {
          if (i >= otherBytes.length)
            return -1; // same as far as they go, but shorter...
          if (myBytes[i] < otherBytes[i])
            return -1;
          if (myBytes[i] > otherBytes[i])
            return 1;
        }
        if (myBytes.length > otherBytes.length)
          return 1; // same as far as they go, but longer...
      }
      if (compareViewIds) {
        // not loners, so look at P2P view ID
        int thisViewId = getVmViewId();
        int otherViewId = other.getVmViewId();
        if (thisViewId >= 0 && otherViewId >= 0) {
          if (thisViewId < otherViewId) {
            return -1;
          } else if (thisViewId > otherViewId) {
            return 1;
          } // else they're the same, so continue
        }
      }

      if (compareMemberData && this.memberData != null && other.memberData != null) {
        return this.memberData.compareAdditionalData(other.memberData);
      } else {
        return 0;
      }
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = result + memberData.getInetAddress().hashCode();
      result = result + getMembershipPort();
      return result;
    }

    @Override
    public String getHostName() {
      return memberData.getHostName();
    }

    @Override
    public InetAddress getInetAddress() {
      return memberData.getInetAddress();
    }

    @Override
    public int getMembershipPort() {
      return memberData.getMembershipPort();
    }

    @Override
    public short getVersionOrdinal() {
      return memberData.getVersionOrdinal();
    }

    @Override
    public int getVmViewId() {
      return memberData.getVmViewId();
    }

    @Override
    public boolean preferredForCoordinator() {
      return memberData.isPreferredForCoordinator();
    }

    @Override
    public int getVmKind() {
      return memberData.getVmKind();
    }

    @Override
    public int getMemberWeight() {
      return memberData.getMemberWeight();
    }

    @Override
    public List<String> getGroups() {
      return Arrays.asList(memberData.getGroups());
    }

    @Override
    public void setVmViewId(int viewNumber) {
      memberData.setVmViewId(viewNumber);
    }

    @Override
    public void setPreferredForCoordinator(boolean preferred) {
      memberData.setPreferredForCoordinator(preferred);
    }

    @Override
    public void setDirectChannelPort(int dcPort) {
      memberData.setDirectChannelPort(dcPort);
    }

    @Override
    public void setVmKind(int dmType) {
      memberData.setVmKind(dmType);
    }

    @Override
    public Version getVersionObject() {
      return Version.fromOrdinalNoThrow(memberData.getVersionOrdinal(), false);
    }

    @Override
    public void setMemberData(MemberData memberData) {
      this.memberData = memberData;
    }

    @Override
    public void setIsPartial(boolean b) {
      isPartialIdentifier = true;
    }

    @Override
    public boolean isPartial() {
      return isPartialIdentifier;
    }

    @Override
    public int getDSFID() {
      return MemberID_DSFID;
    }

    @Override
    public void toData(DataOutput out, SerializationContext context) throws IOException {
      StaticSerialization.writeInetAddress(getInetAddress(), out);
      out.writeInt(getMembershipPort());

      StaticSerialization.writeString(memberData.getHostName(), out);
      out.writeBoolean(memberData.isNetworkPartitionDetectionEnabled());
      out.writeBoolean(memberData.isPreferredForCoordinator());
      out.writeBoolean(memberData.isPartial());

      out.writeInt(memberData.getDirectChannelPort());
      out.writeInt(memberData.getProcessId());
      int vmKind = memberData.getVmKind();
      out.writeByte(vmKind);
      StaticSerialization.writeStringArray(memberData.getGroups(), out);

      StaticSerialization.writeString(memberData.getName(), out);
      out.writeInt(memberData.getVmViewId());
      short version = memberData.getVersionOrdinal();
      out.writeInt(version);
      memberData.writeAdditionalData(out);
    }

    @Override
    public void fromData(DataInput in, DeserializationContext context)
        throws IOException, ClassNotFoundException {
      InetAddress inetAddr = StaticSerialization.readInetAddress(in);
      int port = in.readInt();

      String hostName = StaticSerialization.readString(in);

      boolean sbEnabled = in.readBoolean();
      boolean elCoord = in.readBoolean();
      isPartialIdentifier = in.readBoolean();

      int dcPort = in.readInt();
      int vmPid = in.readInt();
      int vmKind = in.readUnsignedByte();
      String[] groups = StaticSerialization.readStringArray(in);

      String name = StaticSerialization.readString(in);
      int vmViewId = in.readInt();

      int version = in.readInt();

      memberData = MemberDataBuilder.newBuilder(inetAddr, hostName)
          .setMembershipPort(port)
          .setDirectChannelPort(dcPort)
          .setName(name)
          .setNetworkPartitionDetectionEnabled(sbEnabled)
          .setPreferredForCoordinator(elCoord)
          .setVersionOrdinal((short) version)
          .setVmPid(vmPid)
          .setVmKind(vmKind)
          .setVmViewId(vmViewId)
          .setGroups(groups)
          .build();

      memberData.readAdditionalData(in);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      addFixedToString(sb);

      // add version if not current
      short version = memberData.getVersionOrdinal();
      if (version != Version.CURRENT.ordinal()) {
        sb.append("(version:").append(Version.toString(version)).append(')');
      }

      // leave out Roles on purpose

      return sb.toString();
    }

    public void addFixedToString(StringBuilder sb) {
      // Note: This method is used to generate the HARegion name. If it is changed, memory and GII
      // issues will occur in the case of clients with subscriptions during rolling upgrade.
      String host;

      InetAddress add = getInetAddress();
      if (add.isMulticastAddress())
        host = add.getHostAddress();
      else {
        String hostName = memberData.getHostName();
        host = hostName;
      }

      sb.append(host);
      sb.append('/').append(memberData.getInetAddress().getHostAddress());

      int vmPid = memberData.getProcessId();
      int vmKind = memberData.getVmKind();
      if (vmPid > 0 || vmKind != GMSMemberData.NORMAL_DM_TYPE) {
        sb.append("(");

        if (vmPid > 0)
          sb.append(vmPid);

        String vmStr = "";
        switch (vmKind) {
          case GMSMemberData.NORMAL_DM_TYPE:
            // vmStr = ":local"; // let this be silent
            break;
          case GMSMemberData.LOCATOR_DM_TYPE:
            vmStr = ":locator";
            break;
          case GMSMemberData.ADMIN_ONLY_DM_TYPE:
            vmStr = ":admin";
            break;
          case GMSMemberData.LONER_DM_TYPE:
            vmStr = ":loner";
            break;
          default:
            vmStr = ":<unknown:" + vmKind + ">";
            break;
        }
        sb.append(vmStr);
        sb.append(")");
      }
      if (vmKind != GMSMemberData.LONER_DM_TYPE
          && memberData.isPreferredForCoordinator()) {
        sb.append("<ec>");
      }
      int vmViewId = getVmViewId();
      if (vmViewId >= 0) {
        sb.append("<v" + vmViewId + ">");
      }
      sb.append(":");
      sb.append(getMembershipPort());
    }

    @Override
    public Version[] getSerializationVersions() {
      return new Version[0];
    }
  }

  public static class LocationService {
    TcpServer tcpServer;

    public LocationService(TcpServer server) {
      this.tcpServer = server;
    }
  }

  public static class TestTcpSocketCreator implements TcpSocketCreator {

    public TestTcpSocketCreator() {}

    @Override
    public boolean useSSL() {
      return false;
    }

    @Override
    public ServerSocket createServerSocket(int nport, int backlog) throws IOException {
      return new ServerSocket(nport, backlog);
    }

    @Override
    public ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr)
        throws IOException {
      ServerSocket socket = new ServerSocket();
      socket.bind(new InetSocketAddress(bindAddr, nport), backlog);
      return socket;
    }

    @Override
    public InetAddress getLocalHost() throws UnknownHostException {
      return InetAddress.getLocalHost();
    }

    @Override
    public String getHostName(InetAddress addr) {
      return addr.getCanonicalHostName();
    }

    @Override
    public ServerSocket createServerSocketUsingPortRange(InetAddress ba, int backlog,
        boolean isBindAddress, boolean useNIO,
        int tcpBufferSize, int[] tcpPortRange,
        boolean sslConnection) throws IOException {
      for (int port = tcpPortRange[0]; port < tcpPortRange[1]; port++) {
        try {
          return createServerSocket(port, backlog, ba);
        } catch (IOException e) {
          // port in use - continue
        }
      }
      throw new IOException("unable to allocate a server port in the specified range: "
          + Arrays.toString(tcpPortRange));
    }


    @Override
    public Socket connect(InetAddress inetadd, int port, int timeout,
        ConnectionWatcher optionalWatcher, boolean clientSide)
        throws IOException {
      Socket socket = new Socket();
      socket.setSoTimeout(timeout);
      // ignore optionalWatcher for now
      // ignore clientSide for now - used for geode client/server comms and SSL handshakes
      socket.connect(new InetSocketAddress(inetadd, port));
      return socket;
    }

    @Override
    public Socket connect(InetAddress inetadd, int port, int timeout,
        ConnectionWatcher optionalWatcher, boolean clientSide,
        int socketBufferSize, boolean sslConnection) throws IOException {
      return connect(inetadd, port, timeout, optionalWatcher, clientSide);
    }

    @Override
    public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
      // see useSSL()
    }

    @Override
    public boolean resolveDns() {
      return true;
    }
  }

  public static class SerialMessage extends AbstractGMSMessage {

    @Override
    public int getDSFID() {
      return SerialMessage_DSFID;
    }

    @Override
    public void toData(DataOutput out, SerializationContext context) throws IOException {

    }

    @Override
    public void fromData(DataInput in, DeserializationContext context)
        throws IOException, ClassNotFoundException {

    }

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }
  }

  public static ExecutorService newThreadPool() {
    return Executors.newCachedThreadPool();
  }

}
