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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import static org.apache.geode.distributed.internal.membership.gms.GMSUtil.replaceStrings;
import static org.apache.geode.internal.DataSerializableFixedID.FIND_COORDINATOR_REQ;
import static org.apache.geode.internal.DataSerializableFixedID.FIND_COORDINATOR_RESP;
import static org.apache.geode.internal.DataSerializableFixedID.JOIN_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.JOIN_RESPONSE;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.logging.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Message.Flag;
import org.jgroups.Message.TransientFlag;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Digest;
import org.jgroups.util.UUID;

import org.apache.geode.DataSerializer;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemConnectException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MemberAttributes;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.QuorumChecker;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.MessageHandler;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.cache.DirectReplyMessage;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.tcp.MemberShunnedException;


@SuppressWarnings("StatementWithEmptyBody")
public class JGroupsMessenger implements Messenger {

  private static final Logger logger = Services.getLogger();

  /**
   * The location (in the product) of the locator Jgroups config file.
   */
  private static final String DEFAULT_JGROUPS_TCP_CONFIG =
      "org/apache/geode/distributed/internal/membership/gms/messenger/jgroups-config.xml";

  /**
   * The location (in the product) of the mcast Jgroups config file.
   */
  private static final String JGROUPS_MCAST_CONFIG_FILE_NAME =
      "org/apache/geode/distributed/internal/membership/gms/messenger/jgroups-mcast.xml";

  /** JG magic numbers for types added to the JG ClassConfigurator */
  private static final short JGROUPS_TYPE_JGADDRESS = 2000;
  private static final short JGROUPS_PROTOCOL_TRANSPORT = 1000;

  public static boolean THROW_EXCEPTION_ON_START_HOOK;

  private String jgStackConfig;

  JChannel myChannel;
  InternalDistributedMember localAddress;
  JGAddress jgAddress;
  private Services services;

  /** handlers that receive certain classes of messages instead of the Manager */
  private final Map<Class, MessageHandler> handlers = new ConcurrentHashMap<>();

  private volatile NetView view;

  private final GMSPingPonger pingPonger = new GMSPingPonger();

  protected final AtomicLong pongsReceived = new AtomicLong(0);

  /** tracks multicast messages that have been scheduled for processing */
  protected final Map<DistributedMember, MessageTracker> scheduledMcastSeqnos = new HashMap<>();

  protected short nackack2HeaderId;

  /**
   * A set that contains addresses that we have logged JGroups IOExceptions for in the current
   * membership view and possibly initiated suspect processing. This reduces the amount of suspect
   * processing initiated by IOExceptions and the amount of exceptions logged
   */
  private final Set<Address> addressesWithIoExceptionsProcessed =
      Collections.synchronizedSet(new HashSet<Address>());

  static {
    // register classes that we've added to jgroups that are put on the wire
    // or need a header ID
    ClassConfigurator.add(JGROUPS_TYPE_JGADDRESS, JGAddress.class);
    ClassConfigurator.addProtocol(JGROUPS_PROTOCOL_TRANSPORT, Transport.class);
  }

  private GMSEncrypt encrypt;

  /**
   * DistributedMember identifiers already used, either in this JGroupsMessenger instance
   * or in a past one & retained through an auto-reconnect.
   */
  private Set<DistributedMember> usedDistributedMemberIdentifiers = new HashSet<>();

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void init(Services s) {
    this.services = s;

    RemoteTransportConfig transport = services.getConfig().getTransport();
    DistributionConfig dc = services.getConfig().getDistributionConfig();


    boolean b = dc.getEnableNetworkPartitionDetection();
    System.setProperty("jgroups.resolve_dns", String.valueOf(!b));

    InputStream is;

    String r;
    if (transport.isMcastEnabled()) {
      r = JGROUPS_MCAST_CONFIG_FILE_NAME;
    } else {
      r = DEFAULT_JGROUPS_TCP_CONFIG;
    }
    is = ClassPathLoader.getLatest().getResourceAsStream(getClass(), r);
    if (is == null) {
      throw new GemFireConfigException(
          String.format("Cannot find %s", r));
    }

    String properties;
    try {
      StringBuilder sb = new StringBuilder(3000);
      BufferedReader br;
      br = new BufferedReader(new InputStreamReader(is, "US-ASCII"));
      String input;
      while ((input = br.readLine()) != null) {
        sb.append(input);
      }
      br.close();
      properties = sb.toString();
    } catch (Exception ex) {
      throw new GemFireConfigException(
          "An Exception was thrown while reading JGroups config.",
          ex);
    }

    if (properties.startsWith("<!--")) {
      int commentEnd = properties.indexOf("-->");
      properties = properties.substring(commentEnd + 3);
    }


    if (transport.isMcastEnabled()) {
      properties = replaceStrings(properties, "MCAST_PORT",
          String.valueOf(transport.getMcastId().getPort()));
      properties =
          replaceStrings(properties, "MCAST_ADDRESS", dc.getMcastAddress().getHostAddress());
      properties = replaceStrings(properties, "MCAST_TTL", String.valueOf(dc.getMcastTtl()));
      properties = replaceStrings(properties, "MCAST_SEND_BUFFER_SIZE",
          String.valueOf(dc.getMcastSendBufferSize()));
      properties = replaceStrings(properties, "MCAST_RECV_BUFFER_SIZE",
          String.valueOf(dc.getMcastRecvBufferSize()));
      properties = replaceStrings(properties, "MCAST_RETRANSMIT_INTERVAL", "" + Integer
          .getInteger(DistributionConfig.GEMFIRE_PREFIX + "mcast-retransmit-interval", 500));
      properties = replaceStrings(properties, "RETRANSMIT_LIMIT",
          String.valueOf(dc.getUdpFragmentSize() - 256));
    }

    if (transport.isMcastEnabled() || transport.isTcpDisabled()
        || (dc.getUdpRecvBufferSize() != DistributionConfig.DEFAULT_UDP_RECV_BUFFER_SIZE)) {
      properties =
          replaceStrings(properties, "UDP_RECV_BUFFER_SIZE", "" + dc.getUdpRecvBufferSize());
    } else {
      properties = replaceStrings(properties, "UDP_RECV_BUFFER_SIZE",
          "" + DistributionConfig.DEFAULT_UDP_RECV_BUFFER_SIZE_REDUCED);
    }
    properties = replaceStrings(properties, "UDP_SEND_BUFFER_SIZE", "" + dc.getUdpSendBufferSize());

    String str = transport.getBindAddress();
    // JGroups UDP protocol requires a bind address
    if (str == null || str.length() == 0) {
      try {
        str = SocketCreator.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        throw new GemFireConfigException(e.getMessage(), e);
      }
    }
    properties = replaceStrings(properties, "BIND_ADDR_SETTING", "bind_addr=\"" + str + "\"");

    int port = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "jg-bind-port", 0);
    if (port != 0) {
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE_START", "" + port);
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE", "" + 0);
    } else {
      int[] ports = dc.getMembershipPortRange();
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE_START", "" + ports[0]);
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE", "" + (ports[1] - ports[0]));
    }

    properties = replaceStrings(properties, "UDP_FRAGMENT_SIZE", "" + dc.getUdpFragmentSize());

    properties = replaceStrings(properties, "FC_MAX_CREDITS",
        "" + dc.getMcastFlowControl().getByteAllowance());
    properties = replaceStrings(properties, "FC_THRESHOLD",
        "" + dc.getMcastFlowControl().getRechargeThreshold());
    properties = replaceStrings(properties, "FC_MAX_BLOCK",
        "" + dc.getMcastFlowControl().getRechargeBlockMs());

    this.jgStackConfig = properties;

    if (!dc.getSecurityUDPDHAlgo().isEmpty()) {
      try {
        this.encrypt = new GMSEncrypt(services, dc.getSecurityUDPDHAlgo());
        logger.info("Initializing GMSEncrypt ");
      } catch (Exception e) {
        throw new GemFireConfigException("problem initializing encryption protocol", e);
      }
    }
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void start() {
    // create the configuration XML string for JGroups
    String properties = this.jgStackConfig;

    long start = System.currentTimeMillis();

    // start the jgroups channel and establish the membership ID
    boolean reconnecting = false;
    try {
      Object oldDSMembershipInfo = services.getConfig().getTransport().getOldDSMembershipInfo();
      if (oldDSMembershipInfo != null) {
        logger.debug("Reusing JGroups channel from previous system", properties);
        MembershipInformation oldInfo = (MembershipInformation) oldDSMembershipInfo;
        myChannel = oldInfo.getChannel();
        usedDistributedMemberIdentifiers = oldInfo.getMembershipIdentifiers();

        // scrub the old channel
        ViewId vid = new ViewId(new JGAddress(), 0);
        List<Address> members = new ArrayList<>();
        members.add(new UUID(0, 0));// TODO open a JGroups JIRA for GEODE-3034
        View jgv = new View(vid, members);
        this.myChannel.down(new Event(Event.VIEW_CHANGE, jgv));
        UUID logicalAddress = (UUID) myChannel.getAddress();
        if (logicalAddress instanceof JGAddress) {
          ((JGAddress) logicalAddress).setVmViewId(-1);
        }
        reconnecting = true;
      } else {
        logger.debug("JGroups configuration: {}", properties);

        checkForIPv6();
        InputStream is = new ByteArrayInputStream(properties.getBytes("UTF-8"));
        myChannel = new JChannel(is);
      }
    } catch (Exception e) {
      throw new GemFireConfigException("unable to create jgroups channel", e);
    }

    // give the stats to the jchannel statistics recorder
    StatRecorder sr = (StatRecorder) myChannel.getProtocolStack().findProtocol(StatRecorder.class);
    if (sr != null) {
      sr.setServices(services);
    }

    Transport transport = (Transport) myChannel.getProtocolStack().getTransport();
    transport.setMessenger(this);

    nackack2HeaderId = ClassConfigurator.getProtocolId(NAKACK2.class);

    try {
      myChannel.setReceiver(null);
      myChannel.setReceiver(new JGroupsReceiver());
      if (!reconnecting) {
        myChannel.connect("AG"); // apache g***** (whatever we end up calling it)
      }
    } catch (Exception e) {
      myChannel.close();
      throw new SystemConnectException("unable to create jgroups channel", e);
    }

    if (JGroupsMessenger.THROW_EXCEPTION_ON_START_HOOK) {
      JGroupsMessenger.THROW_EXCEPTION_ON_START_HOOK = false;
      throw new SystemConnectException("failing for test");
    }

    establishLocalAddress();

    logger.info("JGroups channel {} (took {}ms)", (reconnecting ? "reinitialized" : "created"),
        System.currentTimeMillis() - start);

  }

  @Override
  public boolean isOldMembershipIdentifier(DistributedMember id) {
    return usedDistributedMemberIdentifiers.contains(id);
  }

  /**
   * JGroups picks an IPv6 address if preferIPv4Stack is false or not set and preferIPv6Addresses is
   * not set or is true. We want it to use an IPv4 address for a dual-IP stack so that both IPv4 and
   * IPv6 messaging work
   */
  private void checkForIPv6() throws Exception {
    boolean preferIpV6Addr = Boolean.getBoolean("java.net.preferIPv6Addresses");
    if (!preferIpV6Addr) {
      logger.debug("forcing JGroups to think IPv4 is being used so it will choose an IPv4 address");
      Field m = org.jgroups.util.Util.class.getDeclaredField("ip_stack_type");
      m.setAccessible(true);
      m.set(null, org.jgroups.util.StackType.IPv4);
    }
  }

  @Override
  public void started() {}

  @Override
  public void stop() {
    if (localAddress != null && localAddress.getVmViewId() >= 0) {
      // keep track of old addresses that were used to successfully join the cluster
      usedDistributedMemberIdentifiers.add(localAddress);
    }
    if (this.myChannel != null) {
      if ((services.isShutdownDueToForcedDisconnect() && services.isAutoReconnectEnabled())
          || services.getManager().isReconnectingDS()) {
        // leave the channel open for reconnect attempts
      } else {
        this.myChannel.close();
      }
    }
  }

  @Override
  public void stopped() {}

  @Override
  public void memberSuspected(InternalDistributedMember initiator,
      InternalDistributedMember suspect, String reason) {}

  @Override
  public void installView(NetView v) {
    this.view = v;

    if (this.jgAddress.getVmViewId() < 0) {
      this.jgAddress.setVmViewId(this.localAddress.getVmViewId());
    }
    List<JGAddress> mbrs = new ArrayList<>(v.size());
    mbrs.addAll(v.getMembers().stream().map(JGAddress::new).collect(Collectors.toList()));
    ViewId vid = new ViewId(new JGAddress(v.getCoordinator()), v.getViewId());
    View jgv = new View(vid, new ArrayList<>(mbrs));
    logger.trace("installing view into JGroups stack: {}", jgv);
    this.myChannel.down(new Event(Event.VIEW_CHANGE, jgv));

    addressesWithIoExceptionsProcessed.clear();
    if (encrypt != null) {
      encrypt.installView(v);
    }
    synchronized (scheduledMcastSeqnos) {
      for (DistributedMember mbr : v.getCrashedMembers()) {
        scheduledMcastSeqnos.remove(mbr);
      }
      for (DistributedMember mbr : v.getShutdownMembers()) {
        scheduledMcastSeqnos.remove(mbr);
      }
    }
  }


  /**
   * If JGroups is unable to send a message it may mean that the network is down. If so we need to
   * initiate suspect processing on the recipient.
   * <p>
   * see Transport._send()
   */
  @SuppressWarnings("UnusedParameters")
  public void handleJGroupsIOException(IOException e, Address dest) {
    if (services.getManager().shutdownInProgress()) { // GEODE-634 - don't log IOExceptions during
                                                      // shutdown
      return;
    }
    if (addressesWithIoExceptionsProcessed.contains(dest)) {
      return;
    }
    addressesWithIoExceptionsProcessed.add(dest);
    NetView v = this.view;
    JGAddress jgMbr = (JGAddress) dest;
    if (jgMbr != null && v != null) {
      List<InternalDistributedMember> members = v.getMembers();
      InternalDistributedMember recipient = null;
      for (InternalDistributedMember mbr : members) {
        GMSMember gmsMbr = ((GMSMember) mbr.getNetMember());
        if (jgMbr.getUUIDLsbs() == gmsMbr.getUuidLSBs()
            && jgMbr.getUUIDMsbs() == gmsMbr.getUuidMSBs()
            && jgMbr.getVmViewId() == gmsMbr.getVmViewId()) {
          recipient = mbr;
          break;
        }
      }
      if (recipient != null) {
        logger.warn("Unable to send message to " + recipient, e);
        services.getHealthMonitor().suspect(recipient,
            "Unable to send messages to this member via JGroups");
      }
    }
  }

  private void establishLocalAddress() {
    UUID logicalAddress = (UUID) myChannel.getAddress();
    logicalAddress = logicalAddress.copy();

    IpAddress ipaddr = (IpAddress) myChannel.down(new Event(Event.GET_PHYSICAL_ADDRESS));

    if (ipaddr != null) {
      this.jgAddress = new JGAddress(logicalAddress, ipaddr);
    } else {
      UDP udp = (UDP) myChannel.getProtocolStack().getTransport();

      try {
        Method getAddress = UDP.class.getDeclaredMethod("getPhysicalAddress");
        getAddress.setAccessible(true);
        ipaddr = (IpAddress) getAddress.invoke(udp, new Object[0]);
        this.jgAddress = new JGAddress(logicalAddress, ipaddr);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        throw new InternalGemFireError(
            "Unable to configure JGroups channel for membership communications", e);
      }
    }

    // install the address in the JGroups channel protocols
    myChannel.down(new Event(Event.SET_LOCAL_ADDRESS, this.jgAddress));

    DistributionConfig config = services.getConfig().getDistributionConfig();
    boolean isLocator = (services.getConfig().getTransport()
        .getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE)
        || !services.getConfig().getDistributionConfig().getStartLocator().isEmpty();

    // establish the DistributedSystem's address
    DurableClientAttributes dca = null;
    if (config.getDurableClientId() != null) {
      dca = new DurableClientAttributes(config.getDurableClientId(),
          config.getDurableClientTimeout());
    }
    MemberAttributes attr = new MemberAttributes(-1/* dcPort - not known at this time */,
        OSProcess.getId(), services.getConfig().getTransport().getVmKind(),
        -1/* view id - not known at this time */, config.getName(),
        MemberAttributes.parseGroups(config.getRoles(), config.getGroups()), dca);
    localAddress = new InternalDistributedMember(jgAddress.getInetAddress(), jgAddress.getPort(),
        config.getEnableNetworkPartitionDetection(), isLocator, attr);

    // add the JGroups logical address to the GMSMember
    UUID uuid = this.jgAddress;
    GMSMember gmsMember = (GMSMember) localAddress.getNetMember();
    gmsMember.setUUID(uuid);
    gmsMember.setMemberWeight((byte) (services.getConfig().getMemberWeight() & 0xff));
    gmsMember.setNetworkPartitionDetectionEnabled(
        services.getConfig().getDistributionConfig().getEnableNetworkPartitionDetection());
  }

  @Override
  public void beSick() {}

  @Override
  public void playDead() {}

  @Override
  public void beHealthy() {}

  @Override
  public void addHandler(Class c, MessageHandler h) {
    handlers.put(c, h);
  }

  @Override
  public boolean testMulticast(long timeout) throws InterruptedException {
    long pongsSnapshot = pongsReceived.longValue();
    JGAddress dest = null;
    try {
      // noinspection ConstantConditions
      pingPonger.sendPingMessage(myChannel, jgAddress, dest);
    } catch (Exception e) {
      logger.warn("unable to send multicast message: {}",
          (jgAddress == null ? "multicast recipients" : jgAddress), e.getMessage());
      return false;
    }
    long giveupTime = System.currentTimeMillis() + timeout;
    while (pongsReceived.longValue() == pongsSnapshot && System.currentTimeMillis() < giveupTime) {
      Thread.sleep(100);
    }
    return pongsReceived.longValue() > pongsSnapshot;
  }

  @Override
  public void getMessageState(InternalDistributedMember target, Map<String, Long> state,
      boolean includeMulticast) {
    if (includeMulticast) {
      NAKACK2 nakack = (NAKACK2) myChannel.getProtocolStack().findProtocol("NAKACK2");
      if (nakack != null) {
        long seqno = nakack.getCurrentSeqno();
        state.put("JGroups.mcastState", seqno);
      }
    }
  }

  @Override
  public void waitForMessageState(InternalDistributedMember sender, Map<String, Long> state)
      throws InterruptedException {
    Long seqno = state.get("JGroups.mcastState");
    if (seqno == null) {
      return;
    }
    long timeout = services.getConfig().getDistributionConfig().getAckWaitThreshold() * 1000L;
    long startTime = System.currentTimeMillis();
    long warnTime = startTime + timeout;
    long quitTime = warnTime + timeout - 1000L;
    boolean warned = false;

    for (;;) {
      String received = "none";
      long highSeqno = 0;
      synchronized (scheduledMcastSeqnos) {
        MessageTracker tracker = scheduledMcastSeqnos.get(sender);
        if (tracker == null) { // no longer in the membership view
          break;
        }
        highSeqno = tracker.get();
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "waiting for multicast messages from {}.  Current seqno={} and expected seqno={}",
            sender, highSeqno, seqno);
      }
      if (highSeqno >= seqno) {
        break;
      }
      long now = System.currentTimeMillis();
      if (!warned && now >= warnTime) {
        warned = true;
        received = String.valueOf(highSeqno);
        logger.warn(
            "{} seconds have elapsed while waiting for multicast messages from {}.  Received {} but expecting at least {}.",
            Long.toString((warnTime - startTime) / 1000L), sender, received, seqno);
      }
      if (now >= quitTime) {
        throw new GemFireIOException("Multicast operations from " + sender
            + " did not distribute within " + (now - startTime) + " milliseconds");
      }
      Thread.sleep(50);
    }
  }

  @Override
  public Set<InternalDistributedMember> sendUnreliably(DistributionMessage msg) {
    return send(msg, false);
  }

  @Override
  public Set<InternalDistributedMember> send(DistributionMessage msg) {
    return send(msg, true);
  }

  private Set<InternalDistributedMember> send(DistributionMessage msg, boolean reliably) {

    // perform the same jgroups messaging as in 8.2's GMSMembershipManager.send() method

    // BUT: when marshalling messages we need to include the version of the product and
    // localAddress at the beginning of the message. These should be used in the receiver
    // code to create a versioned input stream, read the sender address, then read the message
    // and set its sender address
    DMStats theStats = services.getStatistics();
    NetView oldView = this.view;

    if (!myChannel.isConnected()) {
      logger.info("JGroupsMessenger channel is closed - messaging is not possible");
      throw new DistributedSystemDisconnectedException("Distributed System is shutting down");
    }

    filterOutgoingMessage(msg);

    // JGroupsMessenger does not support direct-replies, so register
    // the message's processor if necessary
    if ((msg instanceof DirectReplyMessage) && msg.isDirectAck() && msg.getProcessorId() <= 0) {
      ((DirectReplyMessage) msg).registerProcessor();
    }

    InternalDistributedMember[] destinations = msg.getRecipients();
    boolean allDestinations = msg.forAll();

    boolean useMcast = false;
    if (services.getConfig().getTransport().isMcastEnabled()) {
      if (msg.getMulticast() || allDestinations) {
        useMcast = services.getManager().isMulticastAllowed();
      }
    }

    if (logger.isDebugEnabled() && reliably) {
      String recips = useMcast ? "multicast" : Arrays.toString(msg.getRecipients());
      logger.debug("sending via JGroups: [{}] recipients: {}", msg, recips);
    }

    JGAddress local = this.jgAddress;

    if (useMcast) {

      long startSer = theStats.startMsgSerialization();
      Message jmsg = createJGMessage(msg, local, Version.CURRENT_ORDINAL);
      theStats.endMsgSerialization(startSer);

      Exception problem;
      try {
        jmsg.setTransientFlag(TransientFlag.DONT_LOOPBACK);
        if (!reliably) {
          jmsg.setFlag(Message.Flag.NO_RELIABILITY);
        }
        theStats.incSentBytes(jmsg.getLength());
        logger.trace("Sending JGroups message: {}", jmsg);
        myChannel.send(jmsg);
      } catch (Exception e) {
        logger.debug("caught unexpected exception", e);
        Throwable cause = e.getCause();
        if (cause instanceof ForcedDisconnectException) {
          problem = (Exception) cause;
        } else {
          problem = e;
        }
        if (services.getShutdownCause() != null) {
          Throwable shutdownCause = services.getShutdownCause();
          // If ForcedDisconnectException occurred then report it as actual
          // problem.
          if (shutdownCause instanceof ForcedDisconnectException) {
            problem = (Exception) shutdownCause;
          } else {
            Throwable ne = problem;
            while (ne.getCause() != null) {
              ne = ne.getCause();
            }
            ne.initCause(services.getShutdownCause());
          }
        }
        final String channelClosed =
            "Channel closed";
        throw new DistributedSystemDisconnectedException(channelClosed, problem);
      }
    } // useMcast
    else { // ! useMcast
      int len = destinations.length;
      List<GMSMember> calculatedMembers; // explicit list of members
      int calculatedLen; // == calculatedMembers.len
      if (len == 1 && destinations[0] == DistributionMessage.ALL_RECIPIENTS) { // send to all
        // Grab a copy of the current membership
        NetView v = services.getJoinLeave().getView();

        // Construct the list
        calculatedLen = v.size();
        calculatedMembers = new LinkedList<GMSMember>();
        for (int i = 0; i < calculatedLen; i++) {
          InternalDistributedMember m = (InternalDistributedMember) v.get(i);
          calculatedMembers.add((GMSMember) m.getNetMember());
        }
      } // send to all
      else { // send to explicit list
        calculatedLen = len;
        calculatedMembers = new LinkedList<GMSMember>();
        for (int i = 0; i < calculatedLen; i++) {
          calculatedMembers.add((GMSMember) destinations[i].getNetMember());
        }
      } // send to explicit list
      Int2ObjectOpenHashMap<Message> messages = new Int2ObjectOpenHashMap<>();
      long startSer = theStats.startMsgSerialization();
      boolean firstMessage = true;
      for (GMSMember mbr : calculatedMembers) {
        short version = mbr.getVersionOrdinal();
        if (!messages.containsKey(version)) {
          Message jmsg = createJGMessage(msg, local, version);
          messages.put(version, jmsg);
          if (firstMessage) {
            theStats.incSentBytes(jmsg.getLength());
            firstMessage = false;
          }
        }
      }
      theStats.endMsgSerialization(startSer);
      Collections.shuffle(calculatedMembers);
      int i = 0;
      for (GMSMember mbr : calculatedMembers) {
        JGAddress to = new JGAddress(mbr);
        short version = mbr.getVersionOrdinal();
        Message jmsg = messages.get(version);
        Exception problem = null;
        try {
          Message tmp = (i < (calculatedLen - 1)) ? jmsg.copy(true) : jmsg;
          if (!reliably) {
            jmsg.setFlag(Message.Flag.NO_RELIABILITY);
          }
          tmp.setDest(to);
          tmp.setSrc(this.jgAddress);
          logger.trace("Unicasting to {}", to);
          myChannel.send(tmp);
        } catch (Exception e) {
          problem = e;
        }
        if (problem != null) {
          Throwable cause = services.getShutdownCause();
          if (cause != null) {
            // If ForcedDisconnectException occurred then report it as actual
            // problem.
            if (cause instanceof ForcedDisconnectException) {
              problem = (Exception) cause;
            } else {
              Throwable ne = problem;
              while (ne.getCause() != null) {
                ne = ne.getCause();
              }
              ne.initCause(cause);
            }
          }
          final String channelClosed =
              "Channel closed";
          throw new DistributedSystemDisconnectedException(channelClosed, problem);
        }
      } // send individually
    } // !useMcast

    // The contract is that every destination enumerated in the
    // message should have received the message. If one left
    // (i.e., left the view), we signal it here.
    if (msg.forAll()) {
      return Collections.emptySet();
    }
    Set<InternalDistributedMember> result = new HashSet<>();
    NetView newView = this.view;
    if (newView != null && newView != oldView) {
      for (InternalDistributedMember d : destinations) {
        if (!newView.contains(d)) {
          logger.debug("messenger: member has left the view: {}  view is now {}", d, newView);
          result.add(d);
        }
      }
    }
    return result;
  }

  /**
   * This is the constructor to use to create a JGroups message holding a GemFire
   * DistributionMessage. It sets the appropriate flags in the Message and properly serializes the
   * DistributionMessage for the recipient's product version
   *
   * @param gfmsg the DistributionMessage
   * @param src the sender address
   * @param version the version of the recipient
   * @return the new message
   */
  Message createJGMessage(DistributionMessage gfmsg, JGAddress src, short version) {
    if (gfmsg instanceof DirectReplyMessage) {
      ((DirectReplyMessage) gfmsg).registerProcessor();
    }
    Message msg = new Message();
    msg.setDest(null);
    msg.setSrc(src);
    setMessageFlags(gfmsg, msg);
    try {
      long start = services.getStatistics().startMsgSerialization();
      HeapDataOutputStream out_stream =
          new HeapDataOutputStream(Version.fromOrdinalOrCurrent(version));
      Version.CURRENT.writeOrdinal(out_stream, true);
      if (encrypt != null) {
        out_stream.writeBoolean(true);
        writeEncryptedMessage(gfmsg, version, out_stream);
      } else {
        out_stream.writeBoolean(false);
        serializeMessage(gfmsg, out_stream);
      }

      msg.setBuffer(out_stream.toByteArray());
      services.getStatistics().endMsgSerialization(start);
    } catch (IOException | GemFireIOException ex) {
      logger.warn("Error serializing message", ex);
      if (ex instanceof GemFireIOException) {
        throw (GemFireIOException) ex;
      } else {
        GemFireIOException ioe = new GemFireIOException("Error serializing message");
        ioe.initCause(ex);
        throw ioe;
      }
    } catch (Exception ex) {
      logger.warn("Error serializing message", ex);
      GemFireIOException ioe = new GemFireIOException("Error serializing message");
      ioe.initCause(ex.getCause());
      throw ioe;
    }
    return msg;
  }

  void writeEncryptedMessage(DistributionMessage gfmsg, short version, HeapDataOutputStream out)
      throws Exception {
    long start = services.getStatistics().startUDPMsgEncryption();
    try {
      InternalDataSerializer.writeDSFIDHeader(gfmsg.getDSFID(), out);
      byte[] pk = null;
      int requestId = 0;
      InternalDistributedMember pkMbr = null;
      switch (gfmsg.getDSFID()) {
        case FIND_COORDINATOR_REQ:
        case JOIN_REQUEST:
          // need to append mine PK
          pk = encrypt.getPublicKey(localAddress);

          pkMbr = gfmsg.getRecipients()[0];
          requestId = getRequestId(gfmsg, true);
          break;
        case FIND_COORDINATOR_RESP:
        case JOIN_RESPONSE:
          pkMbr = gfmsg.getRecipients()[0];
          requestId = getRequestId(gfmsg, false);
        default:
          break;
      }
      logger.debug("writeEncryptedMessage gfmsg.getDSFID() = {}  for {} with requestid  {}",
          gfmsg.getDSFID(), pkMbr, requestId);
      out.writeInt(requestId);
      if (pk != null) {
        InternalDataSerializer.writeByteArray(pk, out);
      }

      HeapDataOutputStream out_stream =
          new HeapDataOutputStream(Version.fromOrdinalOrCurrent(version));
      byte[] messageBytes = serializeMessage(gfmsg, out_stream);

      if (pkMbr != null) {
        // using members private key
        messageBytes = encrypt.encryptData(messageBytes, pkMbr);
      } else {
        // using cluster secret key
        messageBytes = encrypt.encryptData(messageBytes);
      }
      InternalDataSerializer.writeByteArray(messageBytes, out);
    } finally {
      services.getStatistics().endUDPMsgEncryption(start);
    }
  }

  int getRequestId(DistributionMessage gfmsg, boolean add) {
    int requestId = 0;
    if (gfmsg instanceof FindCoordinatorRequest) {
      requestId = ((FindCoordinatorRequest) gfmsg).getRequestId();
    } else if (gfmsg instanceof JoinRequestMessage) {
      requestId = ((JoinRequestMessage) gfmsg).getRequestId();
    } else if (gfmsg instanceof FindCoordinatorResponse) {
      requestId = ((FindCoordinatorResponse) gfmsg).getRequestId();
    } else if (gfmsg instanceof JoinResponseMessage) {
      requestId = ((JoinResponseMessage) gfmsg).getRequestId();
    }

    if (add) {
      addRequestId(requestId, gfmsg.getRecipients()[0]);
    }

    return requestId;
  }

  byte[] serializeMessage(DistributionMessage gfmsg, HeapDataOutputStream out_stream)
      throws IOException {
    GMSMember m = (GMSMember) this.localAddress.getNetMember();
    m.writeEssentialData(out_stream);
    DataSerializer.writeObject(gfmsg, out_stream);

    return out_stream.toByteArray();
  }

  void setMessageFlags(DistributionMessage gfmsg, Message msg) {
    // Bundling is mostly only useful if we're doing no-ack work,
    // which is fairly rare
    msg.setFlag(Flag.DONT_BUNDLE);

    if (gfmsg.getProcessorType() == ClusterDistributionManager.HIGH_PRIORITY_EXECUTOR
        || gfmsg instanceof HighPriorityDistributionMessage || AlertAppender.isThreadAlerting()) {
      msg.setFlag(Flag.OOB);
      msg.setFlag(Flag.NO_FC);
      msg.setFlag(Flag.SKIP_BARRIER);
    }

    if (gfmsg instanceof DistributedCacheOperation.CacheOperationMessage) {
      // we don't want to see our own cache operation messages
      msg.setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
    }
  }


  /**
   * deserialize a jgroups payload. If it's a DistributionMessage find the ID of the sender and
   * establish it as the message's sender
   */
  Object readJGMessage(Message jgmsg) {
    Object result = null;

    int messageLength = jgmsg.getLength();

    if (logger.isTraceEnabled()) {
      logger.trace("deserializing a message of length " + messageLength);
    }

    if (messageLength == 0) {
      // jgroups messages with no payload are used for protocol interchange, such
      // as STABLE_GOSSIP
      logger.trace("message length is zero - ignoring");
      return null;
    }

    Exception problem = null;
    byte[] buf = jgmsg.getRawBuffer();
    try {
      long start = services.getStatistics().startMsgDeserialization();

      DataInputStream dis =
          new DataInputStream(new ByteArrayInputStream(buf, jgmsg.getOffset(), jgmsg.getLength()));

      short ordinal = Version.readOrdinal(dis);

      if (ordinal < Version.CURRENT_ORDINAL) {
        dis = new VersionedDataInputStream(dis, Version.fromOrdinalNoThrow(ordinal, true));
      }

      // read
      boolean isEncrypted = dis.readBoolean();

      if (isEncrypted && encrypt == null) {
        throw new GemFireConfigException("Got remote message as encrypted");
      }

      if (isEncrypted) {
        result = readEncryptedMessage(dis, ordinal, encrypt);
      } else {
        result = deserializeMessage(dis, ordinal);
      }


      services.getStatistics().endMsgDeserialization(start);
    } catch (ClassNotFoundException | IOException | RuntimeException e) {
      problem = e;
    } catch (Exception e) {
      problem = e;
    }
    if (problem != null) {
      logger.error(String.format("Exception deserializing message payload: %s", jgmsg),
          problem);
      return null;
    }

    return result;
  }

  void setSender(DistributionMessage dm, GMSMember m, short ordinal) {
    InternalDistributedMember sender = null;
    // JoinRequestMessages are sent with an ID that may have been
    // reused from a previous life by way of auto-reconnect,
    // so we don't want to find a canonical reference for the
    // request's sender ID
    if (dm.getDSFID() == JOIN_REQUEST) {
      sender = ((JoinRequestMessage) dm).getMemberID();
    } else {
      sender = getMemberFromView(m, ordinal);
    }
    dm.setSender(sender);
  }

  @SuppressWarnings("resource")
  DistributionMessage readEncryptedMessage(DataInputStream dis, short ordinal,
      GMSEncrypt encryptLocal) throws Exception {
    int dfsid = InternalDataSerializer.readDSFIDHeader(dis);
    int requestId = dis.readInt();
    long start = services.getStatistics().startUDPMsgDecryption();
    try {
      logger.debug("readEncryptedMessage Reading Request id " + dfsid + " and requestid is "
          + requestId + " myid " + this.localAddress);
      InternalDistributedMember pkMbr = null;
      boolean readPK = false;
      switch (dfsid) {
        case FIND_COORDINATOR_REQ:
        case JOIN_REQUEST:
          readPK = true;
          break;
        case FIND_COORDINATOR_RESP:
        case JOIN_RESPONSE:
          // this will have requestId to know the PK
          pkMbr = getRequestedMember(requestId);
          break;
      }

      byte[] data;

      byte[] pk = null;

      if (readPK) {
        pk = InternalDataSerializer.readByteArray(dis);
        data = InternalDataSerializer.readByteArray(dis);
        // using prefixed pk from sender
        data = encryptLocal.decryptData(data, pk);
      } else {
        data = InternalDataSerializer.readByteArray(dis);
        // from cluster key
        if (pkMbr != null) {
          // using member public key
          data = encryptLocal.decryptData(data, pkMbr);
        } else {
          // from cluster key
          data = encryptLocal.decryptData(data);
        }
      }

      {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

        if (ordinal < Version.CURRENT_ORDINAL) {
          in = new VersionedDataInputStream(in, Version.fromOrdinalNoThrow(ordinal, true));
        }

        DistributionMessage result = deserializeMessage(in, ordinal);

        if (pk != null) {
          logger.info("Setting public key for " + result.getSender() + " len " + pk.length);
          setPublicKey(pk, result.getSender());
        }

        return result;
      }
    } catch (Exception e) {
      throw new Exception("Message id is " + dfsid, e);
    } finally {
      services.getStatistics().endUDPMsgDecryption(start);
    }

  }

  DistributionMessage deserializeMessage(DataInputStream in, short ordinal)
      throws ClassNotFoundException, IOException {
    GMSMember m = new GMSMember();
    m.readEssentialData(in);
    DistributionMessage result = DataSerializer.readObject(in);

    setSender(result, m, ordinal);

    return result;
  }

  /** look for certain messages that may need to be altered before being sent */
  void filterOutgoingMessage(DistributionMessage m) {
    switch (m.getDSFID()) {
      case JOIN_RESPONSE:
        JoinResponseMessage jrsp = (JoinResponseMessage) m;

        if (jrsp.getRejectionMessage() == null
            && services.getConfig().getTransport().isMcastEnabled()) {
          // get the multicast message digest and pass it with the join response
          Digest digest = (Digest) this.myChannel.getProtocolStack().getTopProtocol()
              .down(Event.GET_DIGEST_EVT);
          HeapDataOutputStream hdos = new HeapDataOutputStream(500, Version.CURRENT);
          try {
            digest.writeTo(hdos);
          } catch (Exception e) {
            logger.fatal("Unable to serialize JGroups messaging digest", e);
          }
          jrsp.setMessengerData(hdos.toByteArray());
        }
        break;
      default:
        break;
    }
  }

  void filterIncomingMessage(DistributionMessage m) {
    switch (m.getDSFID()) {
      case JOIN_RESPONSE:
        JoinResponseMessage jrsp = (JoinResponseMessage) m;

        if (jrsp.getRejectionMessage() == null
            && services.getConfig().getTransport().isMcastEnabled()) {
          byte[] serializedDigest = jrsp.getMessengerData();
          ByteArrayInputStream bis = new ByteArrayInputStream(serializedDigest);
          DataInputStream dis = new DataInputStream(bis);
          try {
            Digest digest = new Digest();
            digest.readFrom(dis);
            logger.trace("installing JGroups message digest {}", digest);
            this.myChannel.getProtocolStack().getTopProtocol()
                .down(new Event(Event.MERGE_DIGEST, digest));
            jrsp.setMessengerData(null);
          } catch (Exception e) {
            logger.fatal("Unable to read JGroups messaging digest", e);
          }
        }
        break;
      default:
        break;
    }
  }

  @Override
  public InternalDistributedMember getMemberID() {
    return localAddress;
  }

  /**
   * returns the JGroups configuration string, for testing
   */
  public String getJGroupsStackConfig() {
    return this.jgStackConfig;
  }

  /**
   * returns the pinger, for testing
   */
  public GMSPingPonger getPingPonger() {
    return this.pingPonger;
  }

  /**
   * for unit testing we need to replace UDP with a fake UDP protocol
   */
  public void setJGroupsStackConfigForTesting(String config) {
    this.jgStackConfig = config;
  }

  /**
   * returns the member ID for the given GMSMember object
   */
  @SuppressWarnings("UnusedParameters")
  private InternalDistributedMember getMemberFromView(GMSMember jgId, short version) {
    return this.services.getJoinLeave().getMemberID(jgId);
  }


  @Override
  public void emergencyClose() {
    this.view = null;
    if (localAddress.getVmViewId() >= 0) {
      // keep track of old addresses that were used to successfully join the cluster
      usedDistributedMemberIdentifiers.add(localAddress);
    }
    if (this.myChannel != null) {
      if ((services.isShutdownDueToForcedDisconnect() && services.isAutoReconnectEnabled())
          || services.getManager().isReconnectingDS()) {
      } else {
        this.myChannel.disconnect();
      }
    }
  }

  public QuorumChecker getQuorumChecker() {
    NetView view = this.view;
    if (view == null) {
      view = services.getJoinLeave().getView();
      if (view == null) {
        view = services.getJoinLeave().getPreviousView();
        if (view == null) {
          return null;
        }
      }
    }
    GMSQuorumChecker qc =
        new GMSQuorumChecker(view, services.getConfig().getLossThreshold(), this.myChannel,
            usedDistributedMemberIdentifiers);
    qc.initialize();
    return qc;
  }

  /**
   * JGroupsReceiver receives incoming JGroups messages and passes them to a handler. It may be
   * accessed through JChannel.getReceiver().
   */
  class JGroupsReceiver extends ReceiverAdapter {

    @Override
    public void receive(Message jgmsg) {
      long startTime = DistributionStats.getStatTime();
      try {
        if (services.getManager().shutdownInProgress()) {
          return;
        }

        if (logger.isTraceEnabled()) {
          logger.trace("JGroupsMessenger received {} headers: {}", jgmsg, jgmsg.getHeaders());
        }

        // Respond to ping messages sent from other systems that are in a auto reconnect state
        byte[] contents = jgmsg.getBuffer();
        if (contents == null) {
          return;
        }
        if (pingPonger.isPingMessage(contents)) {
          try {
            pingPonger.sendPongMessage(myChannel, jgAddress, jgmsg.getSrc());
          } catch (Exception e) {
            logger.info("Failed sending Pong response to " + jgmsg.getSrc());
          }
          return;
        } else if (pingPonger.isPongMessage(contents)) {
          pongsReceived.incrementAndGet();
          return;
        }

        Object o = readJGMessage(jgmsg);
        if (o == null) {
          return;
        }

        DistributionMessage msg = (DistributionMessage) o;
        assert msg.getSender() != null;

        // admin-only VMs don't have caches, so we ignore cache operations
        // multicast to them, avoiding deserialization cost and classpath
        // problems
        if ((services.getConfig().getTransport()
            .getVmKind() == ClusterDistributionManager.ADMIN_ONLY_DM_TYPE)
            && (msg instanceof DistributedCacheOperation.CacheOperationMessage)) {
          return;
        }

        msg.resetTimestamp();
        msg.setBytesRead(jgmsg.getLength());

        try {

          if (logger.isTraceEnabled()) {
            logger.trace("JGroupsMessenger dispatching {} from {}", msg, msg.getSender());
          }
          filterIncomingMessage(msg);
          getMessageHandler(msg).processMessage(msg);

          // record the scheduling of broadcast messages
          NakAckHeader2 header = (NakAckHeader2) jgmsg.getHeader(nackack2HeaderId);
          if (header != null && !jgmsg.isFlagSet(Flag.OOB)) {
            recordScheduledSeqno(msg.getSender(), header.getSeqno());
          }

        } catch (MemberShunnedException e) {
          // message from non-member - ignore
        }

      } finally {
        long delta = DistributionStats.getStatTime() - startTime;
        JGroupsMessenger.this.services.getStatistics().incUDPDispatchRequestTime(delta);
      }
    }

    private void recordScheduledSeqno(DistributedMember member, long seqno) {
      synchronized (scheduledMcastSeqnos) {
        MessageTracker counter = scheduledMcastSeqnos.get(member);
        if (counter == null) {
          counter = new MessageTracker(seqno);
          scheduledMcastSeqnos.put(member, counter);
        }
        counter.record(seqno);
      }
    }

    /**
     * returns the handler that should process the given message. The default handler is the
     * membership manager
     */
    private MessageHandler getMessageHandler(DistributionMessage msg) {
      Class<?> msgClazz = msg.getClass();
      MessageHandler h = handlers.get(msgClazz);
      if (h == null) {
        for (Class<?> clazz : handlers.keySet()) {
          if (clazz.isAssignableFrom(msgClazz)) {
            h = handlers.get(clazz);
            handlers.put(msg.getClass(), h);
            break;
          }
        }
      }
      if (h == null) {
        h = services.getManager();
      }
      return h;
    }
  }

  @Override
  public Set<InternalDistributedMember> send(DistributionMessage msg, NetView alternateView) {
    if (this.encrypt != null) {
      this.encrypt.installView(alternateView);
    }
    return send(msg, true);
  }

  @Override
  public byte[] getPublicKey(InternalDistributedMember mbr) {
    if (encrypt != null) {
      return encrypt.getPublicKey(mbr);
    }
    return null;
  }

  @Override
  public void setPublicKey(byte[] publickey, InternalDistributedMember mbr) {
    if (encrypt != null) {
      logger.debug("Setting PK for member " + mbr);
      encrypt.setPublicKey(publickey, mbr);
    }
  }

  @Override
  public void setClusterSecretKey(byte[] clusterSecretKey) {
    if (encrypt != null) {
      logger.debug("Setting cluster key");
      encrypt.setClusterKey(clusterSecretKey);
    }
  }

  @Override
  public byte[] getClusterSecretKey() {
    if (encrypt != null) {
      return encrypt.getClusterSecretKey();
    }
    return null;
  }

  private AtomicInteger requestId = new AtomicInteger((new Random().nextInt()));
  private HashMap<Integer, InternalDistributedMember> requestIdVsRecipients = new HashMap<>();

  InternalDistributedMember getRequestedMember(int requestId) {
    return requestIdVsRecipients.remove(requestId);
  }

  void addRequestId(int requestId, InternalDistributedMember mbr) {
    requestIdVsRecipients.put(requestId, mbr);
  }

  @Override
  public int getRequestId() {
    return requestId.incrementAndGet();
  }

  @Override
  public void initClusterKey() {
    if (encrypt != null) {
      try {
        logger.info("Initializing cluster key");
        encrypt.initClusterSecretKey();
      } catch (Exception e) {
        throw new RuntimeException("unable to create cluster key ", e);
      }
    }
  }

  static class MessageTracker {
    long highestSeqno;

    MessageTracker(long seqno) {
      highestSeqno = seqno;
    }

    long get() {
      return highestSeqno;
    }

    void record(long seqno) {
      if (seqno > highestSeqno) {
        highestSeqno = seqno;
      }
    }
  }
}
