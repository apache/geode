/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import static com.gemstone.gemfire.distributed.internal.membership.gms.GMSUtil.replaceStrings;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.JOIN_REQUEST;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.JOIN_RESPONSE;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Digest;
import org.jgroups.util.UUID;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.DurableClientAttributes;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.QuorumChecker;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.MessageHandler;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Messenger;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinResponseMessage;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataInputStream;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;
import com.gemstone.gemfire.internal.cache.DistributedCacheOperation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.tcp.MemberShunnedException;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class JGroupsMessenger implements Messenger {

  private static final Logger logger = Services.getLogger();

  /**
   * The location (in the product) of the locator Jgroups config file.
   */
  private static final String DEFAULT_JGROUPS_TCP_CONFIG = "com/gemstone/gemfire/distributed/internal/membership/gms/messenger/jgroups-config.xml";

  /**
   * The location (in the product) of the mcast Jgroups config file.
   */
  private static final String JGROUPS_MCAST_CONFIG_FILE_NAME = "com/gemstone/gemfire/distributed/internal/membership/gms/messenger/jgroups-mcast.xml";

  /** JG magic numbers for types added to the JG ClassConfigurator */
  public static final short JGROUPS_TYPE_JGADDRESS = 2000;
  public static final short JGROUPS_PROTOCOL_TRANSPORT = 1000;

  public static boolean THROW_EXCEPTION_ON_START_HOOK;

  String jgStackConfig;
  
  JChannel myChannel;
  InternalDistributedMember localAddress;
  JGAddress jgAddress;
  Services services;

  /** handlers that receive certain classes of messages instead of the Manager */
  Map<Class, MessageHandler> handlers = new ConcurrentHashMap<Class, MessageHandler>();
  
  private volatile NetView view;

  private GMSPingPonger pingPonger = new GMSPingPonger();
  
  protected AtomicLong pongsReceived = new AtomicLong(0);
  
  /**
   * A set that contains addresses that we have logged JGroups IOExceptions for in the
   * current membership view and possibly initiated suspect processing.  This
   * reduces the amount of suspect processing initiated by IOExceptions and the
   * amount of exceptions logged
   */
  private Set<Address> addressesWithioExceptionsProcessed = Collections.synchronizedSet(new HashSet<Address>());
  
  static {
    // register classes that we've added to jgroups that are put on the wire
    // or need a header ID
    ClassConfigurator.add(JGROUPS_TYPE_JGADDRESS, JGAddress.class);
    ClassConfigurator.addProtocol(JGROUPS_PROTOCOL_TRANSPORT, Transport.class);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void init(Services s) {
    this.services = s;

    RemoteTransportConfig transport = services.getConfig().getTransport();
    DistributionConfig dc = services.getConfig().getDistributionConfig();
    

    boolean b = dc.getEnableNetworkPartitionDetection();
    if (b) {
      if (!SocketCreator.FORCE_DNS_USE) {
        SocketCreator.resolve_dns = false;
      }
    }
    System.setProperty("jgroups.resolve_dns", String.valueOf(!b));

    InputStream is= null;

    String r = null;
    if (transport.isMcastEnabled()) {
      r = JGROUPS_MCAST_CONFIG_FILE_NAME;
    } else {
      r = DEFAULT_JGROUPS_TCP_CONFIG;
    }
    is = ClassPathLoader.getLatest().getResourceAsStream(getClass(), r);
    if (is == null) {
      throw new GemFireConfigException(LocalizedStrings.GroupMembershipService_CANNOT_FIND_0.toLocalizedString(r));
    }

    String properties;
    try {
      //PlainConfigurator config = PlainConfigurator.getInstance(is);
      //properties = config.getProtocolStackString();
      StringBuffer sb = new StringBuffer(3000);
      BufferedReader br;
      br = new BufferedReader(new InputStreamReader(is, "US-ASCII"));
      String input;
      while ((input=br.readLine()) != null) {
        sb.append(input);
      }
      br.close();
      properties = sb.toString();
    }
    catch (Exception ex) {
      throw new GemFireConfigException(LocalizedStrings.GroupMembershipService_AN_EXCEPTION_WAS_THROWN_WHILE_READING_JGROUPS_CONFIG.toLocalizedString(), ex);
    }
    
    
    if (transport.isMcastEnabled()) {
      properties = replaceStrings(properties, "MCAST_PORT", String.valueOf(transport.getMcastId().getPort()));
      properties = replaceStrings(properties, "MCAST_ADDRESS", dc.getMcastAddress().getHostAddress());
      properties = replaceStrings(properties, "MCAST_TTL", String.valueOf(dc.getMcastTtl()));
      properties = replaceStrings(properties, "MCAST_SEND_BUFFER_SIZE", String.valueOf(dc.getMcastSendBufferSize()));
      properties = replaceStrings(properties, "MCAST_RECV_BUFFER_SIZE", String.valueOf(dc.getMcastRecvBufferSize()));
      properties = replaceStrings(properties, "MCAST_RETRANSMIT_INTERVAL", ""+Integer.getInteger("gemfire.mcast-retransmit-interval", 500));
      properties = replaceStrings(properties, "RETRANSMIT_LIMIT", String.valueOf(dc.getUdpFragmentSize()-256));
    }

    if (transport.isMcastEnabled() || transport.isTcpDisabled() ||
        (dc.getUdpRecvBufferSize() != DistributionConfig.DEFAULT_UDP_RECV_BUFFER_SIZE) ) {
      properties = replaceStrings(properties, "UDP_RECV_BUFFER_SIZE", ""+dc.getUdpRecvBufferSize());
    }
    else {
      properties = replaceStrings(properties, "UDP_RECV_BUFFER_SIZE", ""+DistributionConfig.DEFAULT_UDP_RECV_BUFFER_SIZE_REDUCED);
    }
    properties = replaceStrings(properties, "UDP_SEND_BUFFER_SIZE", ""+dc.getUdpSendBufferSize());

    String str = transport.getBindAddress();
    // JGroups UDP protocol requires a bind address
    if (str == null || str.length() == 0) {
      try {
        str = SocketCreator.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        throw new GemFireConfigException(e.getMessage(), e);
      }
    }
    properties = replaceStrings(properties, "BIND_ADDR_SETTING", "bind_addr=\""+str+"\"");

    int port = Integer.getInteger("gemfire.jg-bind-port", 0);
    if (port != 0) {
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE_START", ""+port);
    } else {
      int[] ports = dc.getMembershipPortRange();
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE_START", ""+ports[0]);
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE_END", ""+(ports[1]-ports[0]));
    }
    
    properties = replaceStrings(properties, "UDP_FRAGMENT_SIZE", ""+dc.getUdpFragmentSize());
    
    properties = replaceStrings(properties, "FC_MAX_CREDITS", ""+dc.getMcastFlowControl().getByteAllowance());
    properties = replaceStrings(properties, "FC_THRESHOLD", ""+dc.getMcastFlowControl().getRechargeThreshold());
    properties = replaceStrings(properties, "FC_MAX_BLOCK", ""+dc.getMcastFlowControl().getRechargeBlockMs());

    this.jgStackConfig = properties;
    
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void start() {
    // create the configuration XML string for JGroups
    String properties = this.jgStackConfig;
    
    long start = System.currentTimeMillis();
    
    // start the jgroups channel and establish the membership ID
    boolean reconnecting = false;
    try {
      Object oldChannel = services.getConfig().getTransport().getOldDSMembershipInfo();
      if (oldChannel != null) {
        logger.debug("Reusing JGroups channel from previous system", properties);
        
        myChannel = (JChannel)oldChannel;
        // scrub the old channel
        ViewId vid = new ViewId(new JGAddress(), 0);
        View jgv = new View(vid, new ArrayList<Address>());
        this.myChannel.down(new Event(Event.VIEW_CHANGE, jgv));
        UUID logicalAddress = (UUID)myChannel.getAddress();
        if (logicalAddress instanceof JGAddress) {
          ((JGAddress)logicalAddress).setVmViewId(-1);
        }
        reconnecting = true;
      }
      else {
        logger.debug("JGroups configuration: {}", properties);
        
        checkForIPv6();
        InputStream is = new ByteArrayInputStream(properties.getBytes("UTF-8"));
        myChannel = new JChannel(is);
      }
    } catch (Exception e) {
      throw new GemFireConfigException("unable to create jgroups channel", e);
    }
    
    // give the stats to the jchannel statistics recorder
    StatRecorder sr = (StatRecorder)myChannel.getProtocolStack().findProtocol(StatRecorder.class);
    if (sr != null) {
      sr.setDMStats(services.getStatistics());
    }
    
    Transport transport = (Transport)myChannel.getProtocolStack().getTransport();
    transport.setMessenger(this);

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
    
    logger.info("JGroups channel {} (took {}ms)", (reconnecting? "reinitialized" : "created"), System.currentTimeMillis()-start);
    
  }
  
  /**
   * JGroups picks an IPv6 address if preferIPv4Stack is false or not set
   * and preferIPv6Addresses is not set or is true.  We want it to use an
   * IPv4 address for a dual-IP stack so that both IPv4 and IPv6 messaging work
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
  public void started() {
  }

  @Override
  public void stop() {
    if (this.myChannel != null) {
      if ((services.isShutdownDueToForcedDisconnect() && services.isAutoReconnectEnabled()) || services.getManager().isReconnectingDS()) {
        // leave the channel open for reconnect attempts
      }
      else {
        this.myChannel.close();
      }
    }
  }

  @Override
  public void stopped() {
  }

  @Override
  public void memberSuspected(InternalDistributedMember initiator, InternalDistributedMember suspect, String reason) {
  }

  @Override
  public void installView(NetView v) {
    this.view = v;
    
    if (this.jgAddress.getVmViewId() < 0) {
      this.jgAddress.setVmViewId(this.localAddress.getVmViewId());
    }
    List<JGAddress> mbrs = new ArrayList<JGAddress>(v.size());
    for (InternalDistributedMember idm: v.getMembers()) {
      mbrs.add(new JGAddress(idm));
    }
    ViewId vid = new ViewId(new JGAddress(v.getCoordinator()), v.getViewId());
    View jgv = new View(vid, new ArrayList<Address>(mbrs));
    logger.trace("installing JGroups view: {}", jgv);
    this.myChannel.down(new Event(Event.VIEW_CHANGE, jgv));

    addressesWithioExceptionsProcessed.clear();
  }
  

  /**
   * If JGroups is unable to send a message it may mean that the network
   * is down.  If so we need to initiate suspect processing on the
   * recipient.<p>
   * see Transport._send()
   */
  public void handleJGroupsIOException(IOException e, Address dest) {
    if (services.getManager().shutdownInProgress()) { // GEODE-634 - don't log IOExceptions during shutdown
      return;
    }
    if (addressesWithioExceptionsProcessed.contains(dest)) {
      return;
    }
    addressesWithioExceptionsProcessed.add(dest);
    NetView v = this.view;
    JGAddress jgMbr = (JGAddress)dest;
    if (jgMbr != null && v != null) {
      List<InternalDistributedMember> members = v.getMembers();
      InternalDistributedMember recipient = null;
      for (InternalDistributedMember mbr: members) {
        GMSMember gmsMbr = ((GMSMember)mbr.getNetMember());
        if (jgMbr.getUUIDLsbs() == gmsMbr.getUuidLSBs()
            && jgMbr.getUUIDMsbs() == gmsMbr.getUuidMSBs()
            && jgMbr.getVmViewId() == gmsMbr.getVmViewId()) {
          recipient = mbr;
          break;
        }
      }
      if (recipient != null) {
        services.getHealthMonitor().checkIfAvailable(recipient,
            "Unable to send messages to this member via JGroups", true);
      }
    }
  }
  
  private void establishLocalAddress() {
    UUID logicalAddress = (UUID)myChannel.getAddress();
    logicalAddress = logicalAddress.copy();
    
    IpAddress ipaddr = (IpAddress)myChannel.down(new Event(Event.GET_PHYSICAL_ADDRESS));
    
    if (ipaddr != null) {
      this.jgAddress = new JGAddress(logicalAddress, ipaddr);
    }
    else {
      UDP udp = (UDP)myChannel.getProtocolStack().getTransport();

      try {
        Method getAddress = UDP.class.getDeclaredMethod("getPhysicalAddress");
        getAddress.setAccessible(true);
        ipaddr = (IpAddress)getAddress.invoke(udp, new Object[0]);
        this.jgAddress = new JGAddress(logicalAddress, ipaddr);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        logger.info("Unable to find getPhysicallAddress method in UDP - parsing its address instead");
      }
      
//      if (this.jgAddress == null) {
//        String addr = udp.getLocalPhysicalAddress();
//        int cidx = addr.lastIndexOf(':');  // IPv6 literals might have colons
//        String host = addr.substring(0, cidx);
//        int jgport = Integer.parseInt(addr.substring(cidx+1, addr.length()));
//        try {
//          this.jgAddress = new JGAddress(logicalAddress, new IpAddress(InetAddress.getByName(host), jgport));
//        } catch (UnknownHostException e) {
//          myChannel.disconnect();
//          throw new SystemConnectException("unable to initialize jgroups address", e);
//        }
//      }
    }
  
    // install the address in the JGroups channel protocols
    myChannel.down(new Event(Event.SET_LOCAL_ADDRESS, this.jgAddress));

    DistributionConfig config = services.getConfig().getDistributionConfig();
    boolean isLocator = (services.getConfig().getTransport().getVmKind() == DistributionManager.LOCATOR_DM_TYPE)
        || !services.getConfig().getDistributionConfig().getStartLocator().isEmpty();
    
    // establish the DistributedSystem's address
    DurableClientAttributes dca = null;
    if (config.getDurableClientId() != null) {
      dca = new DurableClientAttributes(config.getDurableClientId(), config
          .getDurableClientTimeout());
    }
    MemberAttributes attr = new MemberAttributes(
        -1/*dcPort - not known at this time*/,
        OSProcess.getId(),
        services.getConfig().getTransport().getVmKind(),
        -1/*view id - not known at this time*/,
        config.getName(),
        MemberAttributes.parseGroups(config.getRoles(), config.getGroups()),
        dca);
    localAddress = new InternalDistributedMember(jgAddress.getInetAddress(),
        jgAddress.getPort(), config.getEnableNetworkPartitionDetection(),
        isLocator, attr);

    // add the JGroups logical address to the GMSMember
    UUID uuid = this.jgAddress;
    ((GMSMember)localAddress.getNetMember()).setUUID(uuid);
    ((GMSMember)localAddress.getNetMember()).setMemberWeight((byte)(services.getConfig().getMemberWeight() & 0xff));
  }
  
  @Override
  public void beSick() {
  }

  @Override
  public void playDead() {
  }

  @Override
  public void beHealthy() {
  }

  @Override
  public void addHandler(Class c, MessageHandler h) {
    handlers.put(c, h);
  }
  
  @Override
  public boolean testMulticast(long timeout) throws InterruptedException {
    long pongsSnapshot = pongsReceived.longValue();
    JGAddress dest = null;
    try {
      pingPonger.sendPingMessage(myChannel, jgAddress, dest);
    } catch (Exception e) {
      logger.warn("unable to send multicast message: {}", (jgAddress==null? "multicast recipients":jgAddress),
          e.getMessage());
      return false;
    }
    long giveupTime = System.currentTimeMillis() + timeout;
    while (pongsReceived.longValue() == pongsSnapshot && System.currentTimeMillis() < giveupTime) {
      Thread.sleep(100);
    }
    return pongsReceived.longValue() > pongsSnapshot;
  }
  
  @Override
  public void getMessageState(InternalDistributedMember target, Map state, boolean includeMulticast) {
    if (includeMulticast) {
      NAKACK2 nakack = (NAKACK2)myChannel.getProtocolStack().findProtocol("NAKACK2");
      if (nakack != null) {
        long seqno = nakack.getCurrentSeqno();
        state.put("JGroups.mcastState", Long.valueOf(seqno));
      }
    }
  }
  
  @Override
  public void waitForMessageState(InternalDistributedMember sender, Map state) throws InterruptedException {
    NAKACK2 nakack = (NAKACK2)myChannel.getProtocolStack().findProtocol("NAKACK2");
    Long seqno = (Long)state.get("JGroups.mcastState");
    if (nakack != null && seqno != null) {
      waitForMessageState(nakack, sender, seqno);
    }
  }
  
  /**
   * wait for the mcast state from the given member to reach the given seqno 
   */
  protected void waitForMessageState(NAKACK2 nakack, InternalDistributedMember sender, Long seqno)
    throws InterruptedException {
    long timeout = services.getConfig().getDistributionConfig().getAckWaitThreshold() * 1000L;
    long startTime = System.currentTimeMillis();
    long warnTime = startTime + timeout;
    long quitTime = warnTime + timeout - 1000L;
    boolean warned = false;
    
    JGAddress jgSender = new JGAddress(sender);
    
    for (;;) {
      Digest digest = nakack.getDigest(jgSender);
      if (digest == null) {
        return;
      }
      String received = "none";
      long[] senderSeqnos = digest.get(jgSender);
      if (senderSeqnos == null || senderSeqnos[0] >= seqno.longValue()) {
        break;
      }
      long now = System.currentTimeMillis();
      if (!warned && now >= warnTime) {
        warned = true;
        if (senderSeqnos != null) {
          received = String.valueOf(senderSeqnos[0]);
        }
        logger.warn("{} seconds have elapsed while waiting for multicast messages from {}.  Received {} but expecting at least {}.",
            Long.toString((warnTime-startTime)/1000L), sender, received, seqno);
      }
      if (now >= quitTime) {
        throw new GemFireIOException("Multicast operations from " + sender + " did not distribute within " + (now - startTime) + " milliseconds");
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
    
  public Set<InternalDistributedMember> send(DistributionMessage msg, boolean reliably) {
      
    // perform the same jgroups messaging as in 8.2's GMSMembershipManager.send() method

    // BUT: when marshalling messages we need to include the version of the product and
    // localAddress at the beginning of the message.  These should be used in the receiver
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
      ((DirectReplyMessage)msg).registerProcessor();
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
      String recips = useMcast? "multicast" : Arrays.toString(msg.getRecipients());
      logger.debug("sending via JGroups: [{}] recipients: {}", msg, recips);
    }
    
    JGAddress local = this.jgAddress;
    
    if (useMcast) {

      long startSer = theStats.startMsgSerialization();
      Message jmsg = createJGMessage(msg, local, Version.CURRENT_ORDINAL);
      theStats.endMsgSerialization(startSer);

      Exception problem = null;
      try {
        jmsg.setTransientFlag(TransientFlag.DONT_LOOPBACK);
        if (!reliably) {
          jmsg.setFlag(Message.Flag.NO_RELIABILITY);
        }
        theStats.incSentBytes(jmsg.getLength());
        logger.trace("Sending JGroups message: {}", jmsg);
        myChannel.send(jmsg);
      }
      catch (Exception e) {
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
        final String channelClosed = LocalizedStrings.GroupMembershipService_CHANNEL_CLOSED.toLocalizedString();
//        services.getManager().membershipFailure(channelClosed, problem);
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
        for (int i = 0; i < calculatedLen; i ++) {
          InternalDistributedMember m = (InternalDistributedMember)v.get(i);
          calculatedMembers.add((GMSMember)m.getNetMember());
        }
      } // send to all
      else { // send to explicit list
        calculatedLen = len;
        calculatedMembers = new LinkedList<GMSMember>();
        for (int i = 0; i < calculatedLen; i ++) {
          calculatedMembers.add((GMSMember)destinations[i].getNetMember());
        }
      } // send to explicit list
      Int2ObjectOpenHashMap<Message> messages = new Int2ObjectOpenHashMap<>();
      long startSer = theStats.startMsgSerialization();
      boolean firstMessage = true;
      for (Iterator<GMSMember> it=calculatedMembers.iterator(); it.hasNext(); ) {
        GMSMember mbr = it.next();
        short version = mbr.getVersionOrdinal();
        if ( !messages.containsKey(version) ) {
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
      int i=0;
      for (GMSMember mbr: calculatedMembers) {
        JGAddress to = new JGAddress(mbr);
        short version = mbr.getVersionOrdinal();
        Message jmsg = (Message)messages.get(version);
        Exception problem = null;
        try {
          Message tmp = (i < (calculatedLen-1)) ? jmsg.copy(true) : jmsg;
          if (!reliably) {
            jmsg.setFlag(Message.Flag.NO_RELIABILITY);
          }
          tmp.setDest(to);
          tmp.setSrc(this.jgAddress);
          logger.trace("Unicasting to {}", to);
          myChannel.send(tmp);
        }
        catch (Exception e) {
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
          final String channelClosed = LocalizedStrings.GroupMembershipService_CHANNEL_CLOSED.toLocalizedString();
          //          services.getManager().membershipFailure(channelClosed, problem);
          throw new DistributedSystemDisconnectedException(channelClosed, problem);
        }
      } // send individually
    } // !useMcast

    // The contract is that every destination enumerated in the
    // message should have received the message.  If one left
    // (i.e., left the view), we signal it here.
    if (msg.forAll()) {
      return Collections.emptySet();
    }
    Set<InternalDistributedMember> result = new HashSet<InternalDistributedMember>();
    NetView newView = this.view;
    if (newView != null && newView != oldView) {
      for (int i = 0; i < destinations.length; i ++) {
        InternalDistributedMember d = destinations[i];
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
   * DistributionMessage.  It sets the appropriate flags in the Message and properly
   * serializes the DistributionMessage for the recipient's product version
   * 
   * @param gfmsg the DistributionMessage
   * @param src the sender address
   * @param version the version of the recipient
   * @return the new message
   */
  Message createJGMessage(DistributionMessage gfmsg, JGAddress src, short version) {
    if(gfmsg instanceof DirectReplyMessage) {
      ((DirectReplyMessage) gfmsg).registerProcessor();
    }
    Message msg = new Message();
    msg.setDest(null);
    msg.setSrc(src);
    // GemFire uses its own reply processors so there is no need
    // to maintain message order
    msg.setFlag(Flag.OOB);
    // Bundling is mostly only useful if we're doing no-ack work,
    // which is fairly rare
    msg.setFlag(Flag.DONT_BUNDLE);

    //log.info("Creating message with payload " + gfmsg);
    if (gfmsg.getProcessorType() == DistributionManager.HIGH_PRIORITY_EXECUTOR
        || gfmsg instanceof HighPriorityDistributionMessage) {
      msg.setFlag(Flag.NO_FC);
      msg.setFlag(Flag.SKIP_BARRIER);
    }
    if (gfmsg instanceof DistributedCacheOperation.CacheOperationMessage) {
      // we don't want to see our own cache operation messages
      msg.setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
    }
    try {
      long start = services.getStatistics().startMsgSerialization();
      HeapDataOutputStream out_stream =
        new HeapDataOutputStream(Version.fromOrdinalOrCurrent(version));
      Version.CURRENT.writeOrdinal(out_stream, true);
      DataSerializer.writeObject(this.localAddress.getNetMember(), out_stream);
      DataSerializer.writeObject(gfmsg, out_stream);
      msg.setBuffer(out_stream.toByteArray());
      services.getStatistics().endMsgSerialization(start);
    }
    catch(IOException | GemFireIOException ex) {
      logger.warn("Error serializing message", ex);
      if (ex instanceof GemFireIOException) {
        throw (GemFireIOException)ex;
      } else {
        GemFireIOException ioe = new
          GemFireIOException("Error serializing message");
        ioe.initCause(ex);
        throw ioe;
      }
    }
    return msg;
  }


  /**
   * deserialize a jgroups payload.  If it's a DistributionMessage find
   * the ID of the sender and establish it as the message's sender
   */
  Object readJGMessage(Message jgmsg) {
    Object result = null;
    
    int messageLength = jgmsg.getLength();
    
    if (logger.isTraceEnabled()) {
      logger.trace("deserializing a message of length "+messageLength);
    }
    
    if (messageLength == 0) {
      // jgroups messages with no payload are used for protocol interchange, such
      // as STABLE_GOSSIP
      logger.trace("message length is zero - ignoring");
      return null;
    }

    InternalDistributedMember sender = null;

    Exception problem = null;
    byte[] buf = jgmsg.getRawBuffer();
    try {
      long start = services.getStatistics().startMsgDeserialization();
      
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf, 
          jgmsg.getOffset(), jgmsg.getLength()));

      short ordinal = Version.readOrdinal(dis);
      
      if (ordinal < Version.CURRENT_ORDINAL) {
        dis = new VersionedDataInputStream(dis, Version.fromOrdinalNoThrow(
            ordinal, true));
      }
      
      GMSMember m = DataSerializer.readObject(dis);

      result = DataSerializer.readObject(dis);

      DistributionMessage dm = (DistributionMessage)result;
      
      // JoinRequestMessages are sent with an ID that may have been
      // reused from a previous life by way of auto-reconnect,
      // so we don't want to find a canonical reference for the
      // request's sender ID
      if (dm.getDSFID() == JOIN_REQUEST) {
        sender = ((JoinRequestMessage)dm).getMemberID();
      } else {
        sender = getMemberFromView(m, ordinal);
      }
      ((DistributionMessage)result).setSender(sender);
      
      services.getStatistics().endMsgDeserialization(start);
    }
    catch (ClassNotFoundException | IOException | RuntimeException e) {
      problem = e;
    }
    if (problem != null) {
      logger.error(LocalizedMessage.create(
            LocalizedStrings.GroupMembershipService_EXCEPTION_DESERIALIZING_MESSAGE_PAYLOAD_0, jgmsg), problem);
      return null;
    }

    return result;
  }
  
  
  /** look for certain messages that may need to be altered before being sent */
  void filterOutgoingMessage(DistributionMessage m) {
    switch (m.getDSFID()) {
    case JOIN_RESPONSE:
      JoinResponseMessage jrsp = (JoinResponseMessage)m;
      
      if (jrsp.getRejectionMessage() == null
          &&  services.getConfig().getTransport().isMcastEnabled()) {
        // get the multicast message digest and pass it with the join response
        Digest digest = (Digest)this.myChannel.getProtocolStack()
            .getTopProtocol().down(Event.GET_DIGEST_EVT);
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
      JoinResponseMessage jrsp = (JoinResponseMessage)m;
      
      if (jrsp.getRejectionMessage() == null
          &&  services.getConfig().getTransport().isMcastEnabled()) {
        byte[] serializedDigest = jrsp.getMessengerData();
        ByteArrayInputStream bis = new ByteArrayInputStream(serializedDigest);
        DataInputStream dis = new DataInputStream(bis);
        try {
          Digest digest = new Digest();
          digest.readFrom(dis);
          if (digest != null) {
            logger.trace("installing JGroups message digest {}", digest);
            this.myChannel.getProtocolStack()
                .getTopProtocol().down(new Event(Event.SET_DIGEST, digest));
            jrsp.setMessengerData(null);
          }
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
  private InternalDistributedMember getMemberFromView(GMSMember jgId, short version) {
    NetView v = services.getJoinLeave().getView();
    
    if (v != null) {
      for (InternalDistributedMember m: v.getMembers()) {
        if (((GMSMember)m.getNetMember()).equals(jgId)) {
          return m;
        }
      }
    }
    return new InternalDistributedMember(jgId);
  }


  @Override
  public void emergencyClose() {
    this.view = null;
    if (this.myChannel != null) {
      if ((services.isShutdownDueToForcedDisconnect() && services.isAutoReconnectEnabled()) || services.getManager().isReconnectingDS()) {
      }
      else {
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
    GMSQuorumChecker qc = new GMSQuorumChecker(
          view, services.getConfig().getLossThreshold(),
          this.myChannel);
    qc.initialize();
    return qc;
  }
  /**
   * JGroupsReceiver receives incoming JGroups messages and passes them to a handler.
   * It may be accessed through JChannel.getReceiver().
   */
  class JGroupsReceiver extends ReceiverAdapter  {
  
    @Override
    public void receive(Message jgmsg) {
      if (services.getManager().shutdownInProgress()) {
        return;
      }

      if (logger.isTraceEnabled()) {
        logger.trace("JGroupsMessenger received {} headers: {}", jgmsg, jgmsg.getHeaders());
      }
      
      //Respond to ping messages sent from other systems that are in a auto reconnect state
      byte[] contents = jgmsg.getBuffer();
      if (contents == null) {
        return;
      }
      if (pingPonger.isPingMessage(contents)) {
        try {
          pingPonger.sendPongMessage(myChannel, jgAddress, jgmsg.getSrc());
        }
        catch (Exception e) {
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

      DistributionMessage msg = (DistributionMessage)o;
      assert msg.getSender() != null;
      
      // admin-only VMs don't have caches, so we ignore cache operations
      // multicast to them, avoiding deserialization cost and classpath
      // problems
      if ( (services.getConfig().getTransport().getVmKind() == DistributionManager.ADMIN_ONLY_DM_TYPE)
           && (msg instanceof DistributedCacheOperation.CacheOperationMessage)) {
        return;
      }

      msg.resetTimestamp();
      msg.setBytesRead(jgmsg.getLength());
            
      try {
        logger.trace("JGroupsMessenger dispatching {} from {}", msg, msg.getSender());
        filterIncomingMessage(msg);
        getMessageHandler(msg).processMessage(msg);
      }
      catch (MemberShunnedException e) {
        // message from non-member - ignore
      }
    }
    
    /**
     * returns the handler that should process the given message.
     * The default handler is the membership manager
     * @param msg
     * @return
     */
    private MessageHandler getMessageHandler(DistributionMessage msg) {
      Class<?> msgClazz = msg.getClass();
      MessageHandler h = handlers.get(msgClazz);
      if (h == null) {
        for (Class<?> clazz: handlers.keySet()) {
          if (clazz.isAssignableFrom(msgClazz)) {
            h = handlers.get(clazz);
            handlers.put(msg.getClass(), h);
            break;
          }
        }
      }
      if (h == null) {
        h = (MessageHandler)services.getManager();
      }
      return h;
    }
  }
  
  
  
}
