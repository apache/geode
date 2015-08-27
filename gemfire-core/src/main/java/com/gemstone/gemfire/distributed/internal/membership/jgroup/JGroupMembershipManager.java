/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership.jgroup;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.NotSerializableException;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.ToDataException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.BoundedLinkedHashMap;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DSClock;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SizeableRunnable;
import com.gemstone.gemfire.distributed.internal.StartupMessage;
import com.gemstone.gemfire.distributed.internal.ThrottlingMemLinkedQueueWithDMStats;
import com.gemstone.gemfire.distributed.internal.direct.DirectChannel;
import com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.MembershipTestHook;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.QuorumChecker;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.SystemTimer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheServerCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.shared.StringPrintWriter;
import com.gemstone.gemfire.internal.tcp.ConnectExceptions;
import com.gemstone.gemfire.internal.tcp.ConnectionException;
import com.gemstone.gemfire.internal.tcp.MemberShunnedException;
import com.gemstone.gemfire.internal.tcp.Stub;
import com.gemstone.gemfire.internal.tcp.TCPConduit;
import com.gemstone.gemfire.internal.util.Breadcrumbs;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Channel;
import com.gemstone.org.jgroups.ChannelClosedException;
import com.gemstone.org.jgroups.ChannelNotConnectedException;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.Receiver;
import com.gemstone.org.jgroups.ShunnedAddressException;
import com.gemstone.org.jgroups.SuspectMember;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.debug.JChannelTestHook;
import com.gemstone.org.jgroups.oswego.concurrent.Latch;
import com.gemstone.org.jgroups.protocols.FD;
import com.gemstone.org.jgroups.protocols.FD_SOCK;
import com.gemstone.org.jgroups.protocols.TCP;
import com.gemstone.org.jgroups.protocols.TP;
import com.gemstone.org.jgroups.protocols.UDP;
import com.gemstone.org.jgroups.protocols.VERIFY_SUSPECT;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.protocols.pbcast.NAKACK;
import com.gemstone.org.jgroups.spi.GFBasicAdapter;
import com.gemstone.org.jgroups.spi.GFPeerAdapter;
import com.gemstone.org.jgroups.stack.GossipServer;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.ProtocolStack;
import com.gemstone.org.jgroups.util.GemFireTracer;

public class JGroupMembershipManager implements MembershipManager
{
  private static final Logger logger = LogService.getLogger();
  
  /** product version to use for multicast serialization */
  volatile boolean disableMulticastForRollingUpgrade;
  
  /**
   * set to true if the distributed system that created this manager was
   * auto-reconnecting when it was created.
   */
  boolean wasReconnectingSystem;
  
  /**
   * A quorum checker is created during reconnect and is held
   * here so it is available to the UDP protocol for passing off
   * the ping-pong responses used in the quorum-checking algorithm. 
   */
  private volatile QuorumCheckerImpl quorumChecker;
  
  /**
   * during an auto-reconnect attempt set this to the old DistributedSystem's
   * UDP membership port socket.  The UDP protocol will pick it up and use it.
   */
  private volatile DatagramSocket oldDSMembershipSocket;
  
  /**
   * thread-local used to force use of JGroups for communications, usually to
   * avoid deadlock when conserve-sockets=true.  Use of this should be removed
   * when connection pools are implemented in the direct-channel 
   */
  private ThreadLocal<Boolean> forceUseJGroups = new ThreadLocal<Boolean>();
  
  /**
   * A general use timer
   */
  private Timer timer = new Timer("Membership Timer", true);
  
  
  static {
    // this system property has been available since GemFire 2.0 to turn
    // tracing on in JGroups (formerly known as JavaGroups)
    boolean b = Boolean.getBoolean("DistributionManager.DEBUG_JAVAGROUPS");
    if ( b ) {
      GemFireTracer.DEBUG = true;
    }
  }
  
  
  /**
   * Trick class to make the startup synch more
   * visible in stack traces
   * 
   * @see JGroupMembershipManager#startupLock
   * @author jpenney
   *
   */
  static class EventProcessingLock  {
    public EventProcessingLock() {
      
    }
  }
  
  /**
   * Trick class to make the view lock more visible
   * in stack traces
   * 
   * @author jpenney
   *
   */
  static class ViewLock  {
    public ViewLock() {
      
    }
  }
  
  static class StartupEvent  {
    static final int DEPARTURE = 1;
    static final int CONNECT = 2;
    static final int VIEW = 3;
    static final int MESSAGE = 4;
    
    /**
     * indicates whether the event is a departure, a surprise connect
     * (i.e., before the view message arrived), a view, or a regular
     * message
     * 
     * @see #DEPARTURE
     * @see #CONNECT
     * @see #VIEW
     * @see #MESSAGE
     */
    private int kind;
    
    // Miscellaneous state depending on the kind of event
    InternalDistributedMember member;
    boolean crashed;
    String reason;
    DistributionMessage dmsg;
    Stub stub;
    View jgView;
    
    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("kind=");
      switch (kind) {
      case DEPARTURE:
        sb.append("departure; member = <")
          .append(member)
          .append(">; crashed = ")
          .append(crashed)
          .append("; reason = ");
        if (reason != null && (reason.indexOf("NoSuchMemberException") >= 0)) {
          sb.append(LocalizedStrings.JGroupMembershipManager_TCPIP_CONNECTIONS_CLOSED.toLocalizedString());
        }
        else {
          sb.append(reason);
        }
        break;
      case CONNECT:
        sb.append("connect; member = <" + member + ">; stub = " + stub);
        break;
      case VIEW:
        String text = DistributionManager.printView(
            viewToMemberView(jgView));
        sb.append("view <" + text + ">");
        break;
      case MESSAGE:
        sb.append("message <" + dmsg + ">");
        break;
      default:
        sb.append("unknown=<" + kind + ">");
        break;
      }
      return sb.toString();
    }
    /**
     * Create a departure event
     * @param dm the member that left
     * @param crashed true if this member crashed
     * @param reason reason string, esp. if crashed
     */
    StartupEvent(InternalDistributedMember dm, boolean crashed, String reason) {
      this.kind = DEPARTURE;
      this.member = dm;
      this.crashed = crashed;
      this.reason = reason;
    }
    /**
     * Indicate if this is a departure
     * @return true if this is a departure event
     */
    boolean isDepartureEvent() {
      return this.kind == DEPARTURE;
    }

    /**
     * Create a connect event
     * @param member the member connecting
     * @param id the stub
     */
    StartupEvent(final InternalDistributedMember member, final Stub id) {
      this.kind = CONNECT;
      this.member = member;
      this.stub = id;
    }
    /**
     * Indicate if this is a connect event
     * @return true if this is a connect event
     */
    boolean isConnect() {
      return this.kind == CONNECT;
    }

    /**
     * Create a view event
     * @param v the new view
     */
    StartupEvent(View v) {
      this.kind = VIEW;
      this.jgView = v;
    }
    
    /**
     * Indicate if this is a view event
     * @return true if this is a view event
     */
    boolean isJgView() {
      return this.kind == VIEW;
    }

    /**
     * Create a message event
     * @param d the message
     */
    StartupEvent(DistributionMessage d) {
      this.kind = MESSAGE;
      this.dmsg = d;
    }
    /**
     * Indicate if this is a message event
     * @return true if this is a message event
     */
    boolean isDistributionMessage() {
      return this.kind == MESSAGE;
    }
  }
  
  /**
   * The system property that specifies the name of a file from which to read
   * Jgroups configuration information
   */
  public static final String JAVAGROUPS_CONFIG = System
      .getProperty("DistributionManager.JAVAGROUPS_CONFIG");

  /**
   * The location (in the product) of the locator Jgroups config file.
   */
  private static final String DEFAULT_JAVAGROUPS_TCP_CONFIG = "com/gemstone/gemfire/distributed/internal/javagroups-config.txt";

  /**
   * The location (in the product) of the mcast Jgroups config file.
   */
  private static final String DEFAULT_JAVAGROUPS_MCAST_CONFIG = "com/gemstone/gemfire/distributed/internal/javagroups-mcast.txt";

  /** is multicast being used for group discovery, or locators? */
  private static boolean isMcastDiscovery = false;
  
  /** is use of multicast enabled at all? */
  private static boolean isMcastEnabled = false;

  /** Sometimes the jgroups channel is blocked until unicast messages
      are all ack'd.  This boolean disables that blocking and can
      increase performance in situations where it's okay to deliver
      point-to-point and multicast messages out of order
   */
  public final static boolean DISABLE_UCAST_FLUSH = !Boolean.getBoolean("p2p.enableUcastFlush");
  
  private int membershipCheckTimeout = DistributionConfig.DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT;

//  /** FOR TESTING ONLY:
//      This variable forces most distributed cache operations to be
//      multicast if multicast is available.  See DistributionConfigImpl's
//      multicastTest variable for forcing availability of multicast. */
//  public final static boolean multicastTest = false;

  /**
   * This is the underlying Jgroups channel
   */
  protected volatile JChannel channel;
  
  /**
   * This object synchronizes threads waiting for
   * startup to finish.  Updates to {@link #startupMessages}
   * are synchronized through this object.
   */
  protected final EventProcessingLock startupLock = new EventProcessingLock();
  
  /**
   * This is the latest view (ordered list of DistributedMembers) 
   * that has been returned from Jgroups
   * 
   * All accesses to this object are protected via {@link #latestViewLock}
   */
  protected NetView latestView = new NetView();

  /**
   * This is the lock for protecting access to latestView
   * 
   * @see #latestView
   */
  protected ViewLock latestViewLock = new ViewLock();
  
  /**
   * This is the listener that accepts our membership events
   */
  protected com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener listener;

  /**
   * Membership failure listeners - for testing
   */
  List membershipTestHooks;
  
  /**
   * Channel test hook
   */
  protected JChannelTestHook channelTestHook;
  
  /**
   * This is a representation of the local member (ourself)
   */
  protected InternalDistributedMember myMemberId = null; // new DistributedMember(-1);

  protected DirectChannel directChannel;

  protected TCPConduit conduit;
  
  protected MyDCReceiver dcReceiver;
  
  /** the configuration for the distributed system */
  private DistributionConfig dconfig;
  
  volatile boolean isJoining;
  
  /** has the jgroups channel been connected successfully? */
  volatile boolean hasConnected;
  
  /**
   * a map keyed on InternalDistributedMember, values are Stubs that represent direct
   * channels to other systems
   * 
   * Accesses must be synchronized via {@link #latestViewLock}.
   */
  protected final Map memberToStubMap = new ConcurrentHashMap();

  /**
   * a map of direct channels (Stub) to InternalDistributedMember. key instanceof Stub
   * value instanceof InternalDistributedMember
   * 
   * Accesses must be synchronized via {@link #latestViewLock}.
   */
  protected final Map stubToMemberMap = new ConcurrentHashMap();
  
  /**
   * a map of jgroups addresses (IpAddress) to InternalDistributedMember that
   * is used to avoid creating new IDMs for every jgroups message when the
   * direct-channel is disabled
   */
  private final Map ipAddrToMemberMap = new ConcurrentHashMap();

  /**
   * Members of the distributed system that we believe have shut down.
   * Keys are instances of {@link InternalDistributedMember}, values are 
   * Longs indicating the time this member was shunned.
   * 
   * Members are removed after {@link #SHUNNED_SUNSET} seconds have
   * passed.
   * 
   * Accesses to this list needs to be synchronized via {@link #latestViewLock}
   * 
   * @see System#currentTimeMillis()
   */
//  protected final Set shunnedMembers = Collections.synchronizedSet(new HashSet());
  protected final Map shunnedMembers = new ConcurrentHashMap();

  /**
   * Members that have sent a shutdown message.  This is used to suppress
   * suspect processing in JGroups that otherwise becomes pretty aggressive 
   * when a member is shutting down.
   */
  private final Map shutdownMembers = new BoundedLinkedHashMap(1000);
  
  /**
   * per bug 39552, keep a list of members that have been shunned and for
   * which a message is printed.  Contents of this list are cleared at the
   * same time they are removed from {@link #shunnedMembers}.
   * 
   * Accesses to this list needs to be synchronized via {@link #latestViewLock}
   */
  protected final HashSet shunnedAndWarnedMembers = new HashSet();
  /**
   * The identities and birth-times of others that we have allowed into
   * membership at the distributed system level, but have not yet appeared
   * in a jgroups view.
   * <p>
   * Keys are instances of {@link InternalDistributedMember}, values are 
   * Longs indicating the time this member was shunned.
   * <p>
   * Members are removed when a view containing them is processed.  If,
   * after {@link #surpriseMemberTimeout} milliseconds have passed, a view
   * containing the member has not arrived, the member is removed from
   * membership and member-left notification is performed. 
   * <p>>
   * Accesses to this list needs to be synchronized via {@link #latestViewLock}
   * 
   * @see System#currentTimeMillis()
   */
  protected final Map<InternalDistributedMember, Long> surpriseMembers = new ConcurrentHashMap();

  /**
   * the timeout interval for surprise members.  This is calculated from 
   * the member-timeout setting
   */
  protected int surpriseMemberTimeout;
  
  /**
   * javagroups can skip views and omit telling us about a crashed member.
   * This map holds a history of suspected members that we use to detect
   * crashes.
   */
  private final Map<InternalDistributedMember, Long> suspectedMembers = new ConcurrentHashMap();
  
  /**
   * the timeout interval for suspected members
   */
  private final long suspectMemberTimeout = 180000;
  
  /** sleep period, in millis, that the user of this manager should slumber after creating
      the manager.  This is advice from the JChannel itself when it detects a concurrent
      startup race condition that requires a settling period. */
  private long channelPause = 0;  

  /** whether time-based statistics should be gathered */
//  private boolean enableClockStats = DistributionStats.enableClockStats;
 
  /** whether time-based statistics should be gathered for up/down events in jgroup stacks */
  private boolean enableJgStackStats = Boolean.getBoolean("p2p.enableJgStackStats");

  
  /**
   * Length of time, in seconds, that a member is retained in the zombie set
   * 
   * @see #shunnedMembers
   */
  static private final int SHUNNED_SUNSET = Integer.getInteger(
      "gemfire.shunned-member-timeout", 300).intValue();
  
  /** has the jchannel been initialized? */
  protected volatile boolean channelInitialized = false;
  
  /**
   * Set to true when the service should stop.
   */
  protected volatile boolean shutdownInProgress = false;
  
  /**
   * Set to true when upcalls should be generated for
   * events.
   */
  protected volatile boolean processingEvents = false;
  
  /**
   * This is the latest viewId received from JGroups
   */
  long lastViewId = -1;
  
  /** 
   * the jgroups channel's UNICAST protocol object (used for flushing)
   */
  com.gemstone.org.jgroups.protocols.UNICAST ucastProtocol;
  
  /**
   * the jgroups channel's VERIFY_SUSPECT protocol object
   */
  com.gemstone.org.jgroups.protocols.VERIFY_SUSPECT verifySuspectProtocol;
  
  /**
   * if using UDP-based failure detection, this holds the FD protocol
   */
  com.gemstone.org.jgroups.protocols.FD fdProtocol;
  
  /**
   * The underlying UDP protocol, if it exists
   */
  UDP udpProtocol;
  
  
  /**
   * The underlying TCP protocol, if it exists
   */
  TCP tcpProtocol;
  
  /**
   * The underlying NAKACK protocol, if it exists
   */
  NAKACK nakAckProtocol;
  
  /**
   * the jgroups channel's FD_SOCK protocol object (for system failure)
   */
  com.gemstone.org.jgroups.protocols.FD_SOCK fdSockProtocol;
  
  /** an object used in the suspension of message transmission */
  private Object sendSuspendMutex = new Object();
  
  /** a flag stating that outgoing message transmission is suspended */
  private boolean sendSuspended = false;
  
  /** distribution manager statistics */
  DMStats stats;

  /**
   A list of messages received during channel startup that couldn't be processed yet.
   Additions or removals of this list must be synchronized
   via {@link #startupLock}.
   @since 5.0
   */
  protected LinkedList startupMessages = new LinkedList();
  
  /** the type of vm we're running in. This is also in the membership id, 
      but is needed by some methods before the membership id has been
      created. */
  int vmKind;

  /**
   * ARB: the map of latches is used to block peer handshakes till
   * authentication is confirmed.
   */
  final private HashMap memberLatch = new HashMap();
  
  /**
   * Insert our own MessageReceiver between us and the direct channel, in order
   * to correctly filter membership events.
   * 
   * TODO: DistributionManager shouldn't have to implement this interface.
   * 
   * @author jpenney
   * 
   */
  class MyDCReceiver implements DistributedMembershipListener
  {

    DistributedMembershipListener upCall;
    
    /**
     * Don't provide events until the caller has told us we are ready.
     * 
     * Synchronization provided via JGroupMembershipManager.class.
     * 
     * Note that in practice we only need to delay accepting the first
     * client; we don't need to put this check before every call...
     *
     */
   MyDCReceiver(DistributedMembershipListener up) {
      upCall = up;
    }

    public void messageReceived(DistributionMessage msg)
    {
      // bug 36851 - notify failure detection that we've had contact from a member
      IpAddress addr = ((JGroupMember)msg.getSender().getNetMember()).getAddress();
      if (fdProtocol != null) {
        fdProtocol.messageReceivedFrom(addr);
      }
      if (verifySuspectProtocol != null) {
        if (addr.getBirthViewId() < 0) {
          if (logger.isDebugEnabled()) {
            // if there is no view ID then this is not a valid address
            logger.debug("Membership: invalid address found in sender of {}", msg);
          }
        } else {
          verifySuspectProtocol.unsuspect(addr);
        }
      }
      handleOrDeferMessage(msg);
    }

    public void newMemberConnected(final InternalDistributedMember member, final Stub id)
    {
      handleOrDeferSurpriseConnect(member, id);
    }

    public void memberDeparted(InternalDistributedMember id, boolean crashed, String reason)
    {
      try {
        handleOrDeferRemove(id, crashed, reason);
      }
      catch (DistributedSystemDisconnectedException ignore) {
        // ignore
      }
      catch (RuntimeException e) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_ERROR_HANDLING_MEMBER_DEPARTURE__0), e);
      }
    }
    
    public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
    }

    public void memberSuspect(InternalDistributedMember suspect, InternalDistributedMember whoSuspected) {
      // the direct channel isn't currently a source of suspect events, though
      // it does request initiation of suspicion through the membership
      // manager
    }

    public boolean isShutdownMsgSent()
    {
      return upCall.isShutdownMsgSent();
    }

    public void membershipFailure(String reason, Throwable t)
    {
      upCall.membershipFailure(reason, t);
    }
    
    public void viewInstalled(NetView view) {
      upCall.viewInstalled(view);
    }

    public DistributionManager getDM()
    {
     return upCall.getDM();
    }

  }


//  /**
//   * Perform major validation on a NetView
//   * 
//   * @param v
//   */
//  private void validateView(Vector members)
//  {
//    for (int i = 0; i < members.size(); i++) {
//      InternalDistributedMember dm = (InternalDistributedMember)members.elementAt(i);
//      if ((!NO_DIRECT_CHANNEL && dm.getDirectChannelPort() <= 0)
//          || dm.getVmKind() <= 0 || dm.getPort() <= 0) {
//        members.remove(i);
//        i--; // restart scan at new element at this point
//      }
//    }
//  }

  /** turn javagroups debugging on or off */
  public static void setDebugJGroups(boolean flag) {
    com.gemstone.org.jgroups.util.GemFireTracer.DEBUG=flag;
    com.gemstone.org.jgroups.stack.Protocol.trace=flag;
  }
  

  /** if we connect to a locator that has NPD enabled then we enable it in this VM */
  public void enableNetworkPartitionDetection() {
    if (logger.isDebugEnabled()) {
      logger.debug("Network partition detection is being enabled");
    }
    this.dconfig.setEnableNetworkPartitionDetection(true);
  }
  
  /**
   * Analyze a given view object, generate events as appropriate
   * 
   * @param newView
   */
  protected void processView(long newViewId, NetView newView)
  {
    // Sanity check...
     if (logger.isInfoEnabled(LogMarker.DM_VIEWS)) {
      StringBuffer msg = new StringBuffer(200);
      msg.append("Membership: Processing view (id = ");
      msg.append(newViewId).append(") {");
      msg.append(DistributionManager.printView(newView));
      if (newView.getCrashedMembers().size() > 0) {
        msg.append(" with unexpected departures [");
        for (Iterator it=newView.getCrashedMembers().iterator(); it.hasNext(); ) {
          msg.append(it.next());
          if (it.hasNext()) {
            msg.append(',');
          }
        }
        msg.append(']');
      }
      msg.append("} on " + myMemberId.toString());
      if (logger.isDebugEnabled()) {
        logger.trace(LogMarker.DM_VIEWS, msg);
      }
      if (!newView.contains(myMemberId))
        logger.info(LocalizedMessage.create(
            LocalizedStrings.JGroupMembershipManager_THE_MEMBER_WITH_ID_0_IS_NO_LONGER_IN_MY_OWN_VIEW_1,
            new Object[] {myMemberId, newView}));
    }
     
//     if (newView.getCrashedMembers().size() > 0) {
//       // dump stack for debugging #39827
//       OSProcess.printStacks(0);
//     }
    // We perform the update under a global lock so that other
    // incoming events will not be lost in terms of our global view.
    synchronized (latestViewLock) {
      // first determine the version for multicast message serialization
      Version version = Version.CURRENT;
      for (Iterator<Map.Entry<InternalDistributedMember, Long>> it=surpriseMembers.entrySet().iterator(); it.hasNext(); ) {
        InternalDistributedMember mbr = it.next().getKey();
        Version itsVersion = mbr.getVersionObject();
        if (itsVersion != null && version.compareTo(itsVersion) < 0) {
          version = itsVersion;
        }
      }
      for (InternalDistributedMember mbr: newView) {
        Version itsVersion = mbr.getVersionObject();
        if (itsVersion != null && itsVersion.compareTo(version) < 0) {
          version = mbr.getVersionObject();
        }
      }
      disableMulticastForRollingUpgrade = !version.equals(Version.CURRENT);
      
      if (newViewId < lastViewId) {
        // ignore this view since it is old news
        if (newViewId < lastViewId && logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS)) {
          logger.trace(LogMarker.DISTRIBUTION_VIEWS, "Membership: Ignoring view (with id {}) since it is older than the last view (with id {}); ignoredView={}",
              newViewId, lastViewId, DistributionManager.printView(newView));
        }
        return;
      }

      // Save previous view, for delta analysis
      Vector priorView = latestView;
      
      // update the view to reflect our changes, so that
      // callbacks will see the new (updated) view.
      lastViewId = newViewId;
      latestView = newView;
      
      // look for additions
      Set warnShuns = new HashSet();
      for (int i = 0; i < newView.size(); i++) { // additions
        InternalDistributedMember m = (InternalDistributedMember)newView.elementAt(i);
        
        // Once a member has been seen via JGroups, remove them from the
        // newborn set
        boolean wasSurprise = surpriseMembers.remove(m) != null;
        
        // bug #45155 - membership view processing was slow, causing a member to connect as "surprise"
        // and the surprise timeout removed the member and shunned it, keeping it from being
        // recognized as a valid member when it was finally seen in a view
//        if (isShunned(m)) {
//          warnShuns.add(m);
//          continue;
//        }

        // if it's in a view, it's no longer suspect
        suspectedMembers.remove(m);

        if (priorView.contains(m) || wasSurprise) {
          continue; // already seen
        }
      
        // ARB: unblock any waiters for this particular member.
        // i.e. signal any waiting threads in tcpconduit.
        String authInit = this.dconfig.getSecurityPeerAuthInit();
        boolean isSecure = authInit != null && authInit.length() != 0;

        if (isSecure) {
          Latch currentLatch;
          if ((currentLatch = (Latch)memberLatch.get(m)) != null) {
            currentLatch.release();
          }
        }

        // fix for bug #42006, lingering old identity
        Object oldStub = this.memberToStubMap.remove(m);
        if (oldStub != null) {
          this.stubToMemberMap.remove(oldStub);
        }

        if (shutdownInProgress()) {
          logger.info(LogMarker.DM_VIEWS, LocalizedMessage.create(
              LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_SHUNNING_MEMBER__0__DURING_OUR_SHUTDOWN, m));
          addShunnedMember(m);
          continue; // no additions processed after shutdown begins
        } else {
          boolean wasShunned = endShun(m); // bug #45158 - no longer shun a process that is now in view
          if (wasShunned && logger.isTraceEnabled(LogMarker.DM_VIEWS)) {
            logger.debug("No longer shunning {} as it is in the current membership view", m);
          }
        }
        
        logger.info(LocalizedMessage.create(LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_PROCESSING_ADDITION__0_, m));

        try {
          listener.newMemberConnected(m, getStubForMember(m));
        }
        catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch (DistributedSystemDisconnectedException e) {
          // don't log shutdown exceptions
        }
        catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.info(LocalizedMessage.create(
              LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_FAULT_WHILE_PROCESSING_VIEW_ADDITION_OF__0, m), t);
        }
      } // additions
      
      // now issue warnings outside of the synchronization
      for (Iterator it=warnShuns.iterator(); it.hasNext(); ) {
        warnShun((DistributedMember)it.next());
      }

      // look for departures
      for (int i = 0; i < priorView.size(); i++) { // departures
        InternalDistributedMember m = (InternalDistributedMember)priorView.elementAt(i);
        if (newView.contains(m)) {
          continue; // still alive
        }
        
        if (surpriseMembers.containsKey(m)) {
          continue; // member has not yet appeared in JGroups view
        }

        try {
          logger.info(LogMarker.DM_VIEWS, LocalizedMessage.create(
              LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_PROCESSING_DEPARTING_MEMBER__0_, m));
          removeWithViewLock(m,
              newView.getCrashedMembers().contains(m) || suspectedMembers.containsKey(m)
              , "departed JGroups view");
        }
        catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.info(LocalizedMessage.create(
              LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_FAULT_WHILE_PROCESSING_VIEW_REMOVAL_OF__0, m), t);
        }
      } // departures
      
      // expire surprise members, add others to view
      long oldestAllowed = System.currentTimeMillis() - this.surpriseMemberTimeout;
      for (Iterator<Map.Entry<InternalDistributedMember, Long>> it=surpriseMembers.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<InternalDistributedMember, Long> entry = it.next();
        Long birthtime = (Long)entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          it.remove();
          InternalDistributedMember m = entry.getKey();
          logger.info(LocalizedMessage.create(
            LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_EXPIRING_MEMBERSHIP_OF_SURPRISE_MEMBER_0, m));
          removeWithViewLock(m, true, "not seen in membership view in "
              + this.surpriseMemberTimeout + "ms");
        }
        else {
          if (!latestView.contains(entry.getKey())) {
            latestView.add(entry.getKey());
          }
        }
      }
      // expire suspected members
      oldestAllowed = System.currentTimeMillis() - this.suspectMemberTimeout;
      for (Iterator it=suspectedMembers.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry entry = (Map.Entry)it.next();
        Long birthtime = (Long)entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          InternalDistributedMember m = (InternalDistributedMember)entry.getKey();
          it.remove();
          if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS)) {
            logger.trace(LogMarker.DISTRIBUTION_VIEWS, "Membership: expiring suspect member <{}>", m);
          }
        }
      }
      try {
        listener.viewInstalled(latestView);
        startCleanupTimer();
      }
      catch (DistributedSystemDisconnectedException se) {
      }
    } // synchronized
    logger.info(LogMarker.DM_VIEWS, LocalizedMessage.create(
        LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_FINISHED_VIEW_PROCESSING_VIEWID___0, Long.valueOf(newViewId)));
  }

  /**
   * Convert from a Jgroups View to a GemFire Netview
   * 
   * @param v
   * @return the NetView
   */
  static protected NetView viewToMemberView(View v)
  {
    Vector vmem = v.getMembers();
    NetView members = new NetView(v.getMembers().size(), v.getVid().getId());
    
    Address leadmember = v.getLeadMember();
    Address creator = v.getCreator();

    for (int i = 0; i < vmem.size(); i++) {
      IpAddress a = (IpAddress)vmem.get(i);
      JGroupMember m = new JGroupMember(a);
      InternalDistributedMember d = new InternalDistributedMember(m);
      members.add(d);
      if (leadmember != null && leadmember.equals(a)) {
        members.setLeadMember(m);
      }
      if (creator != null && creator.equals(a)) {
        members.setCreator(m);
      }
    }
    // if there's a suspect set, process it and set it as the crashed set
    Set smem = v.getSuspectedMembers();
    if (smem != null) {
      Set snmem = new HashSet(smem.size());
      for (Iterator it=smem.iterator(); it.hasNext(); ) {
        IpAddress a = (IpAddress)it.next();
        JGroupMember m = new JGroupMember(a);
        InternalDistributedMember d = new InternalDistributedMember(m);
        snmem.add(d);
      }
      members.setCrashedMembers(snmem);
    }
    return members;
  }

  /**
   * A worker thread that reads incoming messages and adds them to the incoming
   * queue.
   */
  class Puller implements Receiver
  {
  
  
    /* ------------------- JGroups Receiver -------------------------- */
    /**
     * Called when a message is received. 
     * @param jgmsg
     */
    public void receive(Message jgmsg) {
      if (shutdownInProgress())
        return;

      Object o = null;
      int messageLength = jgmsg.getLength();
      
      if (messageLength == 0) {
        // jgroups messages with no payload are used for protocol interchange, such
        // as STABLE_GOSSIP
        return;
      }

      // record the time spent getting from the socket to this method.
      // see com.gemstone.org.jgroups.protocols.TP
      if (DistributionStats.enableClockStats && jgmsg.timeStamp > 0) {
        stats.incMessageChannelTime(DistributionStats.getStatTime()-jgmsg.timeStamp);
      }
      
      // admin-only VMs don't have caches, so we ignore cache operations
      // multicast to them, avoiding deserialization cost and classpath
      // problems
      if ( (vmKind == DistributionManager.ADMIN_ONLY_DM_TYPE)
           && jgmsg.getIsDistributedCacheOperation()) {
        if (logger.isTraceEnabled(LogMarker.DM))
          logger.trace(LogMarker.DM, "Membership: admin VM discarding cache operation message {}", (Object)jgmsg.getObject());
        return;
      }

      try {
        o = jgmsg.getObject();
      }
      catch (IllegalArgumentException e) {
        if (e.getCause() != null && e.getCause() instanceof java.io.IOException) {
          logger.error(LocalizedMessage.create(
              LocalizedStrings.JGroupMembershipManager_EXCEPTION_DESERIALIZING_MESSAGE_PAYLOAD_0, jgmsg), e.getCause());
          return;
        }
        else {
          throw e;
        }
      }
      if (o == null) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_GEMFIRE_RECEIVED_NULL_MESSAGE_FROM__0, String.valueOf(jgmsg)));
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_MESSAGE_HEADERS__0, jgmsg.printObjectHeaders()));
        return;
      }

      DistributionMessage msg = (DistributionMessage)o;

      msg.resetTimestamp();
      msg.setBytesRead(messageLength);
            
      IpAddress sender = (IpAddress)jgmsg.getSrc();
      if (sender == null) {
        Exception e = new Exception(LocalizedStrings.JGroupMembershipManager_NULL_SENDER.toLocalizedString());
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_GEMFIRE_RECEIVED_A_MESSAGE_WITH_NO_SENDER_ADDRESS), e);
      }
      InternalDistributedMember dm;
      if (isConnected()) {
        dm = getMemberFromIpAddress(sender, true);
      } else {
        // if the channel isn't initialized getMemberFromIpAddress will block the
        // UDP receive channel, eventually causing a SystemConnectException
        JGroupMember jgm = new JGroupMember(sender);
        dm = new InternalDistributedMember(jgm);
      }
      msg.setSender(dm);
      try {
        handleOrDeferMessage(msg);
      }
      catch (MemberShunnedException e) {
        // message from non-member - ignore
      }
    }

    /**
     * Answers the group state; e.g., when joining.
     * @return byte[] 
     */
    public byte[]  getState() {
      return new byte[0];
    }

    /**
     * Sets the group state; e.g., when joining.
     * @param state
     */
    public void   setState(byte[] state) {
    }
        
    /**
     * Called when a change in membership has occurred.
     * <b>No long running actions should be done in this callback.</b>
     * If some long running action needs to be performed, it should be done in a separate thread.
     */
    public void viewAccepted(View viewArg) {
      handleOrDeferViewEvent(viewArg);
    }
    
    /** 
     * Called whenever a member is suspected of having crashed, 
     * but has not yet been excluded. 
     */
    public void suspect(SuspectMember suspectInfo) {
      if (!shutdownInProgress()) {
        handleOrDeferSuspect(suspectInfo);
      }
    }
    
    /**
     * Called whenever a member has passed suspect processing during suspect
     * processing
     */

    /** 
     * Called whenever the member needs to stop sending messages. 
     * When the next view is received (viewAccepted()), the member can resume sending 
     * messages. If a member does not comply, the message(s) sent between a block() 
     * and a matching viewAccepted() callback will probably be delivered in the next view.
     * The block() callback is only needed by the Virtual Synchrony suite of protocols 
     * (FLUSH protocol)3.2, otherwise it will never be invoked. 
     */
    public void block() {
    }
    
    
    public void channelClosing(Channel c, final Exception e) {
      if (e instanceof ShunnedAddressException) {
        return; // channel creation will retry
      }
      if (JGroupMembershipManager.this.shutdownInProgress) {
        return;  // probably a jgroups race condition
      }
      if (!dconfig.getDisableAutoReconnect()) {
        saveCacheXmlForReconnect(dconfig.getUseSharedConfiguration());
      }
      // make sure that we've received a connected channel and aren't responsible
      // for the notification
      if (JGroupMembershipManager.this.puller != null // not shutting down
          && JGroupMembershipManager.this.channel != null) { // finished connecting
        // cache the exception so it can be appended to ShutdownExceptions
        JGroupMembershipManager.this.shutdownCause = e;
        AlertAppender.getInstance().shuttingDown();

        // QA thought that the long JGroups stack traces were a nuisance
        // I [bruce] changed the error messages to include enough info
        // for us to pinpoint where the ForcedDisconnect occurred.
        String reason;
        Exception logException = e;
        if (e instanceof ForcedDisconnectException) {
          reason = "Channel closed: " + e;
          logException = null;
        }
        else {
          reason = "Channel closed";
        }
        if (!inhibitForceDisconnectLogging) {
          logger.fatal(LocalizedMessage.create(
              LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_SERVICE_FAILURE_0, reason), logException);
        }
        // stop server locators immediately since they may not have correct
        // information.  This has caused client failures in bridge/wan
        // network-down testing
        List locs = Locator.getLocators();
        if (locs.size() > 0) {
          for (Iterator it=locs.iterator(); it.hasNext(); ) {
            InternalLocator loc = (InternalLocator)it.next();
//            if (loc.isServerLocator()) {  auto-reconnect requires locator to stop now
              loc.stop(!JGroupMembershipManager.this.dconfig.getDisableAutoReconnect(), false);
//            }
          }
        }
        InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
        if (system != null) {
          DM dm = system.getDM();
          if (dm != null) {
            dm.setRootCause(e);
          }
        }
        JGroupMembershipManager.this.uncleanShutdown(reason, e);
      }
    }
    
        
  } // Puller class
  
  
  
  /** the jgroups receiver object */
  Puller puller;
  
  /** an exception that caused the manager to shut down */
  volatile Exception shutdownCause;

  /**
   * the timer used to perform periodic tasks
   * @guarded.By latestViewLock
   */
  private SystemTimer cleanupTimer;



  /**
   * Creates a Jgroups {@link Channel} using the GemFire-specified
   * configuration.
   * 
   * @param transport
   *          Transport configuration
   * 
   * @throws GemFireConfigException
   *           The Jgroups config file is missing or malformatted
   * @throws DistributionException
   *           Something goes wrong while connecting to Jgroups
   */
  private JChannel createChannel(RemoteTransportConfig transport, DistributionConfig theConfig)
  {
    this.shutdownCause = null;
    
    JGroupMembershipManager.isMcastDiscovery = transport.isMcastDiscovery();
    JGroupMembershipManager.isMcastEnabled = transport.isMcastEnabled();
    
//    boolean isWindows = false;
//    String os = System.getProperty("os.name");
//    if (os != null) {
//      if (os.indexOf("Windows") != -1) {
//        isWindows = true;
//      }
//    }

    String properties;

    InputStream is;
    if (JAVAGROUPS_CONFIG != null) {
      File file = new File(JAVAGROUPS_CONFIG);
      if (!file.exists()) {
        throw new GemFireConfigException(LocalizedStrings.JGroupMembershipManager_JGROUPS_CONFIGURATION_FILE_0_DOES_NOT_EXIST.toLocalizedString(JAVAGROUPS_CONFIG));
      }

      try {
        if (logger.isDebugEnabled()) {
          logger.debug("Reading Jgroups config from {}", file);
        }
        is = new FileInputStream(file);

      }
      catch (IOException ex) {
        throw new GemFireConfigException(LocalizedStrings.JGroupMembershipManager_AN_IOEXCEPTION_WAS_THROWN_WHILE_OPENING_0.toLocalizedString(file), ex);
      }

    }
    else {
      String r = null;
      if (JGroupMembershipManager.isMcastEnabled) {
        r = DEFAULT_JAVAGROUPS_MCAST_CONFIG;
      } else {
        r = DEFAULT_JAVAGROUPS_TCP_CONFIG;
      }
      is = ClassPathLoader.getLatest().getResourceAsStream(getClass(), r);
      if (is == null) {
        throw new GemFireConfigException(LocalizedStrings.JGroupMembershipManager_CANNOT_FIND_0.toLocalizedString(r));
      }
    }

    try {
      //PlainConfigurator config = PlainConfigurator.getInstance(is);
      //properties = config.getProtocolStackString();
      StringBuffer sb = new StringBuffer(3000);
      BufferedReader br;
      if (JAVAGROUPS_CONFIG != null) {
        br = new BufferedReader(new InputStreamReader(is));
      } else {
        br = new BufferedReader(new InputStreamReader(is, "US-ASCII"));
      }
      String input;
      while ((input=br.readLine()) != null) {
        sb.append(input);
      }
      br.close();
      properties = sb.toString();

    }
    catch (Exception ex) {
      throw new GemFireConfigException(LocalizedStrings.JGroupMembershipManager_AN_EXCEPTION_WAS_THROWN_WHILE_READING_JGROUPS_CONFIG.toLocalizedString(), ex);
    }

    // see whether the FD protocol or FD_SOCK protocol should be used
    long fdTimeout = Long.getLong("gemfire.FD_TIMEOUT", 0).longValue(); // in 4.1.2 to force use of FD
    
    if (JGroupMembershipManager.isMcastEnabled) {
      properties = replaceStrings(properties, "MULTICAST_PORT", String.valueOf(transport.getMcastId().getPort()));
      properties = replaceStrings(properties, "MULTICAST_HOST", transport.getMcastId().getHost().getHostAddress());
      properties = replaceStrings(properties, "MULTICAST_TTL", String.valueOf(theConfig.getMcastTtl()));
      properties = replaceStrings(properties, "MULTICAST_SEND_BUFFER_SIZE", String.valueOf(theConfig.getMcastSendBufferSize()));
      properties = replaceStrings(properties, "MULTICAST_RECV_BUFFER_SIZE", String.valueOf(theConfig.getMcastRecvBufferSize()));
      // set the nakack retransmit limit smaller than the frag size to allow
      // room for nackack headers and src address
      properties = replaceStrings(properties, "RETRANSMIT_LIMIT", String.valueOf(theConfig.getUdpFragmentSize()-256));
      properties = replaceStrings(properties, "MAX_SENT_MSGS_SIZE", System.getProperty("p2p.maxSentMsgsSize", "0"));
    }
    if (Boolean.getBoolean("p2p.simulateDiscard")) {
      properties = replaceStrings(properties, "DISCARD",
        "com.gemstone.org.jgroups.protocols.DISCARD(down_thread=false; up_thread=false;"
          + "up=" + System.getProperty("p2p.simulateDiscard.received", "0") + ";"
          + "down=" + System.getProperty("p2p.simulateDiscard.sent", "0.05") + ")?");
    }
    else {
      properties = replaceStrings(properties, "DISCARD", "");
    }
    if (!JGroupMembershipManager.isMcastDiscovery) {
      if (JGroupMembershipManager.isMcastEnabled) {
        // for non-mcast discovery, we use TCPGOSSIP and remove PING
        properties = replaceStrings(properties, "OPTIONAL_GOSSIP_PROTOCOL",
          "com.gemstone.org.jgroups.protocols.TCPGOSSIP("
            + "num_ping_requests=NUM_PING_REQUESTS;"
            + "timeout=DISCOVERY_TIMEOUT;" // bug #50084 - solo locator takes member-timeout ms to start.  Added DISCOVERY_TIMEOUT setting
                                           // bug #44928 - client "hung" trying to connect to locator so reduced this to member_timeout
            + "split-brain-detection=PARTITION_DETECTION;"
            + "gossip_refresh_rate=GOSSIP_REFRESH_RATE;"
            + "initial_hosts=INITIAL_HOSTS;"
            + "gossip_server_wait_time=GOSSIP_SERVER_WAIT_TIME;"
            + "num_initial_members=2;"
            + "up_thread=false;"
            + "down_thread=false)?");
        properties = replaceStrings(properties, "OPTIONAL_PING_PROTOCOL", "");
      }
//      String locators = transport.locatorsString();
      properties = replaceStrings(properties, "INITIAL_HOSTS", transport.locatorsString());
      properties = replaceStrings(properties, "NUM_INITIAL_MEMBERS", System.getProperty("p2p.numInitialMembers", "1"));

      // with locators, we don't want to become the initial coordinator, so disable this ability
      boolean disableCoord = !Boolean.getBoolean("p2p.enableInitialCoordinator");
      if (theConfig.getStartLocator() != null) {
        disableCoord = false;
      }
      properties = replaceStrings(properties, "DISABLE_COORD", String.valueOf(disableCoord));
    }
    else {
      // PING is the multicast discovery protocol
      properties = replaceStrings(properties, "OPTIONAL_GOSSIP_PROTOCOL", "");
      properties = replaceStrings(properties, "OPTIONAL_PING_PROTOCOL",
          "com.gemstone.org.jgroups.protocols.PING("
           +  "timeout=DISCOVERY_TIMEOUT;"
           +  "down_thread=false;"
           +  "up_thread=false;"
           +  "num_initial_members=NUM_INITIAL_MEMBERS;"
           +  "num_ping_requests=NUM_PING_REQUESTS)?");
      // for multicast, we allow a client to become the coordinator and pray that jgroups
      // won't mess up too badly
      if (System.getProperty("p2p.enableInitialCoordinator") != null) {
        properties = replaceStrings(properties, "DISABLE_COORD", String.valueOf(!Boolean.getBoolean("p2p.enableInitialCoordinator")));
      }
      else {
        properties = replaceStrings(properties, "DISABLE_COORD", "false");
      }
      properties = replaceStrings(properties, "NUM_INITIAL_MEMBERS", System.getProperty("p2p.numInitialMembers", "2"));
    }
    
    long burstLimit = theConfig.getMcastFlowControl().getByteAllowance() / 4;
    burstLimit = Long.getLong("p2p.retransmit-burst-limit", burstLimit);
    properties = replaceStrings(properties, "RETRANSMIT_BURST_LIMIT", String.valueOf(burstLimit));

    long discoveryTimeout = Long.getLong("p2p.discoveryTimeout", 5000).longValue();
    properties = replaceStrings(properties, "DISCOVERY_TIMEOUT", ""+discoveryTimeout);

    int defaultJoinTimeout = 17000;
    int defaultNumPings = 1; // number of get_mbrs loops per findInitialMembers
    if (theConfig.getLocators().length() > 0 && !Locator.hasLocators()) {
      defaultJoinTimeout = 60000;
    }
    int joinTimeout = Integer.getInteger("p2p.joinTimeout", defaultJoinTimeout).intValue();
    int numPings = Integer.getInteger("p2p.discoveryProbes", defaultNumPings);
    properties = replaceStrings(properties, "JOIN_TIMEOUT", ""+joinTimeout);
    properties = replaceStrings(properties, "NUM_PING_REQUESTS", ""+numPings);
    properties = replaceStrings(properties, "LEAVE_TIMEOUT", System.getProperty("p2p.leaveTimeout", "5000"));
    properties = replaceStrings(properties, "SOCKET_TIMEOUT", System.getProperty("p2p.socket_timeout", "60000"));

    final String gossipRefreshRate;
    // if network partition detection is enabled, we must connect to the locators
    // more frequently in order to make sure we're not isolated from them
    if (theConfig.getEnableNetworkPartitionDetection()) {
      if (!SocketCreator.FORCE_DNS_USE) {
        IpAddress.resolve_dns = false; // do not resolve host names since DNS lookup can hang if the NIC fails
        SocketCreator.resolve_dns = false;
      }
    }

    gossipRefreshRate = System.getProperty("p2p.gossipRefreshRate", "57123");
    
    properties = replaceStrings(properties, "GOSSIP_REFRESH_RATE", gossipRefreshRate);


    // for the unicast recv buffer, we use a reduced buffer size if tcpconduit is enabled and multicast is
    // not being used
    if (JGroupMembershipManager.isMcastEnabled || transport.isTcpDisabled() ||
      (theConfig.getUdpRecvBufferSize() != DistributionConfig.DEFAULT_UDP_RECV_BUFFER_SIZE) ) {
      properties = replaceStrings(properties, "UDP_RECV_BUFFER_SIZE", ""+theConfig.getUdpRecvBufferSize());
    }
    else {
      properties = replaceStrings(properties, "UDP_RECV_BUFFER_SIZE", ""+DistributionConfig.DEFAULT_UDP_RECV_BUFFER_SIZE_REDUCED);
    }

    // bug #40436: both Windows and Linux machines will drop messages if the NIC is unplugged from the network.
    // Solaris is unknown at this point. (Bruce 3/23/09)  LOOPBACK ensures that failure-detection methods are
    // received by the sender
    properties = replaceStrings(properties, "LOOPBACK", "" + !Boolean.getBoolean("p2p.DISABLE_LOOPBACK"));

//    View.MAX_VIEW_SIZE = theConfig.getUdpFragmentSize() - 50 - 100;
//    AuthHeader.MAX_CREDENTIAL_SIZE = theConfig.getUdpFragmentSize() - 1000;

    int[] range = theConfig.getMembershipPortRange();
    properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE_START", ""+range[0]);
    properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE_END", ""+range[1]);
    
    properties = replaceStrings(properties, "UDP_SEND_BUFFER_SIZE", ""+theConfig.getUdpSendBufferSize());
    properties = replaceStrings(properties, "UDP_FRAGMENT_SIZE", ""+theConfig.getUdpFragmentSize());
    properties = replaceStrings(properties, "MAX_BUNDLE_SIZE", ""+(theConfig.getUdpFragmentSize()+3072));
    properties = replaceStrings(properties, "FC_MAX_CREDITS", ""+theConfig.getMcastFlowControl().getByteAllowance());
    properties = replaceStrings(properties, "FC_THRESHOLD", ""+theConfig.getMcastFlowControl().getRechargeThreshold());
    properties = replaceStrings(properties, "FC_MAX_BLOCK", ""+theConfig.getMcastFlowControl().getRechargeBlockMs());
    long fdt = (fdTimeout > 0) ? fdTimeout : theConfig.getMemberTimeout();
    String fdts = String.valueOf(fdt);
    properties = replaceStrings(properties, "MEMBER_TIMEOUT", fdts);
    properties = replaceStrings(properties, "CONNECT_TIMEOUT", fdts);
    // The default view-ack timeout in 7.0 is 12347 ms but is adjusted based on the member-timeout.
    // We don't want a longer timeout than 12437 because new members will likely time out trying to 
    // connect because their join timeouts are set to expect a shorter period
    int ackCollectionTimeout = theConfig.getMemberTimeout() * 2 * 12437 / 10000;
    if (ackCollectionTimeout < 1500) {
      ackCollectionTimeout = 1500;
    } else if (ackCollectionTimeout > 12437) {
      ackCollectionTimeout = 12437;
    }
    ackCollectionTimeout = Integer.getInteger("gemfire.VIEW_ACK_TIMEOUT", ackCollectionTimeout).intValue();
    properties = replaceStrings(properties, "ACK_COLLECTION_TIMEOUT", ""+ackCollectionTimeout);
    properties = replaceStrings(properties, "MAX_TRIES", System.getProperty("gemfire.FD_MAX_TRIES", "1"));
    properties = replaceStrings(properties, "VIEW_SYNC_INTERVAL", String.valueOf(fdt));

    if (!Boolean.getBoolean("p2p.enableBatching")) {
      properties = replaceStrings(properties, "ENABLE_BUNDLING", "false");
      properties = replaceStrings(properties, "BUNDLING_TIMEOUT", "30");
    }
    else {
      properties = replaceStrings(properties, "ENABLE_BUNDLING", "true");
      properties = replaceStrings(properties, "BUNDLING_TIMEOUT", System.getProperty("p2p.batchFlushTime", "30"));
    }
    
    properties = replaceStrings(properties, "OUTGOING_PACKET_HANDLER",
      String.valueOf(Boolean.getBoolean("p2p.outgoingPacketHandler")));

    properties = replaceStrings(properties, "INCOMING_PACKET_HANDLER",
      String.valueOf(Boolean.getBoolean("p2p.incomingPacketHandler")));
    
    properties = replaceStrings(properties, "PARTITION_DETECTION",
        String.valueOf(theConfig.getEnableNetworkPartitionDetection()));

    int threshold = Integer.getInteger("gemfire.network-partition-threshold", 51);
    if (threshold < 51) threshold = 51;
    if (threshold > 100) threshold = 100;
    properties = replaceStrings(properties, "PARTITION_THRESHOLD",
        String.valueOf(threshold));
    
    int weight = Integer.getInteger("gemfire.member-weight", 0);
    properties = replaceStrings(properties, "MEMBER_WEIGHT", String.valueOf(weight));
    
    properties = replaceStrings(properties, "GOSSIP_SERVER_WAIT_TIME", ""+theConfig.getLocatorWaitTime());

    if (logger.isDebugEnabled()) {
      logger.debug("Jgroups configuration: {}", properties);
    }
    
    JChannel myChannel = null;

    synchronized (latestViewLock) {
      try {
        this.isJoining = true; // added for bug #44373

        // connect
        long start = System.currentTimeMillis();

        boolean reattempt;

        String channelName = GossipServer.CHANNEL_NAME; 

        boolean debugConnect = Boolean.getBoolean("p2p.debugConnect");

        puller = new Puller();

        GFBasicAdapter jgBasicAdapter = new GFJGBasicAdapter();
        GFPeerAdapter jgPeerAdapter = new GFJGPeerAdapter(this, stats);

        do {
          reattempt = false;

          myChannel = new JChannel(properties, jgBasicAdapter, jgPeerAdapter, 
              DistributionStats.enableClockStats, enableJgStackStats);
          myChannel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
          myChannel.setOpt(Channel.AUTO_GETSTATE, Boolean.FALSE);
          myChannel.setOpt(Channel.LOCAL, Boolean.FALSE);
          this.channelInitialized = true;

          // [bruce] set the puller now to avoid possibility of loss of messages
          myChannel.setReceiver(puller);

          if (debugConnect) {
            setDebugJGroups(true);
          }
          
          
          // [bruce] the argument to connect() goes out with each message and forms the initial
          //         criterion for whether a message will be accepted for us by jgroups
          try {
            myChannel.connect(channelName);
          }
          catch (ShunnedAddressException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("Address was shunned by membership coordinator - will reattempt");
            }
            myChannel.close();
            reattempt = true;
          }
        }
        while (reattempt);

        long delta = System.currentTimeMillis() - start;
        if (debugConnect) {
          setDebugJGroups(false);
        }
        if (logger.isTraceEnabled(LogMarker.JGROUPS)) {
          logger.trace(LogMarker.JGROUPS, "Connected JGroups stack: {}", myChannel.printProtocolSpec(false));
        }

        // Populate our initial view
        View v = myChannel.getView();
        if (v == null)
          throw new DistributionException(LocalizedStrings.JGroupMembershipManager_NULL_VIEW_FROM_JGROUPS.toLocalizedString(),
              new InternalGemFireError(LocalizedStrings.JGroupMembershipManager_NO_VIEW.toLocalizedString()));
        if (this.directChannel != null) {
          this.directChannel.setMembershipSize(v.getMembers().size());
        }
        lastViewId = v.getVid().getId();
        latestView = viewToMemberView(v);
        if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS)) {
          logger.trace(LogMarker.DISTRIBUTION_VIEWS, "JGroups: initial view is {}", DistributionManager.printView(latestView));
        }

        logger.info(LogMarker.DISTRIBUTION, LocalizedMessage.create(
            LocalizedStrings.JGroupMembershipManager_CONNECTED_TO_JGROUPS_CHANNEL_TOOK__0__MS, delta));

        // get the unicast protocol so we can flush it when necessary
        ucastProtocol = (com.gemstone.org.jgroups.protocols.UNICAST)myChannel
        .getProtocolStack().findProtocol("UNICAST");
        Assert.assertTrue(ucastProtocol != null, 
        "Malformed protocol stack is missing UNICAST");

        // Load this protocol for bug 36851 and for use in emergencyClose
        fdProtocol = (FD)myChannel.getProtocolStack().findProtocol("FD");

        // get the VERIFY_SUSPECT protocol for liveness notification
        verifySuspectProtocol = (VERIFY_SUSPECT)myChannel.getProtocolStack()
          .findProtocol("VERIFY_SUSPECT");

        // Get other protocols we should touch during emergencyClose
        fdSockProtocol = (FD_SOCK)myChannel.getProtocolStack().findProtocol("FD_SOCK");
        udpProtocol = (UDP)myChannel.getProtocolStack().findProtocol("UDP");
        tcpProtocol = (TCP)myChannel.getProtocolStack().findProtocol("TCP");
        nakAckProtocol = (NAKACK)myChannel.getProtocolStack().findProtocol("NAKACK");
        gmsProtocol = (GMS)myChannel.getProtocolStack().findProtocol("GMS");

        return myChannel;

      } catch (RuntimeException ex) {
        throw ex;

      }
      catch (Exception ex) {
        if (ex.getCause() != null && ex.getCause().getCause() instanceof SystemConnectException) {
          throw (SystemConnectException)(ex.getCause().getCause());
        }
        throw new DistributionException(LocalizedStrings.JGroupMembershipManager_AN_EXCEPTION_WAS_THROWN_WHILE_CONNECTING_TO_JGROUPS.toLocalizedString(), ex);
      }
      finally {
        this.isJoining = false;
      }
    } // synchronized
  }

  
  public JGroupMembershipManager() {
    // caller must invoke initialize() after creating a JGMM
  }
  
  public JGroupMembershipManager initialize(
      DistributedMembershipListener listener,
      DistributionConfig config,
      RemoteTransportConfig transport,
      DMStats stats
      ) throws ConnectionException
  {
    Assert.assertTrue(listener != null);
    Assert.assertTrue(config != null);
    
    //you can use a dummy stats object to remove most stat overhead
    //this.stats = new com.gemstone.gemfire.distributed.internal.LonerDistributionManager.DummyDMStats();
    this.stats = stats;

    this.listener = listener;
    this.dconfig = config;
    this.membershipCheckTimeout = config.getSecurityPeerMembershipTimeout();
    this.wasReconnectingSystem = transport.getIsReconnectingDS();
    this.oldDSMembershipSocket = (DatagramSocket)transport.getOldDSMembershipInfo();
    
    if (!config.getDisableTcp()) {
      dcReceiver = new MyDCReceiver(listener);
      directChannel = new DirectChannel(this, dcReceiver, config, null);
    }

    int dcPort = 0;
    if (!config.getDisableTcp()) {
      dcPort = directChannel.getPort();
    }
    // FIXME: payload handling is inconsistent (note from JasonP.)
    MemberAttributes.setDefaults(dcPort,
        MemberAttributes.DEFAULT.getVmPid(),
        MemberAttributes.DEFAULT.getVmKind(),
        MemberAttributes.DEFAULT.getVmViewId(),
        MemberAttributes.DEFAULT.getName(),
        MemberAttributes.DEFAULT.getGroups(), MemberAttributes.DEFAULT.getDurableClientAttributes());

    this.vmKind = MemberAttributes.DEFAULT.getVmKind(); // we need this during jchannel startup

    surpriseMemberTimeout = Math.max(20 * DistributionConfig.DEFAULT_MEMBER_TIMEOUT,
        20 * config.getMemberTimeout());
    surpriseMemberTimeout = Integer.getInteger("gemfire.surprise-member-timeout", surpriseMemberTimeout).intValue();

    try {
      this.channel = createChannel(transport, config);
    }
    catch (RuntimeException e) {
      if (directChannel != null) {
        directChannel.disconnect(e);
      }
      throw e;
    }

    IpAddress myAddr = (IpAddress)channel.getLocalAddress();
    
    MemberAttributes.setDefaults(dcPort,
        MemberAttributes.DEFAULT.getVmPid(),
        MemberAttributes.DEFAULT.getVmKind(),
        myAddr.getBirthViewId(),
        MemberAttributes.DEFAULT.getName(),
        MemberAttributes.DEFAULT.getGroups(), MemberAttributes.DEFAULT.getDurableClientAttributes());
    
    if (directChannel != null) {
      directChannel.getConduit().setVmViewID(myAddr.getBirthViewId());
    }

    myMemberId = new InternalDistributedMember(myAddr.getIpAddress(),
                                               myAddr.getPort(),
                                               myAddr.splitBrainEnabled(),
                                               myAddr.preferredForCoordinator(),
                                               MemberAttributes.DEFAULT);
    
    // in order to debug startup issues it we need to announce the membership
    // ID as soon as we know it
    logger.info(LocalizedMessage.create(LocalizedStrings.JGroupMembershipManager_entered_into_membership_in_group_0_with_id_1,
        new Object[]{myMemberId}));

    if (!dconfig.getDisableTcp()) {
      this.conduit = directChannel.getConduit();
      directChannel.setLocalAddr(myMemberId);
      Stub stub = conduit.getId();
      memberToStubMap.put(myMemberId, stub);
      stubToMemberMap.put(stub, myMemberId);
    }
    
    this.hasConnected = true;
    
    return this;
  }
  
  /** this is invoked by JGroups when there is a loss of quorum in the membership system */
  public void quorumLost(Set failures, List remaining) {
    // notify of quorum loss if split-brain detection is enabled (meaning we'll shut down) or
    // if the loss is more than one member
    
    boolean notify = failures.size() > 1;
    if (!notify) {
      JChannel ch = this.channel;
      Address localAddr = (ch != null) ? ch.getLocalAddress() : null;
      notify = (localAddr != null) && localAddr.splitBrainEnabled();
    }
    
    if (notify) {
      if (inhibitForceDisconnectLogging) {
        if (logger.isDebugEnabled()) {
          logger.debug("<ExpectedException action=add>Possible loss of quorum</ExpectedException>");
        }
      }
      logger.fatal(LocalizedMessage.create(
          LocalizedStrings.JGroupMembershipManager_POSSIBLE_LOSS_OF_QUORUM_DETECTED, new Object[] {failures.size(), failures}));
      if (inhibitForceDisconnectLogging) {
        if (logger.isDebugEnabled()) {
          logger.debug("<ExpectedException action=remove>Possible loss of quorum</ExpectedException>");
        }
      }
      
  
      // get member IDs for the collections so we can notify the
      // gemfire distributed system membership listeners
      Set<InternalDistributedMember> idmFailures = new HashSet<InternalDistributedMember>();
      for (Iterator it=failures.iterator(); it.hasNext(); ) {
        IpAddress ipaddr = (IpAddress)it.next();
        JGroupMember jgm = new JGroupMember(ipaddr);
        InternalDistributedMember mbr = new InternalDistributedMember(jgm);
        idmFailures.add(mbr);
      }
      
      List<InternalDistributedMember> idmRemaining = new ArrayList<InternalDistributedMember>(remaining.size());
      for (Iterator it=remaining.iterator(); it.hasNext(); ) {
        IpAddress ipaddr = (IpAddress)it.next();
        JGroupMember jgm = new JGroupMember(ipaddr);
        InternalDistributedMember mbr = new InternalDistributedMember(jgm);
        idmRemaining.add(mbr);
      }
  
      try {
        this.listener.quorumLost(idmFailures, idmRemaining);
      } catch (CancelException e) {
        // safe to ignore - a forced disconnect probably occurred
      }
    }
  }
  
  
  public boolean testMulticast() {
    if (isMcastEnabled) {
      return this.udpProtocol.testMulticast(dconfig.getMemberTimeout());
    } else {
      return true;
    }
  }
  
  /**
   * Remove a member, or queue a startup operation to do so
   * @param dm the member to shun
   * @param crashed true if crashed
   * @param reason the reason, esp. if crashed
   */
  protected void handleOrDeferRemove(InternalDistributedMember dm,
      boolean crashed, String reason) {
    synchronized(startupLock) {
      if (!processingEvents) {
        startupMessages.add(new StartupEvent(dm, crashed, reason));
        return;
      }
    }
    removeMember(dm, crashed, reason);
  }
  
  /**
   * Remove a member.  {@link #latestViewLock} must be held
   * before this method is called.  If member is not already shunned,
   * the uplevel event handler is invoked.
   * 
   * @param dm
   * @param crashed
   * @param reason
   */
  protected void removeWithViewLock(InternalDistributedMember dm,
      boolean crashed, String reason) {
    boolean wasShunned = isShunned(dm);

    // Delete resources
    destroyMember(dm, crashed, reason);

    if (wasShunned) {
      return; // Explicit deletion, no upcall.
    }
    
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS)) {
      logger.trace(LogMarker.DISTRIBUTION_VIEWS, "Membership: dispatching uplevel departure event for < {} >", dm);
    }
    
    try {
      listener.memberDeparted(dm, crashed, reason);
    }
    catch (DistributedSystemDisconnectedException se) {
      // let's not get huffy about it
    }
  }
  
  /**
   * Automatic removal of a member (for internal
   * use only).  Synchronizes on {@link #latestViewLock} and then deletes
   * the member.
   * 
   * @param dm
   * @param crashed
   * @param reason
   */
  protected void removeMember(InternalDistributedMember dm,
      boolean crashed, String reason)
  {
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS)) {
      StringBuffer sb = new StringBuffer(200);
      sb.append("Membership: removing <")
         .append(dm)
         .append(">; crashed = ")
         .append(crashed)
         .append("; reason = ");
      if (reason != null && (reason.indexOf("NoSuchMemberException") >= 0)) {
        sb.append("tcp/ip connections closed");
      }
      else {
        sb.append(reason);
      }
      logger.trace(LogMarker.DISTRIBUTION_VIEWS, sb);
    }
    synchronized (latestViewLock) {
      removeWithViewLock(dm, crashed, reason);
    }
  }
  
 
  /**
   * Process a surprise connect event, or place it on the startup queue.
   * @param member the member
   * @param stub its stub
   */
  protected void handleOrDeferSurpriseConnect(InternalDistributedMember member,
      Stub stub) {
    synchronized (startupLock) {
      if (!processingEvents) {
        startupMessages.add(new StartupEvent(member, stub));
        return;
      }
    }
    processSurpriseConnect(member, stub);
  }
  
  public void startupMessageFailed(DistributedMember mbr, String failureMessage) {
    // fix for bug #40666
    addShunnedMember((InternalDistributedMember)mbr);
    // fix for bug #41329, hang waiting for replies
    try {
      listener.memberDeparted((InternalDistributedMember)mbr, true, "failed to pass startup checks");
    }
    catch (DistributedSystemDisconnectedException se) {
      // let's not get huffy about it
    }
  }

  
  /**
   * Logic for handling a direct connection event (message received
   * from a member not in the JGroups view).  Does not employ the
   * startup queue.
   * <p>
   * Must be called with {@link #latestViewLock} held.  Waits
   * until there is a stable view.  If the member has already
   * been added, simply returns; else adds the member.
   * 
   * @param dm the member joining
   * @param stub the member's stub
   */
  public boolean addSurpriseMember(DistributedMember dm, 
      Stub stub) {
    final InternalDistributedMember member = (InternalDistributedMember)dm;
    Stub s = null;
    boolean warn = false;
    
    synchronized(latestViewLock) {
      // At this point, the join may have been discovered by
      // other means.
      if (latestView.contains(member)) {
        return true;
      }
      if (surpriseMembers.containsKey(member)) {
        return true;
      }
      if (latestView.getViewNumber() > member.getVmViewId()) {
        // tell the process that it should shut down distribution.
        // Run in a separate thread so we don't hold the view lock during the request.  Bug #44995
        new Thread(Thread.currentThread().getThreadGroup(),
            "Removing shunned GemFire node " + member) {
          @Override
          public void run() {
            // fix for bug #42548
            // this is an old member that shouldn't be added
            logger.fatal(LocalizedMessage.create(
                LocalizedStrings.JGroupMembershipManager_Invalid_Surprise_Member, new Object[]{member, latestView}));
            requestMemberRemoval(member, "this member is no longer in the view but is initiating connections");
          }
        }.start();
        addShunnedMember(member);
        return false;
      }

      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS)) {
        logger.trace(LogMarker.DISTRIBUTION_VIEWS, "Membership: Received message from surprise member: <{}>.  My view number is {} it is {}", 
            member, latestView.getViewNumber(), member.getVmViewId());
      }

      // Adding him to this set ensures we won't remove him if a new
      // JGroups view comes in and he's still not visible.
      surpriseMembers.put(member, Long.valueOf(System.currentTimeMillis()));

      if (shutdownInProgress()) {
        if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS)) {
          logger.trace(LogMarker.DISTRIBUTION_VIEWS, "Membership: new member during shutdown ignored: <{}>", member); 
        }

        // Force disconnect, esp. the TCPConduit
        String msg = LocalizedStrings.JGroupMembershipManager_THIS_DISTRIBUTED_SYSTEM_IS_SHUTTING_DOWN.toLocalizedString();
        if (directChannel != null) {
          try {
            directChannel.closeEndpoint(member, msg);
          } catch (DistributedSystemDisconnectedException e) {
            // ignore - happens during shutdown
          }
        }
        destroyMember(member, false, msg); // for good luck
        return true; // allow during shutdown
      }

      if (isShunned(member)) {
        warn = true;
        surpriseMembers.remove(member);
      } else {

        // Now that we're sure the member is new, add them.
        if (logger.isTraceEnabled(LogMarker.DM_VIEWS)) {
          logger.trace(LogMarker.DM_VIEWS, "Membership: Processing surprise addition <{}>", member);
        }

        // make sure the surprise-member cleanup task is running
        if (this.cleanupTimer == null) {
          startCleanupTimer();
        } // cleanupTimer == null

        // fix for bug #42006, lingering old identity
        Object oldStub = this.memberToStubMap.remove(member);
        if (oldStub != null) {
          this.stubToMemberMap.remove(oldStub);
        }

        s = stub == null ? getStubForMember(member) : stub;
        // Make sure that channel information is consistent
        addChannel(member, s);

        // Ensure that the member is accounted for in the view
        // Conjure up a new view including the new member. This is necessary
        // because we are about to tell the listener about a new member, so
        // the listener should rightfully expect that the member is in our
        // membership view.

        // However, we put the new member at the end of the list.  This
        // should ensure he's not chosen as an elder.
        // This will get corrected when he finally shows up in the JGroups
        // view.
        NetView newMembers = new NetView(latestView, latestView.getViewNumber());
        newMembers.add(member);
        latestView = newMembers;
      }
    }
    if (warn) { // fix for bug #41538 - deadlock while alerting
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_IGNORING_SURPRISE_CONNECT_FROM_SHUNNED_MEMBER_0, member));
    } else {
      listener.newMemberConnected(member, s);
    }
    return !warn;
  }
  

  /** starts periodic task to perform cleanup chores such as expire surprise members */
  private void startCleanupTimer() {
    synchronized(this.latestViewLock) {
      if (this.cleanupTimer != null) {
        return;
      }
      DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      if (ds != null && ds.isConnected()) {
        this.cleanupTimer = new SystemTimer(ds, true);
        SystemTimer.SystemTimerTask st = new SystemTimer.SystemTimerTask() {
          @Override
          public void run2() {
            synchronized(latestViewLock) {
              long oldestAllowed = System.currentTimeMillis() - surpriseMemberTimeout;
              for (Iterator it=surpriseMembers.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry entry = (Map.Entry)it.next();
                Long birthtime = (Long)entry.getValue();
                if (birthtime.longValue() < oldestAllowed) {
                  it.remove();
                  InternalDistributedMember m = (InternalDistributedMember)entry.getKey();
                  logger.info(LocalizedMessage.create(
                      LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_EXPIRING_MEMBERSHIP_OF_SURPRISE_MEMBER_0, m));
                  removeWithViewLock(m, true, "not seen in membership view in "
                      + surpriseMemberTimeout + "ms");
                }
              }
            }
          }
        };
        this.cleanupTimer.scheduleAtFixedRate(st, surpriseMemberTimeout, surpriseMemberTimeout/3);
      } // ds != null && ds.isConnected()
    }
  }
  /**
   * Dispatch the distribution message, or place it on the startup queue.
   * 
   * @param msg the message to process
   */
  protected void handleOrDeferMessage(DistributionMessage msg) {
    synchronized(startupLock) {
      if (!processingEvents) {
        startupMessages.add(new StartupEvent(msg));
        return;
      }
    }
    processMessage(msg);
  }
  
  public void warnShun(DistributedMember m) {
    synchronized (latestViewLock) {
      if (!shunnedMembers.containsKey(m)) {
        return; // not shunned
      }
      if (shunnedAndWarnedMembers.contains(m)) {
        return; // already warned
      }
      shunnedAndWarnedMembers.add(m);
    } // synchronized
    // issue warning outside of sync since it may cause messaging and we don't
    // want to hold the view lock while doing that
    logger.warn(LocalizedMessage.create(LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_DISREGARDING_SHUNNED_MEMBER_0, m));
  }
  
  /**
   * Logic for processing a distribution message.  
   * <p>
   * It is possible to receive messages not consistent with our view.
   * We handle this here, and generate an uplevel event if necessary
   * @param msg the message
   */
  protected void processMessage(DistributionMessage msg) {
    boolean isNew = false;
    InternalDistributedMember m = msg.getSender();
    boolean shunned = false;

    // First grab the lock: check the sender against our stabilized view.
    synchronized (latestViewLock) {
      if (isShunned(m)) {
        if (msg instanceof StartupMessage) {
          endShun(m);
        }
        else {
          // fix for bug 41538 - sick alert listener causes deadlock
          // due to view lock being held during messaging
          shunned = true;
        }
      } // isShunned

      if (!shunned) {
        isNew = !latestView.contains(m) && !surpriseMembers.containsKey(m);

        // If it's a new sender, wait our turn, generate the event
        if (isNew) {
          shunned = !addSurpriseMember(m, getStubForMember(m));
        } // isNew
      }

      // Latch the view before we unlock
    } // synchronized
    
    if (shunned) { // bug #41538 - shun notification must be outside synchronization to avoid hanging
      warnShun(m);
//      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS)) {
        logger.info(/*LogMarker.DISTRIBUTION_VIEWS, */"Membership: Ignoring message from shunned member <{}>:{}", m, msg);
//      }
      throw new MemberShunnedException(getStubForMember(m));
    }
    
    // TODO: once upon a time, in a galaxy far far away...
    //
    // I would love for this upcall to be delivered in
    // an environment that guarantees message stability, i.e.
    // use a conditional variable to prevent view changes
    // to occur while this callback is being processed.
    //
    // In practice, some of the getInitialImage stuff seems
    // to cause deadlocks of almost dlock-level complexity
    // when I implemented that, so I've backed off.
    //
    // (Implementation note: I had a viewInUseCount integer
    // and a viewStable conditional variable, so that, with
    // the latestViewLock held, this method incremented the count
    // before and decremented it afterwards; if the count is
    // 0, the viewStable got a signalAll() to allow the
    // view changers to wake up and process their events.)
    //
    // The downside of the current implementation is that
    // the sender of this message might disappear before the
    // message can be processed.  It's also possible for new
    // members to arrive while a message is being processed,
    // but my intuition suggests this is not as likely to
    // cause a problem.
    //
    // As it stands, the system is (mostly) tolerant of these issues, but I'm 
    // very concerned that there may be subtle lock/elder/HA bugs that we
    // haven't caught yet that may be affected by this.
    listener.messageReceived(msg);
  }

  /**
   * Process a new view object, or place on the startup queue
   * @param viewArg the new view
   */
  protected void handleOrDeferViewEvent(View viewArg) {
    if (this.isJoining) {
      // bug #44373 - queue all view messages while connecting the jgroups channel.
      // This is done under the latestViewLock, but we can't block here because
      // we're sitting in the UDP reader thread.
      synchronized(startupLock) {
        startupMessages.add(new StartupEvent(viewArg));
        return;
      }
    }
    synchronized (latestViewLock) {
      synchronized(startupLock) {
        if (!processingEvents) {
          startupMessages.add(new StartupEvent(viewArg));
          return;
        }
      }
      // view processing can take a while, so we use a separate thread
      // to avoid blocking the jgroups stack
      NetView newView = viewToMemberView(viewArg);
      long newId = viewArg.getVid().getId();
      if (logger.isTraceEnabled(LogMarker.DM_VIEWS)) {
        logger.trace(LogMarker.DM_VIEWS, "Membership: queuing new view for processing, id = {}, view = {}", 
            newId, newView);
      }
      ViewMessage v = new ViewMessage(myMemberId, newId, newView,
          JGroupMembershipManager.this);

      listener.messageReceived(v);
    }
  }
  
  /**
   * Process a new view object, or place on the startup queue
   * @param suspectInfo the suspectee and suspector
   */
  protected void handleOrDeferSuspect(SuspectMember suspectInfo) {
    synchronized (latestViewLock) {
      synchronized(startupLock) {
        if (!processingEvents) {
          return;
        }
      }
      InternalDistributedMember suspect = getMemberFromIpAddress((IpAddress)suspectInfo.suspectedMember, true);
      InternalDistributedMember who = getMemberFromIpAddress((IpAddress)suspectInfo.whoSuspected, true);
      this.suspectedMembers.put(suspect, Long.valueOf(System.currentTimeMillis()));
      try {
        listener.memberSuspect(suspect, who);
      }
      catch (DistributedSystemDisconnectedException se) {
        // let's not get huffy about it
      }
    }
  }

  /**
   * Process a potential direct connect.  Does not use
   * the startup queue.  It grabs the {@link #latestViewLock} 
   * and then processes the event.
   * <p>
   * It is a <em>potential</em> event, because we don't know until we've
   * grabbed a stable view if this is really a new member.
   * 
   * @param member
   * @param stub
   */
  private void processSurpriseConnect(
      InternalDistributedMember member, 
      Stub stub) 
  {
    synchronized (latestViewLock) {
      addSurpriseMember(member, stub);
    }
  }
  
  /**
   * Dispatch routine for processing a single startup event
   * @param o the startup event to handle
   */
  private void processStartupEvent(StartupEvent o) {
    // Most common events first
    
    if (o.isDistributionMessage()) { // normal message
      try {
        processMessage(o.dmsg);
      }
      catch (MemberShunnedException e) {
        // message from non-member - ignore
      }
    } 
    else if (o.isJgView()) { // view event
      processView(o.jgView.getVid().getId(), viewToMemberView(o.jgView));
    }
    else if (o.isDepartureEvent()) { // departure
      removeMember(o.member, o.crashed, o.reason);
    }
    else if (o.isConnect()) { // connect
      processSurpriseConnect(o.member, o.stub);
    }
    
    else // sanity
      throw new InternalGemFireError(LocalizedStrings.JGroupMembershipManager_UNKNOWN_STARTUP_EVENT_0.toLocalizedString(o));
  }
  
  /**
   * Special mutex to create a critical section for
   * {@link #startEventProcessing()}
   */
  private final Object startupMutex = new Object();

  
  public void startEventProcessing()
  {
    // Only allow one thread to perform the work
    synchronized (startupMutex) {
      if (logger.isDebugEnabled())
        logger.debug("Membership: draining startup events.");
      // Remove the backqueue of messages, but allow
      // additional messages to be added.
      for (;;) {
        StartupEvent ev;
        // Only grab the mutex while reading the queue.
        // Other events may arrive while we're attempting to
        // drain the queue.  This is OK, we'll just keep processing
        // events here until we've caught up.
        synchronized (startupLock) {
          int remaining = startupMessages.size();
          if (remaining == 0) {
            // While holding the lock, flip the bit so that
            // no more events get put into startupMessages, and
            // notify all waiters to proceed.
            processingEvents = true;
            startupLock.notifyAll();
            break;  // ...and we're done.
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Membership: {} remaining startup message(s)", remaining);
          }
          ev = (StartupEvent)startupMessages.removeFirst();
        } // startupLock
        try {
          processStartupEvent(ev);
        }
        catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.warn(LocalizedMessage.create(LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_ERROR_HANDLING_STARTUP_EVENT), t);
        }
        
      } // for
      if (logger.isDebugEnabled())
        logger.debug("Membership: finished processing startup events.");
    } // startupMutex
  }

 
  public void waitForEventProcessing() throws InterruptedException {
    // First check outside of a synchronized block.  Cheaper and sufficient.
    if (Thread.interrupted()) throw new InterruptedException();
    if (processingEvents)
      return;
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: waiting until the system is ready for events");
    }
    for (;;) {
      conduit.getCancelCriterion().checkCancelInProgress(null);
      synchronized (startupLock) {
        // Now check using a memory fence and synchronization.
        if (processingEvents)
          break;
        boolean interrupted = Thread.interrupted();
        try {
          startupLock.wait();
        }
        catch (InterruptedException e) {
          interrupted = true;
          conduit.getCancelCriterion().checkCancelInProgress(e);
        }
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // synchronized
    } // for
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: continuing");
    }
  }


  public Object getViewLock() {
    return this.latestViewLock;
  }

  /**
   * Returns a copy (possibly not current) of the current
   * view (a list of {@link DistributedMember}s)
   */
  public NetView getView()
  {
    // Grab the latest view under a mutex...
    NetView v;

    synchronized (latestViewLock) {
      v = latestView;
    }

    // Create a copy (read-only)
    NetView result = new NetView(v.size(), v.getViewNumber());
    result.setCreator(v.getCreator());
    
    for (int i = 0; i < v.size(); i ++) {
      InternalDistributedMember m = (InternalDistributedMember)v.elementAt(i);
      if (isShunned(m)) {
        continue;
      }
      result.add(m);
    }
    result.setLeadMember(v.getLeadMember());
    return result;
  }
  
  /**
   * test hook<p>
   * The lead member is the eldest member with partition detection enabled.<p>
   * If no members have partition detection enabled, there will be no
   * lead member and this method will return null.
   * @return the lead member associated with the latest view
   */
  public DistributedMember getLeadMember() {
    // note - we go straight to the jgroups stack because the
    // DistributionManager queues view changes in a serial executor, where
    // they're asynchronously installed.  The DS may still see the old leader
    if (gmsProtocol == null) {
      return null;
    }
    IpAddress jlead = (IpAddress)gmsProtocol.getLeadMember();
    InternalDistributedMember leader;
    if (jlead == null) {
      leader = null;
    }
    else {
      leader = getMemberFromIpAddress(jlead, true);
    }
    return leader;
  }
  
  /**
   * test hook
   * @return the current membership view coordinator
   */
  public DistributedMember getCoordinator() {
    // note - we go straight to the jgroups stack because the
    // DistributionManager queues view changes in a serial executor, where
    // they're asynchronously installed.  The DS may still see the old coordinator
    IpAddress jcoord = (IpAddress)gmsProtocol.determineCoordinator();
    InternalDistributedMember dm;
    if (jcoord == null) {
      dm = null;
    }
    else {
      dm = getMemberFromIpAddress(jcoord, true);
    }
    return dm;
  }
  /** cached group-management system protocol for test hook */
  GMS gmsProtocol;

  public boolean memberExists(InternalDistributedMember m) {
    Vector v;
    
    synchronized (latestViewLock) {
      v = latestView;
    }
    return v.contains(m);
  }
  
  /**
   * Returns the identity associated with this member. WARNING: this value will
   * be returned after the channel is closed, but in that case it is good for
   * logging purposes only. :-)
   */
  public InternalDistributedMember getLocalMember()
  {
    return myMemberId;
  }

  public void postConnect()
  {
    if (channelPause > 0) {
      logger.info(LocalizedMessage.create(
          LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_PAUSING_TO_ALLOW_OTHER_CONCURRENT_PROCESSES_TO_JOIN_THE_DISTRIBUTED_SYSTEM));
      try {
        Thread.sleep(channelPause);
      }
      catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      channelPause = 0;
    }
  }
  
  /**
   * @see SystemFailure#loadEmergencyClasses()
   /**
   * break any potential circularity in {@link #loadEmergencyClasses()}
   */
  private static volatile boolean emergencyClassesLoaded = false;

  /**
   * inhibits logging of ForcedDisconnectException to keep dunit logs clean
   * while testing this feature
   */
  protected static volatile boolean inhibitForceDisconnectLogging;
  
  /**
   * Ensure that the critical classes from JGroups and the TCP conduit
   * implementation get loaded.
   * 
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    if (emergencyClassesLoaded) return;
    emergencyClassesLoaded = true;
    com.gemstone.org.jgroups.protocols.FD_SOCK.loadEmergencyClasses();
    com.gemstone.org.jgroups.protocols.FD.loadEmergencyClasses();
    DirectChannel.loadEmergencyClasses();
    ProtocolStack.loadEmergencyClasses();
    UDP.loadEmergencyClasses();
    TP.loadEmergencyClasses();
  }
  /**
   * Close the receiver, avoiding all potential deadlocks and
   * eschewing any attempts at being graceful.
   * 
   * @see SystemFailure#emergencyClose()
   */
  public void emergencyClose() {
    final boolean DEBUG = SystemFailure.TRACE_CLOSE;
    
    setShutdown(); 
    puller = null;
    // Problematic; let each protocol discover and die on its own
//    if (channel != null && channel.isConnected()) {
//      channel.close();
//    }
    
    // We can't call close() because they will allocate objects.  Attempt
    // a surgical strike and touch the important protocols.
    
    // MOST important, kill the FD protocols...
    if (fdSockProtocol != null) {
      if (DEBUG) {
        System.err.println("DEBUG: emergency close of FD_SOCK");
      }
      fdSockProtocol.emergencyClose();
    }
    if (fdProtocol != null) {
      if (DEBUG) {
        System.err.println("DEBUG: emergency close of FD");
      }
      fdProtocol.emergencyClose();
    }
    
    // Close the TCPConduit sockets...
    if (directChannel != null) {
      if (DEBUG) {
        System.err.println("DEBUG: emergency close of DirectChannel");
      }
      directChannel.emergencyClose();
    }
    
    // Less important, but for cleanliness, get rid of some
    // datagram sockets...
    if (udpProtocol != null) {
      if (DEBUG) {
        System.err.println("DEBUG: emergency close of UDP");
      }
      udpProtocol.emergencyClose();
    }
    if (tcpProtocol != null) {
      if (DEBUG) {
        System.err.println("DEBUG: emergency close of TCP");
      }
      tcpProtocol.emergencyClose();
    }

    // Clear the TimeScheduler, it might act up and cause problems?
    ProtocolStack ps = this.channel.getProtocolStack();
    if (ps != null) {
      if (DEBUG) {
        System.err.println("DEBUG: emergency close of ProtocolStack");
      }
      ps.emergencyClose();
    }
    
    // TODO: could we guarantee not to allocate objects?  We're using Darrel's 
    // factory, so it's possible that an unsafe implementation could be
    // introduced here.
//    stubToMemberMap.clear();
//    memberToStubMap.clear();
    
    this.timer.cancel();
    
    if (DEBUG) {
      System.err.println("DEBUG: done closing JGroupMembershipManager");
    }
  }
  
  
  /**
   * in order to avoid split-brain occurring when a member is shutting down due to
   * race conditions in view management we add it as a shutdown member when we receive
   * a shutdown message.  This is not the same as a SHUNNED member.
   */
  public void shutdownMessageReceived(InternalDistributedMember id, String reason) {
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: recording shutdown status of {}", id);
    }
    synchronized(this.shutdownMembers) { 
      this.shutdownMembers.put(id, id);
    }
  }
  
  /**
   * returns true if a shutdown message has been received from the given address but
   * that member is still in the membership view or is a surprise member.
   */
  public boolean isShuttingDown(IpAddress addr) {
    InternalDistributedMember mbr = (InternalDistributedMember)ipAddrToMemberMap.get(addr);
    if (mbr == null) {
      JGroupMember m = new JGroupMember(addr);
      mbr = new InternalDistributedMember(m);
    }
    synchronized(shutdownMembers) {
      return shutdownMembers.containsKey(mbr);
    }
  }

  
  public void shutdown()
  {
    setShutdown(); // for safety
    puller = null;  // this tells other methods that a shutdown has been requested
    
    // channel.disconnect(); close does this for us
    if (channel != null && channel.isConnected()) {
      try {
        channel.closeAsync();
      }
      catch (DistributedSystemDisconnectedException e) {
        // this can happen if the stack was already shut down by a ForcedDisconnectException
      }
    }
    // [bruce] Do not null out the channel w/o adding appropriate synchronization
    if (directChannel != null) {
      directChannel.disconnect(null);

      // Make sure that channel information is consistent
      // Probably not important in this particular case, but just
      // to be consistent...
      synchronized (latestViewLock) {
        destroyMember(myMemberId, false, "orderly shutdown");
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: channel closed");
    }
  }
  
  public void uncleanShutdown(String reason, final Exception e) {
    inhibitForcedDisconnectLogging(false);
    
    if (this.channel != null && !this.channel.closing()) {
      try {
        // bug #39827 - close the channel without disconnecting it first
        this.channel.shutdown();
      }
      catch (DistributedSystemDisconnectedException se) {
        // ignore - we're already shutting down
      }
    }
    if (this.directChannel != null) {
      this.directChannel.disconnect(e);
    }
    
//    if (logWriter instanceof ManagerLogWriter) {
//      // avoid trying to send any alerts
//      ((ManagerLogWriter)logWriter).shuttingDown(); // TODO:LOG:ALERTS: can we do something here to avoid sending alerts?
//    }
    // first shut down communication so we don't do any more harm to other
    // members
    JGroupMembershipManager.this.emergencyClose();
    // we have to clear the view before notifying the membership listener,
    // so that it won't try sending disconnect messages to members that
    // aren't there.  Otherwise, it sends the disconnect messages to other
    // members, they ignore the "surprise" connections, and we hang.
    //JGroupMembershipManager.this.clearView();
    if (e != null) {
      try {
        if (JGroupMembershipManager.this.membershipTestHooks != null) {
          List l = JGroupMembershipManager.this.membershipTestHooks;
          for (Iterator it=l.iterator(); it.hasNext(); ) {
            MembershipTestHook dml = (MembershipTestHook)it.next();
            dml.beforeMembershipFailure(reason, e);
          }
        }
        listener.membershipFailure(reason, e);
        if (JGroupMembershipManager.this.membershipTestHooks != null) {
          List l = JGroupMembershipManager.this.membershipTestHooks;
          for (Iterator it=l.iterator(); it.hasNext(); ) {
            MembershipTestHook dml = (MembershipTestHook)it.next();
            dml.afterMembershipFailure(reason, e);
          }
        }
      }
      catch (RuntimeException re) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.JGroupMembershipManager_EXCEPTION_CAUGHT_WHILE_SHUTTING_DOWN), re);
      }
    }
  }
  
  /** generate XML for the cache before shutting down due to forced disconnect */
  public void saveCacheXmlForReconnect(boolean sharedConfigEnabled) {
    // first save the current cache description so reconnect can rebuild the cache
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && (cache instanceof Cache)) {
      if (!Boolean.getBoolean("gemfire.autoReconnect-useCacheXMLFile")
          && !cache.isSqlfSystem() && !sharedConfigEnabled) {
        try {
          logger.info("generating XML to rebuild the cache after reconnect completes");
          StringPrintWriter pw = new StringPrintWriter(); 
          CacheXmlGenerator.generate((Cache)cache, pw, true, false);
          String cacheXML = pw.toString();
          cache.getCacheConfig().setCacheXMLDescription(cacheXML);
          logger.info("XML generation completed: {}", cacheXML);
        } catch (CancelException e) {
          logger.info(LocalizedMessage.create(LocalizedStrings.JGroupMembershipManager_PROBLEM_GENERATING_CACHE_XML), e);
        }
      } else if (sharedConfigEnabled && !cache.getCacheServers().isEmpty()) {
        // we need to retain a cache-server description if this JVM was started by gfsh
        List<CacheServerCreation> list = new ArrayList<CacheServerCreation>(cache.getCacheServers().size());
        for (Iterator it = cache.getCacheServers().iterator(); it.hasNext(); ) {
          CacheServer cs = (CacheServer)it.next();
          CacheServerCreation bsc = new CacheServerCreation(cache, cs);
          list.add(bsc);
        }
        cache.getCacheConfig().setCacheServerCreation(list);
        logger.info("CacheServer configuration saved");
      }
    }
  }

  public boolean requestMemberRemoval(DistributedMember mbr, String reason) {
    if (mbr.equals(this.myMemberId)) {
      return false;
    }
    if (gmsProtocol == null) {
      return false; // no GMS = no communication
    }
    IpAddress jcoord = (IpAddress)gmsProtocol.determineCoordinator();
    if (jcoord == null) {
      logger.fatal(LocalizedMessage.create(
        LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_REQUEST_INITIATED_TO_REMOVE_MEMBER_0_BUT_THERE_IS_NO_GROUP_COORDINATOR, mbr));
      return false;
    }
    else {
      logger.fatal(LocalizedMessage.create(
        LocalizedStrings.JGroupMembershipManager_MEMBERSHIP_REQUESTING_REMOVAL_OF_0_REASON_1,
        new Object[] {mbr, reason}));
      Message msg = new Message();
      msg.setDest(jcoord);
      msg.isHighPriority = true;
      InternalDistributedMember imbr = (InternalDistributedMember)mbr;
      msg.putHeader(GMS.name, new GMS.GmsHeader(
          GMS.GmsHeader.REMOVE_REQ, ((JGroupMember)imbr.getNetMember()).getAddress(), reason));
      Exception problem = null;
      try {
        channel.send(msg);
      }
      catch (ChannelClosedException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ForcedDisconnectException) {
          problem = (Exception) cause;
        } else {
          problem = e;
        }
      }
      catch (ChannelNotConnectedException e) {
        problem = e;
      }
      catch (IllegalArgumentException e) {
        problem = e;
      }
      if (problem != null) {
        if (this.shutdownCause != null) {
          Throwable cause = this.shutdownCause;
          // If ForcedDisconnectException occurred then report it as actual
          // problem.
          if (cause instanceof ForcedDisconnectException) {
            problem = (Exception) cause;
          } else {
            Throwable ne = problem;
            while (ne.getCause() != null) {
              ne = ne.getCause();
            }
            try {
              ne.initCause(this.shutdownCause);
            }
            catch (IllegalArgumentException selfCausation) {
              // fix for bug 38895 - the cause is already in place
            }
          }
        }
        if (!dconfig.getDisableAutoReconnect()) {
          saveCacheXmlForReconnect(dconfig.getUseSharedConfiguration());
        }
        listener.membershipFailure("Channel closed", problem);
        throw new DistributedSystemDisconnectedException("Channel closed", problem);
      }
      return true;
    }
  }
  
  public void suspectMembers(Set members, String reason) {
    for (Iterator it=members.iterator(); it.hasNext(); ) {
      suspectMember((DistributedMember)it.next(), reason);
    }
  }
  
  public void suspectMember(DistributedMember mbr, String reason) {
    if (mbr != null) {
      Address jgmbr = ((JGroupMember)((InternalDistributedMember)mbr)
          .getNetMember()).getAddress();
      this.fdSockProtocol.suspect(jgmbr, false, reason);
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Attempt to suspect member with null ID detected.  This usually means that the target has left membership during startup.");
      }
    }
  }

  public void suspectMember(Stub mbr, String reason) {
    InternalDistributedMember idm = getMemberForStub(mbr, false);
    if (idm != null) {
      Address jgmbr = ((JGroupMember)idm.getNetMember()).getAddress();
      this.fdSockProtocol.suspect(jgmbr, false, reason);
    }
  }
  
  /* like memberExists() this checks to see if the given ID is in the current
   * membership view.  If it is in the view though we try to connect to its
   * failure-detection port to see if it's still around.  If we can't then
   * suspect processing is initiated on the member with the given reason string.
   * @param mbr the member to verify
   * @param reason why the check is being done (must not be blank/null)
   * @return true if the member checks out
   */
  public boolean verifyMember(DistributedMember mbr, String reason) {
    if (mbr != null && memberExists((InternalDistributedMember)mbr)) {
      Address jgmbr = ((JGroupMember)((InternalDistributedMember)mbr)
          .getNetMember()).getAddress();
      return this.fdSockProtocol.checkSuspect(jgmbr, reason);
    }
    return false;
  }

  /**
   * Perform the grossness associated with sending a message over
   * a DirectChannel
   * 
   * @param destinations the list of destinations
   * @param content the message
   * @param theStats the statistics object to update
   * @return all recipients who did not receive the message (null if
   * all received it)
   * @throws NotSerializableException if the message is not serializable
   */
  private Set directChannelSend(InternalDistributedMember[] destinations,
      DistributionMessage content,
      com.gemstone.gemfire.distributed.internal.DistributionStats theStats)
      throws NotSerializableException
  {
    boolean allDestinations;
    InternalDistributedMember[] keys;
    if (content.forAll()) {
      allDestinations = true;
      synchronized (latestViewLock) {
        Set keySet = memberToStubMap.keySet();
        keys = new InternalDistributedMember[keySet.size()];
        keys = (InternalDistributedMember[])keySet.toArray(keys);
      }
    }
    else {
      allDestinations = false;
      keys = destinations;
    }

    int sentBytes = 0;
    try {
      sentBytes = directChannel.send(this, keys, content,
          this.dconfig.getAckWaitThreshold(),
          this.dconfig.getAckSevereAlertThreshold());
                                     
      if (theStats != null)
        theStats.incSentBytes(sentBytes);
    }
    catch (DistributedSystemDisconnectedException ex) {
      if (this.shutdownCause != null) {
        throw new DistributedSystemDisconnectedException("DistributedSystem is shutting down", this.shutdownCause);
      } else {
        throw ex; // see bug 41416
      }
    }
    catch (ConnectExceptions ex) {
      if (allDestinations)
        return null;
      
      List members = ex.getMembers(); // We need to return this list of failures
      
      // SANITY CHECK:  If we fail to send a message to an existing member 
      // of the view, we have a serious error (bug36202).
      Vector view = getView(); // grab a recent view, excluding shunned members
      
      // Iterate through members and causes in tandem :-(
      Iterator it_mem = members.iterator();
      Iterator it_causes = ex.getCauses().iterator();
      while (it_mem.hasNext()) {
        InternalDistributedMember member = (InternalDistributedMember)it_mem.next();
        Throwable th = (Throwable)it_causes.next();
        
        if (!view.contains(member)) {
          continue;
        }
        logger.fatal(LocalizedMessage.create(
            LocalizedStrings.JGroupMembershipManager_FAILED_TO_SEND_MESSAGE_0_TO_MEMBER_1_VIEW_2,
            new Object[] {content, member, view}), th);
//        Assert.assertTrue(false, "messaging contract failure");
      }
      return new HashSet(members);
    } // catch ConnectionExceptions
    catch (ToDataException e) {
      throw e; // error in user data
    }
    catch (CancelException e) {
      // not interesting, we're just shutting down
      throw e;
    }
    catch (IOException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: directChannelSend caught exception: {}", e.getMessage(), e);
      }
      if (e instanceof NotSerializableException) {
        throw (NotSerializableException)e;
      }
    }
    catch (RuntimeException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: directChannelSend caught exception: {}", e.getMessage(), e);
      }
      throw e;
    }
    catch (Error e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: directChannelSend caught exception: {}", e.getMessage(), e);
      }
      throw e;
    }
    return null;
  }

  /*
   * (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.membership.MembershipManager#isConnected()
   */
  public boolean isConnected() {
    if (channel != null && channel.isConnected()) {
      return true;
    }
    if (!channelInitialized) {
      return true; // startup noise
    }
    return false;
  }
  
  /**
   * Returns true if the distributed system is in the process of auto-reconnecting.
   * Otherwise returns false.
   */
  public boolean isReconnectingDS() {
    if (this.hasConnected) {
      // if the jgroups channel has been connected then we aren't in the
      // middle of a reconnect attempt in this instance of the distributed system
      return false;
    } else {
      return this.wasReconnectingSystem;
    }
  }
  
  /**
   * A quorum checker is used during reconnect to perform quorum
   * probes.  It is made available here for the UDP protocol to
   * hand off ping-pong responses to the checker.
   */
  public QuorumCheckerImpl getQuorumCheckerImpl() {
    return this.quorumChecker;
  }
  
  /**
   * During jgroups connect the UDP protocol will invoke
   * this method to find the DatagramSocket it should use instead of
   * creating a new one.
   */
  public DatagramSocket getMembershipSocketForUDP() {
    return this.oldDSMembershipSocket;
  }
  
  @Override
  public QuorumChecker getQuorumChecker() {
    if ( ! (this.shutdownCause instanceof ForcedDisconnectException) ) {
      return null;
    }
    if (gmsProtocol == null || udpProtocol == null) {
      return null;
    }
    if (this.quorumChecker != null) {
      return this.quorumChecker;
    }
    QuorumCheckerImpl impl = new QuorumCheckerImpl(
        gmsProtocol.getLastView(), gmsProtocol.getPartitionThreshold(), udpProtocol.getMembershipSocket());
    impl.initialize();
    this.quorumChecker = impl;
    return impl;
  }
  
  @Override
  public void releaseQuorumChecker(QuorumChecker checker) {
    ((QuorumCheckerImpl)checker).teardown();
    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    if (system == null || !system.isConnected()) {
      DatagramSocket sock = (DatagramSocket)checker.getMembershipInfo();
      if (sock != null  &&  !sock.isClosed()) {
        sock.close();
      }
    }
  }
  
  public Set send(InternalDistributedMember[] destinations,
      DistributionMessage msg,
      com.gemstone.gemfire.distributed.internal.DistributionStats theStats)
      throws NotSerializableException
  {
    Set result = null;
    boolean allDestinations = msg.forAll();
    
    // don't allow messages to be sent if we're not connected to the
    // jgroups channel
    if (!channel.isConnected()) {
      // bug34917 - if the channel was previously initialized, it must have got
      // into trouble and we're exiting
      if (channelInitialized) {
        Exception cause = this.shutdownCause;
        if (cause == null && channel.exitEvent != null && 
            (channel.exitEvent.getArg() instanceof Exception)) {
          cause = (Exception)channel.exitEvent.getArg();
        }
        throw new DistributedSystemDisconnectedException("Distributed System is shutting down", cause);
      }
      
      // If we get here, we are starting up, so just report a failure.
      if (allDestinations)
        return null;
      else {
        result = new HashSet();
        for (int i = 0; i < destinations.length; i ++)
          result.add(destinations[i]);
        return result;
      }
    }

    // Handle trivial cases
    if (destinations == null) {
      if (logger.isTraceEnabled())
        logger.trace("Membership: Message send: returning early because null set passed in: '{}'", msg);
      return null; // trivially: all recipients received the message
    }
    if (destinations.length == 0) {
      if (logger.isTraceEnabled())
        logger.trace("Membership: Message send: returning early because empty destination list passed in: '{}'", msg);
      return null; // trivially: all recipients received the message
    }

    msg.setSender(myMemberId);
    
    msg.setBreadcrumbsInSender();
    Breadcrumbs.setProblem(null);
    
    boolean useMcast = false;
    if (isMcastEnabled) {
      useMcast = !disableMulticastForRollingUpgrade && (msg.getMulticast() || allDestinations);
//      if (multicastTest && !useMcast) {
//        useMcast = (msg instanceof DistributedCacheOperation.CacheOperationMessage);
//      }
    }
    
    // some messages are sent to everyone, so we use UDP to avoid having to
    // obtain tcp/ip connections to them
    boolean sendViaJGroups = isForceUDPCommunications(); // enable when bug #46438 is fixed: || msg.sendViaJGroups();
    
    // Wait for sends to be unsuspended
    if (sendSuspended) {
      synchronized(sendSuspendMutex) {
        while (sendSuspended) {
          conduit.getCancelCriterion().checkCancelInProgress(null);
          boolean interrupted = Thread.interrupted();
          try {
            sendSuspendMutex.wait(10);
            conduit.getCancelCriterion().checkCancelInProgress(null);
          }
          catch(InterruptedException e) {
            interrupted = true;
            conduit.getCancelCriterion().checkCancelInProgress(e);
          }
          finally {
            if (interrupted)
              Thread.currentThread().interrupt();
          }
        } // while
      } // synchronized
    } // sendsSuspended
    
    
    // Handle TCPConduit case
    if (!useMcast && !dconfig.getDisableTcp() && !sendViaJGroups) {
      result = directChannelSend(destinations, msg, theStats);
      // If the message was a broadcast, don't enumerate failures.
      if (allDestinations)
        return null;
      else {
        return result;
      }
    }
    // Otherwise, JGroups is going to handle this message
    
    Address local = channel.getLocalAddress();
    
    if (useMcast) {
      if (logger.isTraceEnabled(LogMarker.DM))
        logger.trace(LogMarker.DM, "Membership: sending < {} > via multicast", msg);

      // if there are unack'd unicast messages, we need to wait for
      // them to be processed.
      if (!DISABLE_UCAST_FLUSH && ucastProtocol != null && ucastProtocol.getNumberOfUnackedMessages() > 0) {
         flushUnicast();
      }

      Exception problem = null;
      try {
        long startSer = theStats.startMsgSerialization();
        Vector<IpAddress> mbrs = this.channel.getView().getMembers();
        Message jmsg = createJGMessage(msg, local, Version.CURRENT_ORDINAL);
        theStats.endMsgSerialization(startSer);
        theStats.incSentBytes(jmsg.getLength());
        channel.send(jmsg);
      }
      catch (ChannelClosedException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ForcedDisconnectException) {
          problem = (Exception) cause;
        } else {
          problem = e;
        }
      }
      catch (ChannelNotConnectedException e) {
        problem = e;
      }
      catch (IllegalArgumentException e) {
        problem = e;
      }
      if (problem != null) {
        if (this.shutdownCause != null) {
          Throwable cause = this.shutdownCause;
          // If ForcedDisconnectException occurred then report it as actual
          // problem.
          if (cause instanceof ForcedDisconnectException) {
            problem = (Exception) cause;
          } else {
            Throwable ne = problem;
            while (ne.getCause() != null) {
              ne = ne.getCause();
            }
            ne.initCause(this.shutdownCause);
          }
        }
        final String channelClosed = LocalizedStrings.JGroupMembershipManager_CHANNEL_CLOSED.toLocalizedString();
        listener.membershipFailure(channelClosed, problem);
        throw new DistributedSystemDisconnectedException(channelClosed, problem);
      }
    } // useMcast
    else { // ! useMcast
      int len = destinations.length;
        List<JGroupMember> calculatedMembers; // explicit list of members
        int calculatedLen; // == calculatedMembers.len
        if (len == 1 && destinations[0] == DistributionMessage.ALL_RECIPIENTS) { // send to all
          // Grab a copy of the current membership
          Vector v = getView();
          
          // Construct the list
          calculatedLen = v.size();
          calculatedMembers = new LinkedList<JGroupMember>();
          for (int i = 0; i < calculatedLen; i ++) {
            InternalDistributedMember m = (InternalDistributedMember)v.elementAt(i);
            calculatedMembers.add((JGroupMember)m.getNetMember());
          }
        } // send to all
        else { // send to explicit list
          calculatedLen = len;
          calculatedMembers = new LinkedList<JGroupMember>();
          for (int i = 0; i < calculatedLen; i ++) {
            calculatedMembers.add((JGroupMember)destinations[i].getNetMember());
          }
        } // send to explicit list
        Int2ObjectOpenHashMap messages = new Int2ObjectOpenHashMap();
        long startSer = theStats.startMsgSerialization();
        boolean firstMessage = true;
        for (Iterator it=calculatedMembers.iterator(); it.hasNext(); ) {
          JGroupMember mbr = (JGroupMember)it.next();
          short version = mbr.getAddress().getVersionOrdinal();
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
        for (Iterator<JGroupMember> it=calculatedMembers.iterator();
          it.hasNext(); i++) { // send individually
          JGroupMember mbr = it.next();
          IpAddress to = mbr.getAddress();
          short version = to.getVersionOrdinal();
          Message jmsg = (Message)messages.get(version);
          if (logger.isTraceEnabled(LogMarker.DM))
            logger.trace(LogMarker.DM, "Membership: Sending '{}' to '{}' via udp unicast", msg, mbr);
          Exception problem = null;
          try {
            Message tmp = (i < (calculatedLen-1)) ? jmsg.copy(true) : jmsg;
            tmp.setDest(to);
            channel.send(tmp);
          }
          catch (ChannelClosedException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ForcedDisconnectException) {
              problem = (Exception) cause;
            } else {
              problem = e;
            }
          }
          catch (ChannelNotConnectedException e) {
            problem = e;
          }
          catch (IllegalArgumentException e) {
            problem = e;
          }
          if (problem != null) {
            if (this.shutdownCause != null) {
              Throwable cause = this.shutdownCause;
              // If ForcedDisconnectException occurred then report it as actual
              // problem.
              if (cause instanceof ForcedDisconnectException) {
                problem = (Exception) cause;
              } else {
                Throwable ne = problem;
                while (ne.getCause() != null) {
                  ne = ne.getCause();
                }
                ne.initCause(this.shutdownCause);
              }
            }
            listener.membershipFailure("Channel closed", problem);
            throw new DistributedSystemDisconnectedException("Channel closed", problem);
          }
        } // send individually
    } // !useMcast

    // The contract is that every destination enumerated in the
    // message should have received the message.  If one left
    // (i.e., left the view), we signal it here.
    if (allDestinations)
      return null;
    result = new HashSet();
    Vector newView = getView();
    for (int i = 0; i < destinations.length; i ++) {
      InternalDistributedMember d = destinations[i];
      if (!newView.contains(d)) {
        result.add(d);
      }
    }
    if (result.size() == 0)
      return null;
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
  private Message createJGMessage(DistributionMessage gfmsg, Address src, short version) {
    if(gfmsg instanceof DirectReplyMessage) {
      ((DirectReplyMessage) gfmsg).registerProcessor();
    }
    Message msg = new Message();
    msg.setDest(null);
    msg.setSrc(src);
    msg.setVersion(version);
    //log.info("Creating message with payload " + gfmsg);
    if (gfmsg instanceof com.gemstone.gemfire.internal.cache.DistributedCacheOperation.CacheOperationMessage) {
      com.gemstone.gemfire.internal.cache.DistributedCacheOperation.CacheOperationMessage cmsg =
          (com.gemstone.gemfire.internal.cache.DistributedCacheOperation.CacheOperationMessage)gfmsg;
      msg.isCacheOperation = true;
      if (cmsg.getProcessorId() == 0 && cmsg.getMulticast()) {
        msg.bundleable = true;
      }
    }
    msg.isHighPriority = (gfmsg.getProcessorType() == DistributionManager.HIGH_PRIORITY_EXECUTOR
        || gfmsg instanceof HighPriorityDistributionMessage);
    if (msg.isHighPriority) {
      msg.bundleable = false;
    }
    try {
      HeapDataOutputStream out_stream =
        new HeapDataOutputStream(Version.fromOrdinalOrCurrent(version)); // GemStoneAddition
        DataSerializer.writeObject(gfmsg, out_stream); // GemStoneAddition
        msg.setBuffer(out_stream.toByteArray());
    }
    catch(IOException ex) {
        // GemStoneAddition - we need the cause to figure out what went wrong
        IllegalArgumentException ia = new
          IllegalArgumentException("Error serializing message");
        ia.initCause(ex);
        throw ia;
        //throw new IllegalArgumentException(ex.toString());
    }
    return msg;
  }

  /**
   * @throws ConnectionException if the conduit has stopped
   */
  public void reset() throws DistributionException
  {
    if (conduit != null) {
      try {
        conduit.restart();
      } catch (ConnectionException e) {
        throw new DistributionException(LocalizedStrings.JGroupMembershipManager_UNABLE_TO_RESTART_CONDUIT.toLocalizedString(), e);
      }
    }
  }

  // MembershipManager method
  @Override
  public void forceUDPMessagingForCurrentThread() {
    forceUseJGroups.set(Boolean.TRUE);
  }
  
  // MembershipManager method
  @Override
  public void releaseUDPMessagingForCurrentThread() {
    forceUseJGroups.set(null);
  }
  
  private boolean isForceUDPCommunications() {
    Boolean forced = forceUseJGroups.get();
    return forced == Boolean.TRUE;
  }

  /**
   * Establish a sleep period to be instituted after the JChannel finishes connecting.
   * This is used by the channel itself when it detects a concurrent startup
   * situation and has elected this process as the group coordinator.  This allows
   * other processes to merge into this process's view (or vice versa) before
   * cache processing commences
   */
  public void establishChannelPause(long period) {
    channelPause = period;
  }

  /**
   * Get or create stub for given member
   */
  public Stub getStubForMember(InternalDistributedMember m)
  {
    if (shutdownInProgress) {
      throw new DistributedSystemDisconnectedException(LocalizedStrings.JGroupMembershipManager_DISTRIBUTEDSYSTEM_IS_SHUTTING_DOWN.toLocalizedString(), this.shutdownCause);
    }
    // Bogus stub object if direct channels not being used
    if (conduit == null)
      return new Stub(m.getIpAddress(), m.getPort(), m.getVmViewId());
    
    // Return existing one if it is already in place
    Stub result;
    result = (Stub)memberToStubMap.get(m);
    if (result != null)
      return result;

    synchronized (latestViewLock) {
      // Do all of this work in a critical region to prevent
      // members from slipping in during shutdown
      if (shutdownInProgress())
        return null; // don't try to create a stub during shutdown
      if (isShunned(m))
        return null; // don't let zombies come back to life
      
      // OK, create one.  Update the table to reflect the creation.
      result = directChannel.createConduitStub(m);
      addChannel(m, result);
    }
   return result;
  }

  public InternalDistributedMember getMemberForStub(Stub s, boolean validated)
  {
    synchronized (latestViewLock) {
      if (shutdownInProgress) {
        throw new DistributedSystemDisconnectedException(LocalizedStrings.JGroupMembershipManager_DISTRIBUTEDSYSTEM_IS_SHUTTING_DOWN.toLocalizedString(), this.shutdownCause);
      }
      InternalDistributedMember result = (InternalDistributedMember)
          stubToMemberMap.get(s);
      if (result != null) {
        if (validated && !this.latestView.contains(result)) {
          // Do not return this member unless it is in the current view.
          if (!surpriseMembers.containsKey(result)) {
            // if not a surprise member, this stub is lingering and should be removed
            stubToMemberMap.remove(s);
            memberToStubMap.remove(result);
          }
          result = null;
          // fall through to see if there is a newer member using the same direct port
        }
      }
      if (result == null) {
        // it may have not been added to the stub->idm map yet, so check the current view
        for (Iterator it=this.latestView.iterator(); it.hasNext(); ) {
          InternalDistributedMember idm = (InternalDistributedMember)it.next();
          if (idm.getIpAddress().equals(s.getInetAddress())
              && idm.getDirectChannelPort() == s.getPort()) {
            addChannel(idm, s);
            return idm;
          }
        }
      }
      return result;
    }
  }

  public void setShutdown()
  {
    // shutdown state needs to be set atomically between this
    // class and the direct channel.  Don't allow new members
    // to slip in.
    synchronized (latestViewLock) {
      shutdownInProgress = true;
    }
  }

  public boolean shutdownInProgress() {
    // Impossible condition (bug36329): make sure that we check DM's
    // view of shutdown here
    return shutdownInProgress || listener.getDM().shutdownInProgress();
  }
  
  /**
   * Add a mapping from the given member to the given stub. Must be
   * synchronized on {@link #latestViewLock} by caller.
   * 
   * @param member
   * @param theChannel
   */
  protected void addChannel(InternalDistributedMember member, Stub theChannel)
  {
    if (theChannel != null) {
      // Don't overwrite existing stub information with a null
      this.memberToStubMap.put(member, theChannel);

      // Can't create reverse mapping if the stub is null
      this.stubToMemberMap.put(theChannel, member);
    }
  }


  /**
   * Attempt to ensure that the jgroups unicast channel has no outstanding unack'd messages.
   */
  public void flushUnicast() {
    if (ucastProtocol != null) {
      long flushStart = 0;
      if (DistributionStats.enableClockStats)
        flushStart = stats.startUcastFlush();
      try {
        suspendSends();
        long start = System.currentTimeMillis();
        while (ucastProtocol.getNumberOfUnackedMessages() > 0) {
          try {
            Thread.sleep(3);
          }
          catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
          // don't wait longer than the member timeout
          if (System.currentTimeMillis() - start > dconfig.getMemberTimeout()) {
            break;
          }
        }
      }
      finally {
        if (DistributionStats.enableClockStats)
          stats.endUcastFlush(flushStart);
        resumeSends();
      }
    }
  }
  
  /**
   * Suspend outgoing messaging
   */
  private void suspendSends() {
    synchronized(sendSuspendMutex) {
      sendSuspended = true;
    }
  }
  
  /**
   * Resume outgoing messaging
   */
  private void resumeSends() {
    synchronized(sendSuspendMutex) {
      sendSuspended = false;
      sendSuspendMutex.notifyAll();
    }
  }

  /**
   * Clean up and create consistent new view with member removed.
   * No uplevel events are generated.
   * 
   * Must be called with the {@link #latestViewLock} held.
   */
  protected void destroyMember(final InternalDistributedMember member,
      boolean crashed, final String reason) {
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS))
      logger.trace(LogMarker.DISTRIBUTION_VIEWS, "Membership: destroying < {} >", member);
    
    // Clean up the maps
    Stub theChannel = (Stub)memberToStubMap.remove(member);
    if (theChannel != null) {
      this.stubToMemberMap.remove(theChannel);
    }
    
    this.ipAddrToMemberMap.remove(
        new IpAddress(member.getIpAddress(), member.getPort()));
    
    // Make sure it is removed from the view
    synchronized (this.latestViewLock) {
      if (latestView.contains(member)) {
        NetView newView = new NetView(latestView, latestView.getViewNumber());
        newView.remove(member);
        latestView = newView;
      }
    }
    
    surpriseMembers.remove(member);
    
    // Trickiness: there is a minor recursion
    // with addShunnedMembers, since it will
    // attempt to destroy really really old members.  Performing the check
    // here breaks the recursion.
    if (!isShunned(member)) {
      addShunnedMember(member);
    }

    final DirectChannel dc = directChannel;
    if (dc != null) {
//      if (crashed) {
//        dc.closeEndpoint(member, reason);
//      }
//      else
      // Bug 37944: make sure this is always done in a separate thread,
      // so that shutdown conditions don't wedge the view lock
      { // fix for bug 34010
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              Thread.sleep(
                  Integer.getInteger("p2p.disconnectDelay", 3000).intValue());
            }
            catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              // Keep going, try to close the endpoint.
            }
            if (logger.isDebugEnabled())
              logger.debug("Membership: closing connections for departed member {}", member);
            // close connections, but don't do membership notification since it's already been done
            dc.closeEndpoint(member, reason, false); 
          }
        };
        t.setDaemon(true);
        t.setName("disconnect thread for " + member);
        t.start();
      } // fix for bug 34010
    }
  }
  
  public Stub getDirectChannel()
  {
    synchronized (latestViewLock) {
      return (Stub)memberToStubMap.get(myMemberId);
    }
  }

  /** replace strings in a jgroups properties file.  This is used during channel creation to
      insert parameters from gemfire.properties into the jgroups stack configuration */
  private static String replaceStrings(String properties, String property, String value)
  {
    StringBuffer sb = new StringBuffer();
    int start = 0;
    int index = properties.indexOf(property);
    while (index != -1) {
      sb.append(properties.substring(start, index));
      sb.append(value);

      start = index + property.length();
      index = properties.indexOf(property, start);
    }
    sb.append(properties.substring(start));
    return sb.toString();
  }

  /**
   * Indicate whether the given member is in the zombie list (dead or dying)
   * @param m the member in question
   * 
   * This also checks the time the given member was shunned, and
   * has the side effect of removing the member from the
   * list if it was shunned too far in the past.
   * 
   * @guarded.By latestViewLock
   * @return true if the given member is a zombie
   */
  protected boolean isShunned(DistributedMember m) {
    synchronized (latestViewLock) {
      if (!shunnedMembers.containsKey(m))
        return false;
      
      // Make sure that the entry isn't stale...
      long shunTime = ((Long)shunnedMembers.get(m)).longValue();
      long now = System.currentTimeMillis();
      if (shunTime + SHUNNED_SUNSET * 1000 > now)
        return true;
      
      // Oh, it _is_ stale.  Remove it while we're here.
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS)) {
        logger.debug("Membership: no longer shunning <  {} >", m);
      }
      endShun(m);
      return false;
    } // sync
  }

//  public boolean isShunnedMember(IpAddress addr) {
//    synchronized(latestViewLock) {
//      InternalDistributedMember idm = getMemberFromIpAddress(addr, false);
//      if (idm == null) {
//        return false; // don't know about this member, so don't shun it
//      }
//      return isShunned(idm);
//    }
//  }

  public boolean isShunnedMemberNoSync(IpAddress addr) {
    InternalDistributedMember mbr = (InternalDistributedMember)ipAddrToMemberMap.get(addr);
    if (mbr == null) {
      return false;
    }
    return shunnedMembers.containsKey(mbr);
  }

  /**
   * Indicate whether the given member is in the surprise member list
   * <P>
   * Unlike isShunned, this method will not cause expiry of a surprise member.
   * That must be done during view processing.
   * <p>
   * Like isShunned, this method holds the view lock while executing
   * 
   * @guarded.By latestViewLock
   * @param m the member in question
   * @return true if the given member is a surprise member
   */
  public boolean isSurpriseMember(DistributedMember m) {
    synchronized (latestViewLock) {
      if (surpriseMembers.containsKey(m)) {
        long birthTime = ((Long)surpriseMembers.get(m)).longValue();
        long now = System.currentTimeMillis();
        return (birthTime >= (now - this.surpriseMemberTimeout));
      }
      return false;
    } // sync
  }
  
  /**
   * for testing we need to be able to inject surprise members into
   * the view to ensure that sunsetting works properly
   * @param m the member ID to add
   * @param birthTime the millisecond clock time that the member was first seen
   */
  protected void addSurpriseMemberForTesting(DistributedMember m, long birthTime) {
    if (logger.isDebugEnabled()) {
      logger.debug("test hook is adding surprise member {} birthTime={}", m, birthTime);
    }
    synchronized(latestViewLock) {
      surpriseMembers.put((InternalDistributedMember)m, Long.valueOf(birthTime));
    }
  }
  
  /**
   * returns the surpriseMemberTimeout interval, in milliseconds
   */
  public int getSurpriseMemberTimeout() {
    return this.surpriseMemberTimeout;
  }
  
  /**
   * returns the shunned member shunset interval, in milliseconds
   */
  public int getShunnedMemberTimeout() {
    return SHUNNED_SUNSET * 1000;
  }
  

  private boolean endShun(DistributedMember m) {
    boolean wasShunned = (shunnedMembers.remove(m) != null);
    shunnedAndWarnedMembers.remove(m);
    return wasShunned;
  }
  
 /**
   * Add the given member to the shunned list.  Also, purge any shunned
   * members that are really really old.
   * <p>
   * Must be called with {@link #latestViewLock} held and
   * the view stable.
   * 
   * @param m the member to add
   */
  protected void addShunnedMember(InternalDistributedMember m) {
    long deathTime = System.currentTimeMillis() - SHUNNED_SUNSET * 1000;
    
    surpriseMembers.remove(m); // for safety

    // Update the shunned set.
    if (!isShunned(m)) {
      shunnedMembers.put(m, Long.valueOf(System.currentTimeMillis()));
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS))
        logger.trace(LogMarker.DISTRIBUTION_VIEWS, "Membership: added shunned member < {} >", m);
    }

    // Remove really really old shunned members.
    // First, make a copy of the old set.  New arrivals _a priori_ don't matter,
    // and we're going to be updating the list so we don't want to disturb
    // the iterator.
    Set oldMembers = new HashSet(shunnedMembers.entrySet());
    HashSet removedMembers = new HashSet();
    
    Iterator it = oldMembers.iterator();
    while (it.hasNext()) {
      Map.Entry e = (Map.Entry)it.next();
      
      // Key is the member.  Value is the time to remove it.
      long ll = ((Long)e.getValue()).longValue(); 
      if (ll >= deathTime)
        continue; // too new.
      InternalDistributedMember mm = (InternalDistributedMember)e.getKey();

      if (latestView.contains(mm)) {
        // Fault tolerance: a shunned member can conceivably linger but never 
        // disconnect.
        //
        // We may not delete it at the time that we shun it because the view 
        // isn't necessarily stable.  (Note that a well-behaved cache member
        // will depart on its own accord, but we force the issue here.)
        destroyMember(mm, true, "shunned but never disconnected");
      }
      if (logger.isDebugEnabled())
        logger.debug("Membership: finally removed shunned member entry <{}>", mm);
      removedMembers.add(mm);
    } // while
    
    // Now remove these folk from the list...
    synchronized (latestViewLock) {
      it = removedMembers.iterator();
      while (it.hasNext()) {
        InternalDistributedMember idm = (InternalDistributedMember)it.next();
        endShun(idm);
        ipAddrToMemberMap.remove(new IpAddress(idm.getIpAddress(), idm.getPort()));
      }
    }
  }

  /**
   * Retrieve thread-local data for transport to another thread in hydra
   */
  public Object getThreadLocalData() {
    Map result = new HashMap();
//    if (dirackProtocol != null) {
//      Object td = dirackProtocol.getThreadDataIfPresent();
//      if (td != null) {
//        result.put("DirAck", td);
//      }
//    }
    return result;
  }
  
  /**
   * Set thread-local data for hydra
   */
  public void setThreadLocalData(Object data) {
    Map dataMap = (Map)data;
    Object td = dataMap.get("DirAck");
//    if (td != null && dirackProtocol != null) {
//      dirackProtocol.setThreadData(td);
//    }
  }
  
  /**
   * for testing verification purposes, this return the port for the
   * direct channel, or zero if there is no direct
   * channel
   */
  public int getDirectChannelPort() {
    return directChannel == null? 0 : directChannel.getPort();
  }
  
  public int getSerialQueueThrottleTime(Address sender) {
    IpAddress senderAddr = (IpAddress) sender;
    //if (!serialQueueInitialized) { // no need to synchronize - queue is invariant
    //  serialQueueInitialized = true;
    InternalDistributedMember member = getMemberFromIpAddress(senderAddr, true);
    ThrottlingMemLinkedQueueWithDMStats serialQueue = listener.getDM()
        .getSerialQueue(member);
    //}
    // return serialQueue != null && serialQueue.wouldBlock();
    if (serialQueue == null)
      return 0;
    
    return serialQueue.getThrottleTime();
  }
  
  /**
   * return an InternalDistributedMember representing the given jgroups address
   * @param sender
   * @param createIfAbsent
   * @return the IDM for the given jgroups address
   */
  public InternalDistributedMember getMemberFromIpAddress(IpAddress sender,
      boolean createIfAbsent) {
    synchronized(latestViewLock) {
      InternalDistributedMember mbr = (InternalDistributedMember)ipAddrToMemberMap.get(sender);
      if (mbr == null && createIfAbsent) {
        JGroupMember jgm = new JGroupMember(sender);
        mbr = new InternalDistributedMember(jgm);
        // if a fully formed address, retain it in the map for future use
        if (sender.getVmKind() != 0) {
          ipAddrToMemberMap.put(sender, mbr);
        }
      }
      return mbr;
    }
  }
  
  /* non-thread-owned serial channels and high priority channels are not
   * included
   */
  public HashMap getChannelStates(DistributedMember member, boolean includeMulticast) {
    HashMap result = new HashMap();
    Stub stub = (Stub)memberToStubMap.get(member);
    DirectChannel dc = directChannel;
    if (stub != null && dc != null) {
      dc.getChannelStates(stub, result);
    }
    if (includeMulticast) {
      result.put("JGroups.MCast", Long.valueOf(channel.getMulticastState()));
    }
    return result;
  }

  public void waitForChannelState(DistributedMember otherMember, HashMap channelState)
    throws InterruptedException
  {
    if (Thread.interrupted()) throw new InterruptedException();
    DirectChannel dc = directChannel;
    Long mcastState = (Long)channelState.remove("JGroups.MCast");
    Stub stub;
    synchronized (latestViewLock) {
      stub = (Stub)memberToStubMap.get(otherMember);
    }
    if (dc != null && stub != null) {
      dc.waitForChannelState(stub, channelState);
    }
    if (mcastState != null) {
      InternalDistributedMember idm = (InternalDistributedMember)otherMember;
      JGroupMember jgm = (JGroupMember)idm.getNetMember();
      Address other = jgm.getAddress();
      channel.waitForMulticastState(other, mcastState.longValue());
    }
  }
  
  /* 
   * (non-Javadoc)
   * MembershipManager method: wait for the given member to be gone.  Throws TimeoutException if
   * the wait goes too long
   * @see com.gemstone.gemfire.distributed.internal.membership.MembershipManager#waitForDeparture(com.gemstone.gemfire.distributed.DistributedMember)
   */
  public boolean waitForDeparture(DistributedMember mbr) throws TimeoutException, InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    boolean result = false;
    DirectChannel dc = directChannel;
    InternalDistributedMember idm = (InternalDistributedMember)mbr;
    Stub stub = new Stub(idm.getIpAddress(), idm.getPort(), idm.getVmViewId());
    int memberTimeout = this.dconfig.getMemberTimeout();
    long pauseTime = (memberTimeout < 1000) ? 100 : memberTimeout / 10;
    boolean wait;
    int numWaits = 0;
    do {
      wait = false;
      if (dc != null) {
        if (dc.hasReceiversFor(stub)) {
          wait = true;
        }
        if (wait && logger.isDebugEnabled()) {
          logger.info("waiting for receivers for {} to shut down", mbr);
        }
      }
      if (!wait) {
        synchronized(latestViewLock) {
          wait = this.latestView.contains(idm);
        }
        if (wait && logger.isDebugEnabled()) {
          logger.debug("waiting for {} to leave the membership view", mbr);
        }
      }
      if (!wait) {
        // run a message through the member's serial execution queue to ensure that all of its
        // current messages have been processed
        ThrottlingMemLinkedQueueWithDMStats serialQueue = listener.getDM().getSerialQueue(idm);
        if (serialQueue != null) {
          final boolean done[] = new boolean[1];
          final FlushingMessage msg = new FlushingMessage(done);
          serialQueue.add(new SizeableRunnable(100) {
            public void run() {
              msg.invoke();
            }
            public String toString() {
              return "Processing fake message";
            }
          });
          synchronized(done) {
            while (done[0] == false) {
              done.wait(10);
            }
            result = true;
          }
        }
      }
      if (wait) {
        numWaits++;
        if (numWaits > 40) {
          // waited over 4 * memberTimeout ms.  Give up at this point
          throw new TimeoutException("waited too long for " + idm + " to be removed");
        }
        Thread.sleep(pauseTime);
      }
    } while (wait && (this.channel.isOpen() || (dc != null && dc.isOpen())) );
    if (logger.isDebugEnabled()) {
      logger.debug("operations for {} should all be in the cache at this point", mbr);
    }
    return result;
  }
  

  /** return the distribution config used to instantiate this membership manager */
  public DistributionConfig getDistributionConfig() {
    return this.dconfig;
  }

  /**
   * check to see if the member is shunned
   * @param mbr the JGroups address of the member
   * @return true if the address has been removed from membership
   */
  public boolean memberExists(IpAddress mbr) {
    synchronized(latestViewLock) {
      InternalDistributedMember idm = getMemberFromIpAddress(mbr, true);
      if (idm == null) {
        return true; // TODO I don't think this happens
      }
      return memberExists(idm);
    }
  }
  
  public void warnShun(IpAddress mbr) {
    InternalDistributedMember idm;
    synchronized(latestViewLock) {
      idm = getMemberFromIpAddress(mbr, true);
    }
    if (idm == null) {
      return;
    }
    warnShun(idm);
  }
  
  public boolean waitForMembershipCheck(InternalDistributedMember remoteId) {
    boolean foundRemoteId = false;
    Latch currentLatch = null;
    // ARB: preconditions
    // remoteId != null
    synchronized (latestViewLock) {
      if (latestView == null) {
        // Not sure how this would happen, but see bug 38460.
        // No view?? Not found!
      }
      else if (latestView.contains(remoteId)) {
        // ARB: check if remoteId is already in membership view.
        // If not, then create a latch if needed and wait for the latch to open.
        foundRemoteId = true;
      }
      else if ((currentLatch = (Latch)this.memberLatch.get(remoteId)) == null) {
        currentLatch = new Latch();
        this.memberLatch.put(remoteId, currentLatch);
      }
    } // synchronized

    if (!foundRemoteId) {
      // ARB: wait for hardcoded 1000 ms for latch to open.
      // if-stmt precondition: currentLatch is non-null
      try {
        if (currentLatch.attempt(membershipCheckTimeout)) {
          foundRemoteId = true;
          // @todo 
          // ARB: remove latch from memberLatch map if this is the last thread waiting on latch.
        }
      }
      catch (InterruptedException ex) {
        // ARB: latch attempt was interrupted.
        Thread.currentThread().interrupt();
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.JGroupMembershipManager_THE_MEMBERSHIP_CHECK_WAS_TERMINATED_WITH_AN_EXCEPTION));
      }
    }

    // ARB: postconditions
    // (foundRemoteId == true) ==> (currentLatch is non-null ==> currentLatch is open)
    return foundRemoteId;
  }
  
  /* returns the cause of shutdown, if known */
  public Throwable getShutdownCause() {
    return this.shutdownCause;
  }
  
  public void registerTestHook(JChannelTestHook mth) {
    this.channelTestHook = mth;
  }
  
  public void unregisterTestHook(JChannelTestHook mth) {
    this.channelTestHook = null;
  }
  
  public void registerTestHook(MembershipTestHook mth) {
    // synchronize additions to avoid races during startup
    synchronized(this.latestViewLock) {
      if (this.membershipTestHooks == null) {
        this.membershipTestHooks = Collections.singletonList(mth);
      }
      else {
        List l = new ArrayList(this.membershipTestHooks);
        l.add(mth);
        this.membershipTestHooks = l;
      }
    }
  }
  
  public void unregisterTestHook(MembershipTestHook mth) {
    synchronized(this.latestViewLock) {
      if (this.membershipTestHooks != null) {
        if (this.membershipTestHooks.size() == 1) {
          this.membershipTestHooks = null;
        }
        else {
          List l = new ArrayList(this.membershipTestHooks);
          l.remove(mth);
        }
      }
    }
  }
  
  boolean beingSick;
  boolean playingDead;
  
  /**
   * Test hook - be a sick member
   */
  protected synchronized void beSick() {
    if (!beingSick) {
      beingSick = true;
      if (logger.isDebugEnabled()) {
        logger.debug("JGroupMembershipManager.beSick invoked for {} - simulating sickness", this.myMemberId);
      }
      fdProtocol.beSick();
      // close current connections and stop accepting new ones
      fdSockProtocol.beSick();
      if (directChannel != null) {
        directChannel.beSick();
      }
    }
  }
  
  /**
   * Test hook - don't answer "are you alive" requests
   */
  protected synchronized void playDead() {
    if (!playingDead) {
      playingDead = true;
      if (logger.isDebugEnabled()) {
        logger.debug("JGroupMembershipManager.playDead invoked for {}", this.myMemberId);
      }
      // close current connections and stop accepting new ones
      verifySuspectProtocol.playDead(true);
      fdProtocol.beSick();
      fdSockProtocol.beSick();
    }
  }

  /**
   * Test hook - recover health
   */
  protected synchronized void beHealthy() {
    if (beingSick || playingDead) {
      beingSick = false;
      playingDead = false;
      if (logger.isDebugEnabled()) {
        logger.debug("JGroupMembershipManager.beHealthy invoked for {} - recovering health now", this.myMemberId);
      }
      fdSockProtocol.beHealthy();
      fdProtocol.beHealthy();
      if (directChannel != null) {
        directChannel.beHealthy();
      }
      verifySuspectProtocol.playDead(false);
    }
  }
  
  /**
   * Test hook
   */
  public boolean isBeingSick() {
    return this.beingSick;
  }

  /**
   * Test hook - inhibit ForcedDisconnectException logging to keep dunit logs clean
   * @param b
   */
  public static void inhibitForcedDisconnectLogging(boolean b) {
    inhibitForceDisconnectLogging = true;
  }

  /**
   * @param uniqueID
   */
  public void setUniqueID(int uniqueID) {
    MemberAttributes.setDefaultVmPid(uniqueID);
  }

  /**
   * @return the nakack protocol if it exists
   */
  public NAKACK getNakAck() {
    return this.nakAckProtocol;
  }
  
  /** this is a fake message class that is used to flush the serial execution queue */
  static class FlushingMessage extends DistributionMessage {
    boolean[] done;
    FlushingMessage(boolean[] done) {
      this.done = done;
    }
    public void invoke() {
      synchronized(done) {
        done[0] = true;
        done.notify();
      }
    }
    protected void process(DistributionManager dm) {
      // not used
    }
    public int getDSFID() {
      return 0;
    }
    public int getProcessorType() {
      return DistributionManager.SERIAL_EXECUTOR;
    }
  }

  /**
   * Sets cache time offset in {@link DistributionManager}.
   * 
   * @param src
   * @param timeOffset
   * @see InternalDistributedSystem#getClock()
   * @see DSClock#setCacheTimeOffset(DistributedMember, long, boolean)
   */
  public void setCacheTimeOffset(Address src, long timeOffset, boolean isJoin) {
    // check if offset calculator is still in view
    InternalDistributedMember coord = (InternalDistributedMember) ipAddrToMemberMap
        .get(src);
    if (coord == null && src != null) {
      JGroupMember jgm = new JGroupMember((IpAddress)src);
      coord = new InternalDistributedMember(jgm);
    }
    if (this.listener != null) {
      DistributionManager dm = this.listener.getDM();
      dm.getSystem().getClock().setCacheTimeOffset(coord, timeOffset, isJoin);
    }
  }
  
  /**
   * returns the general purpose membership timer
   */
  public Timer getTimer() {
    return this.timer;
  }
}
