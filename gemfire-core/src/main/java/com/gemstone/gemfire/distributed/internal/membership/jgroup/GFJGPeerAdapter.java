package com.gemstone.gemfire.distributed.internal.membership.jgroup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.cache.UnsupportedVersionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataInputStream;
import com.gemstone.gemfire.internal.VersionedDataOutputStream;
import com.gemstone.gemfire.internal.VersionedObjectInput;
import com.gemstone.gemfire.internal.VersionedObjectOutput;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.HandShake;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.internal.process.StartupStatus;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.GFPeerAdapter;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.GFLogWriter;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.SockCreator;
import com.gemstone.org.jgroups.util.StringId;
import com.gemstone.org.jgroups.util.Util;

public class GFJGPeerAdapter implements GFPeerAdapter {
  
//  protected final GemFireTracer log=GemFireTracer.getLog(GFJGAdapter.class);

  private JGroupMembershipManager jgmm;
  private DMStats stats;
  
  public GFJGPeerAdapter(JGroupMembershipManager mgr, DMStats stats) {
    this.jgmm = mgr;
    this.stats = stats;
  }

  /**
   * Construct an adapter that can only be used to invoke functions
   * that do not require a membership manager or DMStats, such
   * as setMemberAttributes
   */
  public GFJGPeerAdapter() {
  }

  @Override
  public DatagramSocket getMembershipSocketForUDP() {
    return jgmm.getMembershipSocketForUDP();
  }

  @Override
  public boolean getDisableAutoReconnect() {
    return jgmm.getDistributionConfig().getDisableAutoReconnect();
  }

  @Override
  public boolean isReconnectingDS() {
    return jgmm.isReconnectingDS();
  }

  @Override
  public Timer getConnectionTimeoutTimer() {
    return jgmm.getTimer();
  }

  @Override
  public void setCacheTimeOffset(Address src, long timeOffs, boolean b) {
    jgmm.setCacheTimeOffset(src, timeOffs, b);
  }

  @Override
  public boolean getDisableTcp() {
    return jgmm.getDistributionConfig().getDisableTcp();
  }

  @Override
  public boolean isShunnedMemberNoSync(IpAddress mbr) {
    return jgmm.isShunnedMemberNoSync(mbr);
  }

  @Override
  public int getAckWaitThreshold() {
    return jgmm.getDistributionConfig().getAckWaitThreshold();
  }

  @Override
  public int getAckSevereAlertThreshold() {
    return jgmm.getDistributionConfig().getAckSevereAlertThreshold();
  }

  @Override
  public int getSerialQueueThrottleTime(Address src) {
    return jgmm.getSerialQueueThrottleTime(src);
  }

  @Override
  public void pongReceived(SocketAddress socketAddress) {
    if (jgmm.getQuorumCheckerImpl() != null) {
      jgmm.getQuorumCheckerImpl().pongReceived(socketAddress);
    }
  }

  @Override
  public void quorumLost(Set failures, List remaining) {
    jgmm.quorumLost(failures, remaining);
  }

  @Override
  public boolean isShuttingDown(IpAddress addr) {
    return jgmm.isShuttingDown(addr);
  }

  @Override
  public int getMcastPort() {
    return jgmm.getDistributionConfig().getMcastPort();
  }

  @Override
  public void enableNetworkPartitionDetection() {
    jgmm.enableNetworkPartitionDetection();
  }

  @Override
  public long getShunnedMemberTimeout() {
    return jgmm.getShunnedMemberTimeout();
  }

  @Override
  public long getMemberTimeout() {
    return jgmm.getDistributionConfig().getMemberTimeout();
  }

  @Override
  public void verifyCredentials(String authenticator, Properties credentials,
      Properties securityProperties, GFLogWriter logWriter,
      GFLogWriter securityLogWriter, Address src) {
    DistributedMember addr = createDistributedMember(src);
    if (addr != null) {
      HandShake.verifyCredentials(authenticator, credentials, securityProperties, 
          (InternalLogWriter)logWriter, (InternalLogWriter)securityLogWriter, addr);
    }
  }
  
  @Override
  public Properties getCredentials(String authInitMethod, Properties secProps,
      Address mbr, boolean isPeer, GFLogWriter logWriter,
      GFLogWriter securityLogWriter) {
    DistributedMember addr = createDistributedMember(mbr);
    return HandShake.getCredentials(authInitMethod, secProps, addr, isPeer,
        (InternalLogWriter)logWriter, (InternalLogWriter)securityLogWriter);
  }

  
  /**
   * Creates an {@link InternalDistributedMember} from the given {@link Address}
   * object assuming the address to be an instance of {@link IpAddress}.
   * 
   * @param addr
   *                the {@link Address} object of the member
   * @return the {@link DistributedMember} for the given address
   */
  private DistributedMember createDistributedMember(Address addr) {
    if (addr == null) {
      return null;
    }
    IpAddress ipAddr = (IpAddress)addr;
    DistributedMember member = jgmm.getMemberFromIpAddress(ipAddr, false);
    if (member == null) {
      JGroupMember jgm = new JGroupMember(ipAddr);
      member = new InternalDistributedMember(jgm);
    }
    if (member != null && member.equals(jgmm.getLocalMember())) {
      return null;
    }
    return member;
  }

  /** log startup status that will be displayed in gfsh or other user-interfaces */
  public void logStartup(StringId msgId, Object...params) {
    StartupStatus.startup(msgId, params);
  }


  @Override
  public void beforeChannelClosing(String string, RuntimeException closeException) {
    AlertAppender.getInstance().shuttingDown();
    
    // This test hook is used in LocatorTest
    if (jgmm.channelTestHook != null) {
      jgmm.channelTestHook.beforeChannelClosing("before channel closing", closeException);
    }
  }
  

  @Override
  public void beforeSendingPayload(Object gfmsg) {
    if(gfmsg instanceof DirectReplyMessage) {
      ((DirectReplyMessage) gfmsg).registerProcessor();
    }
  }

  

  
  
  
  @Override
  public void incUcastReadBytes(int len) {
    stats.incUcastReadBytes(len);
  }

  @Override
  public void incjChannelUpTime(long l) {
    stats.incjChannelUpTime(l);

  }

  @Override
  public void incjgChannelDownTime(long l) {
    stats.incjgDownTime(l);
  }

  @Override
  public void setJgUNICASTreceivedMessagesSize(long rm) {
    stats.setJgUNICASTreceivedMessagesSize(rm);
  }

  @Override
  public void incJgUNICASTdataReceived(long l) {
    stats.incJgUNICASTdataReceived(l);
  }

  @Override
  public void setJgUNICASTsentHighPriorityMessagesSize(long hm) {
    stats.setJgUNICASTreceivedMessagesSize(hm);
  }

  @Override
  public void setJgUNICASTsentMessagesSize(long sm) {
    stats.setJgUNICASTsentMessagesSize(sm);
  }

  @Override
  public void incUcastRetransmits() {
    stats.incUcastRetransmits();
  }

  @Override
  public void incJgFragmentsCreated(int num_frags) {
    stats.incJgFragmentsCreated(num_frags);
  }

  @Override
  public void incJgFragmentationsPerformed() {
    stats.incJgFragmentationsPerformed();
  }

  @Override
  public void incMcastReadBytes(int len) {
    stats.incMcastReadBytes(len);
  }

  @Override
  public long startMcastWrite() {
    return stats.startMcastWrite();
  }

  @Override
  public void endMcastWrite(long start, int length) {
    stats.endMcastWrite(start, length);
  }

  @Override
  public long startUcastWrite() {
    return stats.startUcastWrite();
  }

  @Override
  public void endUcastWrite(long start, int length) {
    stats.endUcastWrite(start, length);
  }

  @Override
  public void incFlowControlResponses() {
    stats.incFlowControlResponses();
  }

  @Override
  public void incJgFCsendBlocks(int i) {
    stats.incJgFCsendBlocks(i);
  }

  @Override
  public long startFlowControlWait() {
    return stats.startFlowControlWait();
  }

  @Override
  public void incJgFCautoRequests(int i) {
    stats.incJgFCautoRequests(i);
  }

  @Override
  public void endFlowControlWait(long blockStartTime) {
    stats.endFlowControlWait(blockStartTime);
  }
  

  @Override
  public long startFlowControlThrottleWait() {
    return stats.startFlowControlThrottleWait();
  }

  @Override
  public void endFlowControlThrottleWait(long blockStartTime) {
    stats.endFlowControlThrottleWait(blockStartTime);
  }

  @Override
  public void incJgFCreplenish(int i) {
    stats.incJgFCreplenish(i);
  }

  @Override
  public void incJgFCresumes(int i) {
    stats.incJgFCresumes(i);
  }

  @Override
  public void setJgQueuedMessagesSize(int size) {
    stats.setJgQueuedMessagesSize(size);
  }

  @Override
  public void incJgFCsentThrottleRequests(int i) {
    stats.incJgFCsentThrottleRequests(i);
  }

  @Override
  public void incJgFCsentCredits(int i) {
    stats.incJgFCsentCredits(i);
  }

  @Override
  public void incFlowControlRequests() {
    stats.incFlowControlRequests();
  }

  @Override
  public void incjgUpTime(long l) {
    stats.incjgUpTime(l);
  }

  @Override
  public void incBatchSendTime(long gsstart) {
    stats.incBatchSendTime(gsstart);
  }

  @Override
  public void incBatchCopyTime(long start) {
    stats.incBatchCopyTime(start);
  }

  @Override
  public void incBatchFlushTime(long start) {
    stats.incBatchFlushTime(start);
  }

  @Override
  public void incMcastRetransmitRequests() {
    stats.incMcastRetransmitRequests();
  }

  @Override
  public void setJgSTABLEreceivedMessagesSize(long received_msgs_size) {
    stats.setJgSTABLEreceivedMessagesSize(received_msgs_size);
  }

  @Override
  public void setJgSTABLEsentMessagesSize(int size) {
    stats.setJgSTABLEsentMessagesSize(size);
  }

  @Override
  public void incJgNAKACKwaits(int i) {
    stats.incJgNAKACKwaits(i);
  }

  @Override
  public void incMcastRetransmits() {
    stats.incMcastRetransmits();
  }

  @Override
  public void incJgSTABLEmessagesSent(int i) {
    stats.incJgSTABLEmessagesSent(i);
  }

  @Override
  public void incJgSTABLEmessages(int i) {
    stats.incJgSTABLEmessages(i);
  }

  @Override
  public void incJgSTABLEsuspendTime(long l) {
    stats.incJgSTABLEsuspendTime(l);
  }

  @Override
  public void incJgSTABILITYmessages(int i) {
    stats.incJgSTABILITYmessages(i);
  }
  
  
  
  ///////////////////////////////// Serialization methods

  @Override
  public boolean shutdownHookIsAlive() {
    Thread shutdownHook = InternalDistributedSystem.shutdownHook;
    return (shutdownHook != null && shutdownHook.isAlive());
  }
  
  @Override
  public boolean isAdminOnlyMember() {
    return (DistributionManager.getDistributionManagerType() == 
        DistributionManager.ADMIN_ONLY_DM_TYPE || DistributionManager.getDistributionManagerType() == 
        DistributionManager.LOCATOR_DM_TYPE) // [GemStone]
        && !Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE)
        && !DistributionManager.isCommandLineAdminVM;
  }


  @Override
  public boolean hasLocator() {
    return Locator.hasLocator();
  }

  @Override
  public boolean isDisconnecting() {
    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    return (system != null && system.isDisconnecting());
  }

}
