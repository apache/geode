package com.gemstone.org.jgroups.stack;

import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.util.GFLogWriter;
import com.gemstone.org.jgroups.util.StringId;

/**
 * The GFAdapter interface provides API access to certain GemFire functions
 * such as statistics recording.
 * 
 * @author bschuchardt
 *
 */

public interface GFPeerAdapter {
  
  //////////////////////// Configuration access and notifications
  
  DatagramSocket getMembershipSocketForUDP();

  boolean getDisableAutoReconnect();

  boolean isReconnectingDS();

  Timer getConnectionTimeoutTimer();

  void setCacheTimeOffset(Address src, long timeOffs, boolean b);

  boolean getDisableTcp();

  boolean isShunnedMemberNoSync(IpAddress mbr);

  int getAckWaitThreshold();

  int getAckSevereAlertThreshold();

  int getSerialQueueThrottleTime(Address src);

  void pongReceived(SocketAddress socketAddress);

  void quorumLost(Set failures, List remaining);

  boolean isShuttingDown(IpAddress addr);

  int getMcastPort();

  void enableNetworkPartitionDetection();

  long getShunnedMemberTimeout();

  long getMemberTimeout();

  void logStartup(StringId str, Object...args);

  

  //////////////////////// Statistics recording

  void incUcastReadBytes(int len);

  void incjChannelUpTime(long l);

  void incjgChannelDownTime(long l);

  void setJgUNICASTreceivedMessagesSize(long rm);

  void incJgUNICASTdataReceived(long l);

  void setJgUNICASTsentHighPriorityMessagesSize(long hm);

  void setJgUNICASTsentMessagesSize(long sm);

  void incUcastRetransmits();

  void incJgFragmentsCreated(int num_frags);

  void incJgFragmentationsPerformed();

  void incMcastReadBytes(int len);

  long startMcastWrite();

  void endMcastWrite(long start, int length);

  long startUcastWrite();

  void endUcastWrite(long start, int length);

  void incFlowControlResponses();

  void incJgFCsendBlocks(int i);

  long startFlowControlWait();

  void incJgFCautoRequests(int i);

  void endFlowControlWait(long blockStartTime);

  long startFlowControlThrottleWait();

  void endFlowControlThrottleWait(long blockStartTime);

  void incJgFCreplenish(int i);

  void incJgFCresumes(int i);

  void setJgQueuedMessagesSize(int size);

  void incJgFCsentThrottleRequests(int i);

  void incJgFCsentCredits(int i);

  void incFlowControlRequests();

  void incjgUpTime(long l);

  void incBatchSendTime(long gsstart);

  void incBatchCopyTime(long start);

  void incBatchFlushTime(long start);

  void incMcastRetransmitRequests();

  void setJgSTABLEreceivedMessagesSize(long received_msgs_size);

  void setJgSTABLEsentMessagesSize(int size);

  void incJgNAKACKwaits(int i);

  void incMcastRetransmits();

  void incJgSTABLEmessagesSent(int i);

  void incJgSTABLEmessages(int i);

  void incJgSTABLEsuspendTime(long l);

  void incJgSTABILITYmessages(int i);

  void beforeChannelClosing(String string, RuntimeException closeException);

  void beforeSendingPayload(Object gfmsg);

  boolean shutdownHookIsAlive();

  boolean isAdminOnlyMember();

  boolean hasLocator();

  boolean isDisconnecting();

  Properties getCredentials(String authInitMethod, Properties secProps,
      Address createDistributedMember, boolean b,
      GFLogWriter logWriter, GFLogWriter securityLogWriter);

  void verifyCredentials(String authenticator, Properties credentials,
      Properties securityProperties, GFLogWriter logWriter,
      GFLogWriter securityLogWriter, Address src);

}
