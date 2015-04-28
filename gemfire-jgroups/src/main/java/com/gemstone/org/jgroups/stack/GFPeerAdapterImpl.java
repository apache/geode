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

public class GFPeerAdapterImpl implements GFPeerAdapter {
  
  @Override
  public DatagramSocket getMembershipSocketForUDP() {
    return null;
  }

  @Override
  public boolean getDisableAutoReconnect() {
    return true;
  }

  @Override
  public boolean isReconnectingDS() {
    return false;
  }

  @Override
  public Timer getConnectionTimeoutTimer() {
    return null;
  }

  @Override
  public void setCacheTimeOffset(Address src, long timeOffs, boolean b) {
  }
  
  @Override
  public boolean getDisableTcp() {
    return true;
  }

  @Override
  public boolean isShunnedMemberNoSync(IpAddress mbr) {
    return false;
  }

  @Override
  public int getAckWaitThreshold() {
    return 15;
  }

  @Override
  public int getAckSevereAlertThreshold() {
    return 0;
  }

  @Override
  public int getSerialQueueThrottleTime(Address src) {
    return 0;
  }

  @Override
  public void pongReceived(SocketAddress socketAddress) {
  }

  @Override
  public void quorumLost(Set failures, List remaining) {
  }

  @Override
  public boolean isShuttingDown(IpAddress addr) {
    return false;
  }

  @Override
  public int getMcastPort() {
    return 0;
  }

  @Override
  public void enableNetworkPartitionDetection() {
  }

  @Override
  public long getShunnedMemberTimeout() {
    return 60000;
  }

  @Override
  public long getMemberTimeout() {
    return 5000;
  }

  @Override
  public Properties getCredentials(String authInitMethod, Properties secProps,
      Address createDistributedMember, boolean b, GFLogWriter logWriter,
      GFLogWriter securityLogWriter) {
    return null;
  }

  @Override
  public void verifyCredentials(String authenticator, Properties credentials,
      Properties securityProperties, GFLogWriter logWriter,
      GFLogWriter securityLogWriter, Address src) {
  }

  @Override
  public void logStartup(StringId msgId, Object...params) {
  }


  
  
  
  
  
  
  
  @Override
  public void incUcastReadBytes(int len) {
  }

  @Override
  public void incjChannelUpTime(long l) {
  }

  @Override
  public void incjgChannelDownTime(long l) {
  }

  @Override
  public void setJgUNICASTreceivedMessagesSize(long rm) {
  }

  @Override
  public void incJgUNICASTdataReceived(long l) {
  }

  @Override
  public void setJgUNICASTsentHighPriorityMessagesSize(long hm) {
  }

  @Override
  public void setJgUNICASTsentMessagesSize(long sm) {
  }

  @Override
  public void incUcastRetransmits() {
  }

  @Override
  public void incJgFragmentsCreated(int num_frags) {
  }

  @Override
  public void incJgFragmentationsPerformed() {
  }

  @Override
  public void incMcastReadBytes(int len) {
  }

  @Override
  public long startMcastWrite() {
    return 0;
  }

  @Override
  public void endMcastWrite(long start, int length) {
  }

  @Override
  public long startUcastWrite() {
    return 0;
  }

  @Override
  public void endUcastWrite(long start, int length) {
  }

  @Override
  public void incFlowControlResponses() {
  }

  @Override
  public void incJgFCsendBlocks(int i) {
  }

  @Override
  public long startFlowControlWait() {
    return 0;
  }

  @Override
  public void incJgFCautoRequests(int i) {
  }

  @Override
  public void endFlowControlWait(long blockStartTime) {
  }

  @Override
  public long startFlowControlThrottleWait() {
    return 0;
  }

  @Override
  public void endFlowControlThrottleWait(long blockStartTime) {
  }

  @Override
  public void incJgFCreplenish(int i) {
  }

  @Override
  public void incJgFCresumes(int i) {
  }

  @Override
  public void setJgQueuedMessagesSize(int size) {
  }

  @Override
  public void incJgFCsentThrottleRequests(int i) {
  }

  @Override
  public void incJgFCsentCredits(int i) {
  }

  @Override
  public void incFlowControlRequests() {
  }

  @Override
  public void incjgUpTime(long l) {
  }

  @Override
  public void incBatchSendTime(long gsstart) {
  }

  @Override
  public void incBatchCopyTime(long start) {
  }

  @Override
  public void incBatchFlushTime(long start) {
  }

  @Override
  public void incMcastRetransmitRequests() {
  }

  @Override
  public void setJgSTABLEreceivedMessagesSize(long received_msgs_size) {
  }

  @Override
  public void setJgSTABLEsentMessagesSize(int size) {
  }

  @Override
  public void incJgNAKACKwaits(int i) {
  }

  @Override
  public void incMcastRetransmits() {
  }

  @Override
  public void incJgSTABLEmessagesSent(int i) {
  }

  @Override
  public void incJgSTABLEmessages(int i) {
  }

  @Override
  public void incJgSTABLEsuspendTime(long l) {
  }

  @Override
  public void incJgSTABILITYmessages(int i) {
  }


  
  @Override
  public void beforeChannelClosing(String string,
      RuntimeException closeException) {
  }

  @Override
  public void beforeSendingPayload(Object gfmsg) {
  }

  @Override
  public boolean shutdownHookIsAlive() {
    return false;
  }

  @Override
  public boolean isAdminOnlyMember() {
    return false;
  }

  @Override
  public boolean hasLocator() {
    return false;
  }

  @Override
  public boolean isDisconnecting() {
    return false;
  }

}
