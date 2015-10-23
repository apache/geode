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
package com.gemstone.org.jgroups.spi;

import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.stack.IpAddress;
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
