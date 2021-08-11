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
package org.apache.geode.distributed.internal.membership.api;

/**
 * Create and install a MembershipStatistics in your MembershipBuilder if you want to
 * record membership stats
 */
public interface MembershipStatistics {
  long startMsgSerialization();

  void endMsgSerialization(long start);

  long startUDPMsgEncryption();

  void endUDPMsgEncryption(long start);

  long startUDPMsgDecryption();

  void endUDPMsgDecryption(long start);

  long startMsgDeserialization();

  void endMsgDeserialization(long start);

  /**
   * Increments the total number of message bytes sent by the distribution manager
   */
  void incSentBytes(long bytes);

  long startUDPDispatchRequest();

  void endUDPDispatchRequest(long start);

  /**
   * increments the number of unicast writes performed and the number of bytes written
   *
   * @since GemFire 5.0
   */
  void incUcastWriteBytes(long bytesWritten);

  /**
   * increment the number of unicast datagram payload bytes received and the number of unicast reads
   * performed
   */
  void incUcastReadBytes(long amount);

  /**
   * increment the number of multicast datagrams sent and the number of multicast bytes transmitted
   */
  void incMcastWriteBytes(long bytesWritten);

  /**
   * increment the number of multicast datagram payload bytes received, and the number of mcast
   * messages read
   */
  void incMcastReadBytes(long amount);

  /**
   * increment the number of unicast UDP retransmission requests received from other processes
   *
   * @since GemFire 5.0
   */
  void incUcastRetransmits();

  /**
   * increment the number of multicast UDP retransmissions sent to other processes
   *
   * @since GemFire 5.0
   */
  void incMcastRetransmits();

  /**
   * increment the number of multicast UDP retransmission requests sent to other processes
   *
   * @since GemFire 5.0
   */
  void incMcastRetransmitRequests();

  void incHeartbeatRequestsSent();

  void incHeartbeatRequestsReceived();

  void incHeartbeatsSent();

  void incHeartbeatsReceived();

  void incSuspectsSent();

  void incSuspectsReceived();

  void incFinalCheckRequestsSent();

  void incFinalCheckRequestsReceived();

  void incFinalCheckResponsesSent();

  void incFinalCheckResponsesReceived();

  void incTcpFinalCheckRequestsSent();

  void incTcpFinalCheckRequestsReceived();

  void incTcpFinalCheckResponsesSent();

  void incTcpFinalCheckResponsesReceived();

  void incUdpFinalCheckRequestsSent();

  long getUdpFinalCheckRequestsSent();

  void incUdpFinalCheckResponsesReceived();

  long getHeartbeatRequestsReceived();

  long getHeartbeatsSent();

  long getSuspectsSent();

  long getSuspectsReceived();

  long getFinalCheckRequestsSent();

  long getFinalCheckRequestsReceived();

  long getFinalCheckResponsesSent();

  long getFinalCheckResponsesReceived();

  long getTcpFinalCheckRequestsSent();

  long getTcpFinalCheckRequestsReceived();

  long getTcpFinalCheckResponsesSent();

  long getTcpFinalCheckResponsesReceived();

  // Stats for GMSHealthMonitor
  long getHeartbeatRequestsSent();
}
