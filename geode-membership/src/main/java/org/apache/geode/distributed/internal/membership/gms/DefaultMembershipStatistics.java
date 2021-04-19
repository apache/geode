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

import org.apache.geode.distributed.internal.membership.api.MembershipStatistics;

/**
 * DefaultMembeshipStatistics is the default implementation of MembershipStatistics. Create
 * and install your own if you want to record membership stats.
 */
public class DefaultMembershipStatistics implements MembershipStatistics {
  private long sentBytes;
  private long ucastWriteBytes;
  private long ucastReadBytes;
  private long mcastWriteBytes;
  private long mcastReadBytes;
  private long ucastRetransmits;
  private long mcastRetransmits;
  private long mcastRetransmitRequests;
  private long heartbeatRequestsSent;
  private long heartbeatRequestsReceived;
  private long heartbeatsSent;
  private long heartbeatsReceived;
  private long suspectsSent;
  private long suspectsReceived;
  private long finalCheckRequestsSent;
  private long finalCheckRequestsReceived;
  private long finalCheckResponsesSent;
  private long finalCheckResponsesReceived;
  private long tcpFinalCheckRequestsSent;
  private long tcpFinalCheckRequestsReceived;
  private long tcpFinalCheckResponsesSent;
  private long tcpFinalCheckResponsesReceived;
  private long udpFinalCheckRequestsSent;
  private long udpFinalCheckResponsesReceived;

  @Override
  public long startMsgSerialization() {
    return 0;
  }

  @Override
  public void endMsgSerialization(final long start) {

  }

  @Override
  public long startUDPMsgEncryption() {
    return 0;
  }

  @Override
  public void endUDPMsgEncryption(final long start) {

  }

  @Override
  public long startUDPMsgDecryption() {
    return 0;
  }

  @Override
  public void endUDPMsgDecryption(final long start) {

  }

  @Override
  public long startMsgDeserialization() {
    return 0;
  }

  @Override
  public void endMsgDeserialization(final long start) {

  }

  @Override
  public void incSentBytes(final long bytes) {
    sentBytes++;
  }

  @Override
  public long startUDPDispatchRequest() {
    return 0;
  }

  @Override
  public void endUDPDispatchRequest(final long start) {

  }

  @Override
  public void incUcastWriteBytes(final long bytesWritten) {
    ucastWriteBytes++;
  }

  @Override
  public void incUcastReadBytes(final long amount) {
    ucastReadBytes++;
  }

  @Override
  public void incMcastWriteBytes(final long bytesWritten) {
    mcastWriteBytes++;
  }

  @Override
  public void incMcastReadBytes(final long amount) {
    mcastReadBytes++;
  }

  @Override
  public void incUcastRetransmits() {
    ucastRetransmits++;
  }

  @Override
  public void incMcastRetransmits() {
    mcastRetransmits++;
  }

  @Override
  public void incMcastRetransmitRequests() {
    mcastRetransmitRequests++;
  }

  @Override
  public void incHeartbeatRequestsSent() {
    heartbeatRequestsSent++;
  }

  @Override
  public void incHeartbeatRequestsReceived() {
    heartbeatRequestsReceived++;
  }

  @Override
  public void incHeartbeatsSent() {
    heartbeatsSent++;
  }

  @Override
  public void incHeartbeatsReceived() {
    heartbeatsReceived++;
  }

  @Override
  public void incSuspectsSent() {
    suspectsSent++;
  }

  @Override
  public void incSuspectsReceived() {
    suspectsReceived++;
  }

  @Override
  public void incFinalCheckRequestsSent() {
    finalCheckRequestsSent++;
  }

  @Override
  public void incFinalCheckRequestsReceived() {
    finalCheckRequestsReceived++;
  }

  @Override
  public void incFinalCheckResponsesSent() {
    finalCheckResponsesSent++;
  }

  @Override
  public void incFinalCheckResponsesReceived() {
    finalCheckResponsesReceived++;
  }

  @Override
  public void incTcpFinalCheckRequestsSent() {
    tcpFinalCheckRequestsSent++;
  }

  @Override
  public void incTcpFinalCheckRequestsReceived() {
    tcpFinalCheckRequestsReceived++;
  }

  @Override
  public void incTcpFinalCheckResponsesSent() {
    tcpFinalCheckResponsesSent++;
  }

  @Override
  public void incTcpFinalCheckResponsesReceived() {
    tcpFinalCheckResponsesReceived++;
  }

  @Override
  public void incUdpFinalCheckRequestsSent() {
    udpFinalCheckRequestsSent++;
  }

  @Override
  public long getUdpFinalCheckRequestsSent() {
    return udpFinalCheckRequestsSent;
  }

  @Override
  public void incUdpFinalCheckResponsesReceived() {
    udpFinalCheckResponsesReceived++;
  }

  @Override
  public long getHeartbeatRequestsReceived() {
    return heartbeatRequestsReceived;
  }

  @Override
  public long getHeartbeatsSent() {
    return heartbeatsSent;
  }

  @Override
  public long getSuspectsSent() {
    return suspectsSent;
  }

  @Override
  public long getSuspectsReceived() {
    return suspectsReceived;
  }

  @Override
  public long getFinalCheckRequestsSent() {
    return finalCheckRequestsSent;
  }

  @Override
  public long getFinalCheckRequestsReceived() {
    return finalCheckRequestsReceived;
  }

  @Override
  public long getFinalCheckResponsesSent() {
    return finalCheckResponsesSent;
  }

  @Override
  public long getFinalCheckResponsesReceived() {
    return finalCheckResponsesReceived;
  }

  @Override
  public long getTcpFinalCheckRequestsSent() {
    return tcpFinalCheckRequestsSent;
  }

  @Override
  public long getTcpFinalCheckRequestsReceived() {
    return tcpFinalCheckRequestsReceived;
  }

  @Override
  public long getTcpFinalCheckResponsesSent() {
    return tcpFinalCheckResponsesSent;
  }

  @Override
  public long getTcpFinalCheckResponsesReceived() {
    return tcpFinalCheckResponsesReceived;
  }

  @Override
  public long getHeartbeatRequestsSent() {
    return heartbeatRequestsSent;
  }
}
