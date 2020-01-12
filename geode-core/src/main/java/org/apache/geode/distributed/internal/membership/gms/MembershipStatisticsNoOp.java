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
 * MembeshipStatisticsNoOp is the default implementation of MembershipStatistics. Create
 * and install your own if you want to record membership stats.
 */
public class MembershipStatisticsNoOp implements MembershipStatistics {
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

  }

  @Override
  public long startUDPDispatchRequest() {
    return 0;
  }

  @Override
  public void endUDPDispatchRequest(final long start) {

  }

  @Override
  public void incUcastWriteBytes(final int bytesWritten) {

  }

  @Override
  public void incUcastReadBytes(final int amount) {

  }

  @Override
  public void incMcastWriteBytes(final int bytesWritten) {

  }

  @Override
  public void incMcastReadBytes(final int amount) {

  }

  @Override
  public void incUcastRetransmits() {

  }

  @Override
  public void incMcastRetransmits() {

  }

  @Override
  public void incMcastRetransmitRequests() {

  }

  @Override
  public void incHeartbeatRequestsSent() {

  }

  @Override
  public void incHeartbeatRequestsReceived() {

  }

  @Override
  public void incHeartbeatsSent() {

  }

  @Override
  public void incHeartbeatsReceived() {

  }

  @Override
  public void incSuspectsSent() {

  }

  @Override
  public void incSuspectsReceived() {

  }

  @Override
  public void incFinalCheckRequestsSent() {

  }

  @Override
  public void incFinalCheckRequestsReceived() {

  }

  @Override
  public void incFinalCheckResponsesSent() {

  }

  @Override
  public void incFinalCheckResponsesReceived() {

  }

  @Override
  public void incTcpFinalCheckRequestsSent() {

  }

  @Override
  public void incTcpFinalCheckRequestsReceived() {

  }

  @Override
  public void incTcpFinalCheckResponsesSent() {

  }

  @Override
  public void incTcpFinalCheckResponsesReceived() {

  }

  @Override
  public void incUdpFinalCheckRequestsSent() {

  }

  @Override
  public long getUdpFinalCheckRequestsSent() {
    return 0;
  }

  @Override
  public void incUdpFinalCheckResponsesReceived() {

  }

  @Override
  public long getHeartbeatRequestsReceived() {
    return 0;
  }

  @Override
  public long getHeartbeatsSent() {
    return 0;
  }

  @Override
  public long getSuspectsSent() {
    return 0;
  }

  @Override
  public long getSuspectsReceived() {
    return 0;
  }

  @Override
  public long getFinalCheckRequestsSent() {
    return 0;
  }

  @Override
  public long getFinalCheckRequestsReceived() {
    return 0;
  }

  @Override
  public long getFinalCheckResponsesSent() {
    return 0;
  }

  @Override
  public long getFinalCheckResponsesReceived() {
    return 0;
  }

  @Override
  public long getTcpFinalCheckRequestsSent() {
    return 0;
  }

  @Override
  public long getTcpFinalCheckRequestsReceived() {
    return 0;
  }

  @Override
  public long getTcpFinalCheckResponsesSent() {
    return 0;
  }

  @Override
  public long getTcpFinalCheckResponsesReceived() {
    return 0;
  }

  @Override
  public long getHeartbeatRequestsSent() {
    return 0;
  }
}
