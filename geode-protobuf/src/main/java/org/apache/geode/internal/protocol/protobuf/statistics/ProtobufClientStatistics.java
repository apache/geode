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
package org.apache.geode.internal.protocol.protobuf.statistics;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class ProtobufClientStatistics implements ClientStatistics {
  public static final String PROTOBUF_CLIENT_STATISTICS = "ProtobufProtocolStats";
  private final Statistics stats;
  private final int currentClientConnectionsId;
  private final int clientConnectionTerminationsId;
  private final int clientConnectionStartsId;
  private final int bytesReceivedId;
  private final int bytesSentId;
  private final int messagesReceivedId;
  private final int messagesSentId;
  private final int authorizationViolationsId;
  private final int authenticationFailuresId;
  private final int operationTimeId;

  public ProtobufClientStatistics(StatisticsFactory statisticsFactory, String statisticsName) {
    if (statisticsFactory == null) {
      statisticsFactory = InternalDistributedSystem.getAnyInstance();
    }
    StatisticDescriptor[] serverStatDescriptors = new StatisticDescriptor[] {
        statisticsFactory.createLongGauge("currentClientConnections",
            "Number of sockets accepted and used for client to server messaging.", "sockets"),
        statisticsFactory.createLongCounter("clientConnectionStarts",
            "Number of sockets accepted and used for client to server messaging.", "sockets"),
        statisticsFactory.createLongCounter("clientConnectionTerminations",
            "Number of sockets that were used for client to server messaging.", "sockets"),
        statisticsFactory.createLongCounter("authenticationFailures", "Authentication failures",
            "attemptss"),
        statisticsFactory.createLongCounter("authorizationViolations",
            "Operations not allowed to proceed", "operations"),
        statisticsFactory.createLongCounter("bytesReceived",
            "Bytes received from client messaging.", "bytes"),
        statisticsFactory.createLongCounter("bytesSent", "Bytes sent for client messaging.",
            "bytes"),
        statisticsFactory.createLongCounter("messagesReceived", "Messages received from clients.",
            "messages"),
        statisticsFactory.createLongCounter("messagesSent", "Messages sent to clients.",
            "messages"),
        statisticsFactory.createLongCounter("operationTime", "Time spent performing operations",
            "nanoseconds")};
    StatisticsType statType =
        statisticsFactory.createType(getStatsName(), "Protobuf client/server statistics",
            serverStatDescriptors);
    stats = statisticsFactory.createAtomicStatistics(statType, statisticsName);
    currentClientConnectionsId = stats.nameToId("currentClientConnections");
    clientConnectionStartsId = stats.nameToId("clientConnectionStarts");
    clientConnectionTerminationsId = stats.nameToId("clientConnectionTerminations");
    authorizationViolationsId = stats.nameToId("authorizationViolations");
    authenticationFailuresId = stats.nameToId("authenticationFailures");
    bytesReceivedId = stats.nameToId("bytesReceived");
    bytesSentId = stats.nameToId("bytesSent");
    messagesReceivedId = stats.nameToId("messagesReceived");
    messagesSentId = stats.nameToId("messagesSent");
    operationTimeId = stats.nameToId("operationTime");
  }


  @Override
  public String getStatsName() {
    return PROTOBUF_CLIENT_STATISTICS;
  }

  @Override
  public void clientConnected() {
    stats.incLong(currentClientConnectionsId, 1);
    stats.incLong(clientConnectionStartsId, 1);
  }

  @Override
  public void clientDisconnected() {
    stats.incLong(currentClientConnectionsId, -1);
    stats.incLong(clientConnectionTerminationsId, 1);
  }

  @Override
  public void messageReceived(int bytes) {
    stats.incLong(bytesReceivedId, bytes);
    stats.incLong(messagesReceivedId, 1);
  }

  @Override
  public void messageSent(int bytes) {
    stats.incLong(bytesSentId, bytes);
    stats.incLong(messagesSentId, 1);
  }

  @Override
  public void incAuthorizationViolations() {
    stats.incLong(authorizationViolationsId, 1);
  }

  @Override
  public void incAuthenticationFailures() {
    stats.incLong(authenticationFailuresId, 1);
  }

  @Override
  public long startOperation() {
    return System.nanoTime();
  }

  @Override
  public void endOperation(long startOperationTime) {
    stats.incLong(operationTimeId, System.nanoTime() - startOperationTime);
  }
}
