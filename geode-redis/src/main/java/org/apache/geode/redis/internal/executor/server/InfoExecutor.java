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

package org.apache.geode.redis.internal.executor.server;

import java.text.DecimalFormat;
import java.util.List;

import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class InfoExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    String result;
    List<ByteArrayWrapper> commands =
        command.getProcessedCommandWrappers();

    if (containsSectionParameter(commands)) {
      result = getSpecifiedSection(context, commands);
    } else {
      result = getAllSections(context);
    }
    return RedisResponse.bulkString(result);
  }

  private boolean containsSectionParameter(List<ByteArrayWrapper> commands) {
    return commands.size() == 2;
  }

  private String getSpecifiedSection(ExecutionHandlerContext context,
      List<ByteArrayWrapper> commands) {
    String result;
    String section = commands.get(1).toString().toLowerCase();
    switch (section) {
      case "server":
        result = getServerSection(context);
        break;
      case "clients":
        result = getClientsSection(context);
        break;
      case "memory":
        result = getMemorySection(context);
        break;
      case "stats":
        result = getStatsSection(context);
        break;
      case "keyspace":
        result = getKeyspaceSection(context);
        break;
      case "cluster":
        result = getClusterSection();
        break;
      case "persistence":
        result = getPersistenceSection();
        break;
      case "default":
      case "all":
        result = getAllSections(context);
        break;
      default:
        result = "";
        break;
    }
    return result;
  }

  private String getServerSection(ExecutionHandlerContext context) {
    final String CURRENT_REDIS_VERSION = "5.0.6";
    final int TCP_PORT = context.getServerPort();
    final RedisStats redisStats = context.getRedisStats();
    final String SERVER_STRING =
        "# Server\r\n" +
            "redis_version:" + CURRENT_REDIS_VERSION + "\r\n" +
            "redis_mode:standalone\r\n" +
            "tcp_port:" + TCP_PORT + "\r\n" +
            "uptime_in_seconds:" + redisStats.getUptimeInSeconds() + "\r\n" +
            "uptime_in_days:" + redisStats.getUptimeInDays() + "\r\n";
    return SERVER_STRING;
  }

  private String getClientsSection(ExecutionHandlerContext context) {
    final RedisStats redisStats = context.getRedisStats();
    final String CLIENTS_STRING =
        "# Clients\r\n" +
            "connected_clients:" + redisStats.getConnectedClients() + "\r\n";
    return CLIENTS_STRING;
  }

  private String getMemorySection(ExecutionHandlerContext context) {
    PartitionedRegion pr = (PartitionedRegion) context.getRegionProvider().getDataRegion();
    long usedMemory = pr.getDataStore().currentAllocatedMemory();
    final String MEMORY_STRING =
        "# Memory\r\n" +
            "used_memory:" + usedMemory + "\r\n";
    return MEMORY_STRING;
  }

  private String getStatsSection(ExecutionHandlerContext context) {
    final RedisStats redisStats = context.getRedisStats();
    String instantaneous_input_kbps =
        new DecimalFormat("0.00")
            .format(redisStats.getNetworkKilobytesReadPerSecond());
    final String STATS_STRING =
        "# Stats\r\n" +
            "total_commands_processed:" + redisStats.getCommandsProcessed() + "\r\n" +
            "instantaneous_ops_per_sec:" + redisStats.getOpsPerSecond() + "\r\n" +
            "total_net_input_bytes:" + redisStats.getNetworkBytesRead() + "\r\n" +
            "instantaneous_input_kbps:" + instantaneous_input_kbps + "\r\n" +
            "total_connections_received:" + redisStats.getConnectionsReceived() + "\r\n" +
            "rejected_connections:0\r\n";
    return STATS_STRING;
  }

  private String getKeyspaceSection(ExecutionHandlerContext context) {
    final RedisStats redisStats = context.getRedisStats();
    final String KEYSPACE_STRING =
        "# Keyspace\r\n" +
            "db0:keys=" + context.getRegionProvider().getDataRegion().size() +
            ",expires=" + redisStats.getExpirations() + ",avg_ttl=0\r\n";
    return KEYSPACE_STRING;
  }

  private String getPersistenceSection() {
    return "# Persistence\r\nloading:0\r\n";
  }

  private String getClusterSection() {
    return "# Cluster\r\ncluster_enabled:0\r\n";
  }

  private String getAllSections(ExecutionHandlerContext context) {
    final String SECTION_SEPARATOR = "\r\n";
    return getServerSection(context) + SECTION_SEPARATOR +
        getClientsSection(context) + SECTION_SEPARATOR +
        getMemorySection(context) + SECTION_SEPARATOR +
        getPersistenceSection() + SECTION_SEPARATOR +
        getStatsSection(context) + SECTION_SEPARATOR +
        getKeyspaceSection(context) + SECTION_SEPARATOR +
        getClusterSection();
  }
}
