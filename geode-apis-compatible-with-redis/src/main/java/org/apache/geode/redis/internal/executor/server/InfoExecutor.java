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
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.statistics.RedisStats;

public class InfoExecutor extends AbstractExecutor {

  private static final Long ONE_MEGABYTE = 1024 * 1024L;

  private final DecimalFormat decimalFormat = new DecimalFormat("0.00");

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
      case "cluster":
        result = getClusterSection();
        break;
      case "persistence":
        result = getPersistenceSection();
        break;
      case "replication":
        result = getReplicationSection();
        break;
      case "stats":
        result = getStatsSection(context);
        break;
      case "clients":
        result = getClientsSection(context);
        break;
      case "memory":
        result = getMemorySection(context);
        break;
      case "keyspace":
        result = getKeyspaceSection(context);
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

  private String getStatsSection(ExecutionHandlerContext context) {
    final RedisStats redisStats = context.getRedisStats();
    String instantaneous_input_kbps =
        decimalFormat.format(redisStats
            .getNetworkKiloBytesReadOverLastSecond());

    return "# Stats\r\n" +
        "total_commands_processed:" + redisStats.getCommandsProcessed() + "\r\n" +
        "instantaneous_ops_per_sec:" + redisStats.getOpsPerformedOverLastSecond() + "\r\n" +
        "total_net_input_bytes:" + redisStats.getTotalNetworkBytesRead() + "\r\n" +
        "instantaneous_input_kbps:" + instantaneous_input_kbps + "\r\n" +
        "total_connections_received:" + redisStats.getTotalConnectionsReceived() + "\r\n" +
        "keyspace_hits:" + redisStats.getKeyspaceHits() + "\r\n" +
        "keyspace_misses:" + redisStats.getKeyspaceMisses() + "\r\n" +
        "evicted_keys:0\r\n" +
        "rejected_connections:0\r\n";
  }

  private String getServerSection(ExecutionHandlerContext context) {
    final String CURRENT_REDIS_VERSION = "5.0.6";
    // @todo test in info command integration test?
    final int TCP_PORT = context.getServerPort();
    final RedisStats redisStats = context.getRedisStats();
    return "# Server\r\n" +
        "redis_version:" + CURRENT_REDIS_VERSION + "\r\n" +
        "redis_mode:standalone\r\n" +
        "tcp_port:" + TCP_PORT + "\r\n" +
        "uptime_in_seconds:" + redisStats.getUptimeInSeconds() + "\r\n" +
        "uptime_in_days:" + redisStats.getUptimeInDays() + "\r\n";
  }

  private String getClientsSection(ExecutionHandlerContext context) {
    final RedisStats redisStats = context.getRedisStats();
    return "# Clients\r\n" +
        "connected_clients:" + redisStats.getConnectedClients() + "\r\n" +
        "blocked_clients:0\r\n";
  }

  private String getMemorySection(ExecutionHandlerContext context) {
    PartitionedRegion pr = (PartitionedRegion) context.getRegionProvider().getDataRegion();
    long usedMemory = pr.getDataStore().currentAllocatedMemory();
    return "# Memory\r\n" +
        "maxmemory:" + pr.getLocalMaxMemory() * ONE_MEGABYTE + "\r\n" +
        "used_memory:" + usedMemory + "\r\n" +
        "mem_fragmentation_ratio:1.00\r\n";
  }

  private String getKeyspaceSection(ExecutionHandlerContext context) {
    int numberOfKeys = context.getRegionProvider().getDataRegion().size();
    String keyspaceString = "# Keyspace\r\n";

    if (numberOfKeys > 0) {
      keyspaceString +=
          "db0:keys=" + numberOfKeys +
              ",expires=0" +
              ",avg_ttl=0\r\n";
    }

    return keyspaceString;
  }

  private String getPersistenceSection() {
    return "# Persistence\r\n" +
        "loading:0\r\n" +
        "rdb_changes_since_last_save:0\r\n" +
        "rdb_last_save_time:0\r\n";
  }

  private String getClusterSection() {
    return "# Cluster\r\ncluster_enabled:0\r\n";
  }

  private String getReplicationSection() {
    return "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n";
  }

  private String getAllSections(ExecutionHandlerContext context) {
    final String SECTION_SEPARATOR = "\r\n";
    return getServerSection(context) + SECTION_SEPARATOR +
        getClientsSection(context) + SECTION_SEPARATOR +
        getMemorySection(context) + SECTION_SEPARATOR +
        getPersistenceSection() + SECTION_SEPARATOR +
        getStatsSection(context) + SECTION_SEPARATOR +
        getKeyspaceSection(context) + SECTION_SEPARATOR +
        getReplicationSection() + SECTION_SEPARATOR +
        getClusterSection();
  }
}
