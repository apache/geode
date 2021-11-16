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

import static org.apache.geode.redis.internal.netty.Coder.toUpperCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.ALL;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.CLIENTS;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.CLUSTER;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.DEFAULT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.KEYSPACE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.MEMORY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.PERSISTENCE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.REPLICATION;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.SERVER;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.STATS;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.statistics.RedisStats;

public class InfoExecutor implements CommandExecutor {

  private static final Long ONE_MEGABYTE = 1024 * 1024L;

  private DecimalFormat decimalFormat = new DecimalFormat("0.00");

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    String result;
    List<byte[]> commands = command.getProcessedCommand();

    if (containsSectionParameter(commands)) {
      result = getSpecifiedSection(context, commands);
    } else {
      result = getAllSections(context);
    }
    return RedisResponse.bulkString(result);
  }

  private boolean containsSectionParameter(List<byte[]> commands) {
    return commands.size() == 2;
  }

  private String getSpecifiedSection(ExecutionHandlerContext context, List<byte[]> commands) {
    byte[] bytes = toUpperCaseBytes(commands.get(1));
    if (Arrays.equals(bytes, SERVER)) {
      return getServerSection(context);
    } else if (Arrays.equals(bytes, CLUSTER)) {
      return getClusterSection();
    } else if (Arrays.equals(bytes, PERSISTENCE)) {
      return getPersistenceSection();
    } else if (Arrays.equals(bytes, REPLICATION)) {
      return getReplicationSection();
    } else if (Arrays.equals(bytes, STATS)) {
      return getStatsSection(context);
    } else if (Arrays.equals(bytes, CLIENTS)) {
      return getClientsSection(context);
    } else if (Arrays.equals(bytes, MEMORY)) {
      return getMemorySection(context);
    } else if (Arrays.equals(bytes, KEYSPACE)) {
      return getKeyspaceSection(context);
    } else if (Arrays.equals(bytes, DEFAULT) || Arrays.equals(bytes, ALL)) {
      return getAllSections(context);
    } else {
      return "";
    }
  }

  private String getStatsSection(ExecutionHandlerContext context) {
    final RedisStats redisStats = context.getRedisStats();
    String instantaneous_input_kbps =
        decimalFormat.format(redisStats
            .getNetworkKiloBytesReadOverLastSecond());

    final String STATS_STRING =
        "# Stats\r\n" +
            "total_commands_processed:" + redisStats.getCommandsProcessed() + "\r\n" +
            "instantaneous_ops_per_sec:" + redisStats.getOpsPerformedOverLastSecond() + "\r\n" +
            "total_net_input_bytes:" + redisStats.getTotalNetworkBytesRead() + "\r\n" +
            "instantaneous_input_kbps:" + instantaneous_input_kbps + "\r\n" +
            "total_connections_received:" + redisStats.getTotalConnectionsReceived() + "\r\n" +
            "keyspace_hits:" + redisStats.getKeyspaceHits() + "\r\n" +
            "keyspace_misses:" + redisStats.getKeyspaceMisses() + "\r\n" +
            "evicted_keys:0\r\n" +
            "rejected_connections:0\r\n";

    return STATS_STRING;
  }

  private String getServerSection(ExecutionHandlerContext context) {
    final String CURRENT_REDIS_VERSION = "5.0.6";
    // @todo test in info command integration test?
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
            "connected_clients:" + redisStats.getConnectedClients() + "\r\n" +
            "blocked_clients:0\r\n";
    return CLIENTS_STRING;
  }

  private String getMemorySection(ExecutionHandlerContext context) {
    PartitionedRegion pr = (PartitionedRegion) context.getRegionProvider().getDataRegion();
    long usedMemory = pr.getDataStore().currentAllocatedMemory();
    final String MEMORY_STRING =
        "# Memory\r\n" +
            "maxmemory:" + pr.getLocalMaxMemory() * ONE_MEGABYTE + "\r\n" +
            "used_memory:" + usedMemory + "\r\n" +
            "mem_fragmentation_ratio:1.00\r\n";
    return MEMORY_STRING;
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
    final String PERSISTENCE_STRING =
        "# Persistence\r\n" +
            "loading:0\r\n" +
            "rdb_changes_since_last_save:0\r\n" +
            "rdb_last_save_time:0\r\n";
    return PERSISTENCE_STRING;
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
