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
 *
 */

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;

import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.logging.internal.log4j.api.FastLogger;
import org.apache.geode.redis.ClusterNode;
import org.apache.geode.redis.ClusterNodes;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.cluster.RedisMemberInfo;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RedisClusterStartupRule extends ClusterStartupRule {

  public static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  public static final String BIND_ADDRESS = "127.0.0.1";

  public RedisClusterStartupRule() {
    super();
  }

  public RedisClusterStartupRule(int numVMs) {
    super(numVMs);
  }

  public MemberVM startRedisVM(int index, int... locatorPort) {
    return startServerVM(index, r -> withRedis(r)
        .withConnectionToLocator(locatorPort)
        .withSystemProperty("enable-unsupported-commands", "true"));
  }


  public MemberVM startRedisVM(int index, String redisPort, int... locatorPort) {
    return startServerVM(index, r -> withRedis(r, redisPort)
        .withConnectionToLocator(locatorPort));
  }


  public MemberVM startRedisVM(int index, Properties properties, int... locatorPort) {
    return startServerVM(index, x -> withRedis(x)
        .withProperties(properties)
        .withConnectionToLocator(locatorPort));
  }

  public MemberVM startRedisVM(int index, SerializableFunction<ServerStarterRule> ruleOperator) {
    return startServerVM(index, x -> ruleOperator.apply(withRedis(x)));
  }

  private ServerStarterRule withRedis(ServerStarterRule rule) {
    return rule.withProperty(REDIS_BIND_ADDRESS, BIND_ADDRESS)
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(GeodeRedisServer.ENABLE_UNSUPPORTED_COMMANDS_PARAM,
            "true");
  }

  private ServerStarterRule withRedis(ServerStarterRule rule, String redisPort) {
    return rule.withProperty(REDIS_BIND_ADDRESS, BIND_ADDRESS)
        .withProperty(REDIS_PORT, redisPort)
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(GeodeRedisServer.ENABLE_UNSUPPORTED_COMMANDS_PARAM,
            "true");
  }

  public int getRedisPort(int vmNumber) {
    return getRedisPort(getMember(vmNumber));
  }

  public int getRedisPort(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getRedisServer().getPort();
    });
  }

  public void setEnableUnsupported(MemberVM vm, boolean enableUnsupported) {
    vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      service.getRedisServer().setAllowUnsupportedCommands(enableUnsupported);
    });
  }

  public Long getDataStoreBytesInUseForDataRegion(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getRedisServer().getDataStoreBytesInUseForDataRegion();
    });
  }

  public void flushAll() {
    flushAll(getRedisPort(1));
  }

  private void flushAll(int redisPort) {
    ClusterNodes nodes;
    try (Jedis jedis = new Jedis(BIND_ADDRESS, redisPort, REDIS_CLIENT_TIMEOUT)) {
      nodes = ClusterNodes.parseClusterNodes(jedis.clusterNodes());
    }

    for (ClusterNode node : nodes.getNodes()) {
      if (!node.primary) {
        continue;
      }
      try (Jedis jedis = new Jedis(node.ipAddress, (int) node.port, REDIS_CLIENT_TIMEOUT)) {
        jedis.flushAll();
      }
    }
  }

  /**
   * Given a key, return the {@link RedisMemberInfo} of the member serving that key. This call
   * assumes that VM 1 is a Radish server.
   */
  public RedisMemberInfo getMemberInfo(String key) {
    return getMember(1).invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getRedisServer().getMemberInfo(key);
    });
  }

  public void enableDebugLogging(int vmId) {
    getMember(vmId).invoke("Set logging level to DEBUG", () -> {
      Logger logger = LogManager.getLogger("org.apache.geode.redis.internal");
      Configurator.setAllLevels(logger.getName(), Level.getLevel("DEBUG"));
      FastLogger.setDelegating(true);
    });
  }

  /**
   * Assuming a redundancy of 1, and at least 3 members, move the given key's primary bucket to a
   * non-hosting member.
   */
  public DistributedMember moveBucketForKey(String key) {
    return moveBucketForKey(key, null);
  }

  /**
   * Explicitly move the primary bucket to a specific server identified by the server name.
   */
  public DistributedMember moveBucketForKey(String key, String targetServerName) {
    return getMember(1).invoke("moveBucketForKey: " + key + " -> " + targetServerName,
        () -> {
          Region<RedisKey, RedisData> r = RedisClusterStartupRule.getCache()
              .getRegion(RegionProvider.REDIS_DATA_REGION);

          RedisKey redisKey = new RedisKey(key.getBytes());
          DistributedMember primaryMember =
              PartitionRegionHelper.getPrimaryMemberForKey(r, redisKey);
          Set<DistributedMember> allHosting =
              PartitionRegionHelper.getAllMembersForKey(r, redisKey);

          // Returns all members, except the one calling.
          Set<DistributedMember> allMembers = getCache().getMembers(r);
          allMembers.add(getCache().getDistributedSystem().getDistributedMember());

          DistributedMember targetMember;
          if (targetServerName == null) {
            allMembers.removeAll(allHosting);
            targetMember = allMembers.stream().findFirst().orElseThrow(
                () -> new IllegalStateException("No non-hosting member found for key: " + key));
          } else {
            targetMember = allMembers.stream().filter(m -> m.getName().equals(targetServerName))
                .findFirst().orElseThrow(
                    () -> new IllegalStateException(
                        "Could not find member with name: " + targetServerName));
          }

          try {
            PartitionRegionHelper.moveBucketByKey(r, primaryMember, targetMember, redisKey);
          } catch (IllegalStateException e) {
            if (targetServerName == null || !e.getMessage().contains("is already hosting")) {
              throw e;
            }
          }

          // Who is the primary now?
          return PartitionRegionHelper.getPrimaryMemberForKey(r, redisKey);
        });
  }

  /**
   * Return some key of the form {@code prefix<N>}, (where {@code N} is an integer), for which the
   * given VM is the primary bucket holder. This is useful in tests where one needs to ensure that
   * a given key would be hosted on a given server.
   */
  public String getKeyOnServer(String keyPrefix, int vmId) {
    return getMember(1).invoke("getKeyOnServer", () -> {
      Region<RedisKey, RedisData> r = RedisClusterStartupRule.getCache()
          .getRegion(RegionProvider.REDIS_DATA_REGION);

      String server = "server-" + vmId;
      String key;
      int i = 0;
      while (true) {
        key = keyPrefix + i;
        DistributedMember primaryMember =
            PartitionRegionHelper.getPrimaryMemberForKey(r, new RedisKey(key.getBytes()));
        if (primaryMember.getName().equals(server)) {
          return key;
        }
        i++;
      }
    });
  }

}
