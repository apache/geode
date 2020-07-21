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

import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RedisClusterStartupRule extends ClusterStartupRule {

  public RedisClusterStartupRule() {
    super();
  }

  public RedisClusterStartupRule(int numVMs) {
    super(numVMs);
  }

  public MemberVM startRedisVM(int index, int... locatorPort) {
    return startServerVM(index, r -> withRedis(r)
        .withConnectionToLocator(locatorPort));
  }

  public MemberVM startRedisVM(int index, Properties properties, int... locatorPort) {
    return startServerVM(index, x -> withRedis(x)
        .withProperties(properties)
        .withConnectionToLocator(locatorPort));
  }

  private ServerStarterRule withRedis(ServerStarterRule rule) {
    return rule.withProperty(REDIS_BIND_ADDRESS, "localhost")
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(GeodeRedisServer.ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM,
            "true");
  }

  public int getRedisPort(int vmNumber) {
    return getRedisPort(getMember(vmNumber));
  }

  public int getRedisPort(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getPort();
    });
  }

  public void setEnableUnsupported(MemberVM vm, boolean enableUnsupported) {
    vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      service.setEnableUnsupported(enableUnsupported);
    });
  }
}
