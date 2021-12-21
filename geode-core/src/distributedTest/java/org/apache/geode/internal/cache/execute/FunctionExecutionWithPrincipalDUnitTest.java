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

package org.apache.geode.internal.cache.execute;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.security.TestFunctions.ReadFunction;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.SerializableFunction;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class FunctionExecutionWithPrincipalDUnitTest {

  private static final String PR_REGION_NAME = "partitioned-region";
  private static final String REGION_NAME = "replicated-region";
  private static Region<String, String> replicateRegion;
  private static Region<String, String> partitionedRegion;

  private static final Function<?> readFunction = new ReadFunction();

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static ClientCache client;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static ClientCacheRule clientRule = new ClientCacheRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0, x -> x
        .withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();

    SerializableFunction<ServerStarterRule> startupFunction = x -> x
        .withConnectionToLocator(locatorPort)
        .withCredential("cluster", "cluster")
        .withProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
            "org.apache.geode.management.internal.security.TestFunctions*");

    server1 = cluster.startServerVM(1, startupFunction);
    server2 = cluster.startServerVM(2, startupFunction);

    Stream.of(server1, server2).forEach(v -> v.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(
          REGION_NAME);
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
          .create(PR_REGION_NAME);
    }));

    client = clientRule
        .withLocatorConnection(locatorPort)
        .withCredential("data", "data")
        .createCache();

    replicateRegion = clientRule.createProxyRegion(REGION_NAME);
    partitionedRegion = clientRule.createProxyRegion(PR_REGION_NAME);

    for (int i = 0; i < 10; i++) {
      replicateRegion.put("key-" + i, "value-" + i);
      partitionedRegion.put("key-" + i, "value-" + i);
    }
  }

  @Test
  public void verifyPrincipal_whenUsingReplicateRegion_andCallingOnRegion() {
    FunctionService.onRegion(replicateRegion)
        .execute(readFunction)
        .getResult();
  }

  @Test
  public void verifyPrincipal_whenUsingPartitionedRegion_andCallingOnRegion() {
    FunctionService.onRegion(partitionedRegion)
        .execute(readFunction)
        .getResult();
  }

  @Test
  public void verifyPrincipal_whenUsingPartitionedRegion_andCallingOnRegion_withFilter() {
    Set<String> filter = new HashSet<>();
    filter.add("key-1");
    filter.add("key-2");
    filter.add("key-4");
    filter.add("key-7");

    FunctionService.onRegion(partitionedRegion)
        .withFilter(filter)
        .execute(readFunction)
        .getResult();
  }

  @Test
  public void verifyPrincipal_whenUsingPartitionedRegion_andCallingOnServer() {
    FunctionService.onServer(partitionedRegion.getRegionService())
        .execute(readFunction)
        .getResult();
  }

  @Test
  public void verifyPrincipal_whenUsingPartitionedRegion_andCallingOnServers() {
    FunctionService.onServers(partitionedRegion.getRegionService())
        .execute(readFunction)
        .getResult();
  }

  @Test
  public void verifyPrincipal_whenUsingReplicateRegion_andCallingOnServers() {
    FunctionService.onServers(replicateRegion.getRegionService())
        .execute(readFunction)
        .getResult();
  }

}
