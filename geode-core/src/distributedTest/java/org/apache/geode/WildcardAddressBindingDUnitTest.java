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
package org.apache.geode;

import java.util.Properties;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class WildcardAddressBindingDUnitTest {

  private static MemberVM locator, server1, server2;
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @After
  public void stopCluster() {
    cluster.stop(2);
    cluster.stop(1);
    cluster.stop(0);
  }

  private void startCluster(Properties gemfireProperties) {
    locator = cluster.startLocatorVM(0, gemfireProperties);
    server1 = cluster.startServerVM(1, gemfireProperties, locator.getPort());
    server2 = cluster.startServerVM(2, gemfireProperties, locator.getPort());
  }

  @Test
  public void startClusterUsingWildcardIp() {
    Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(ConfigurationProperties.BIND_ADDRESS, "0.0.0.0");
    startCluster(gemfireProperties);
  }

  @Test
  public void startClusterUsingWildcardCharacter() {
    Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(ConfigurationProperties.BIND_ADDRESS, "*");
    startCluster(gemfireProperties);
  }
}
