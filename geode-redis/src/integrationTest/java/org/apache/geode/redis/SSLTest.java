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

package org.apache.geode.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.RedisTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({RedisTest.class})
public class SSLTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @ClassRule
  public static ServerStarterRule sslEnabledServer = new ServerStarterRule()
      .withSSL("server", true, true)
      .withProperty("redis-bind-address", "localhost")
      .withProperty("redis-port", "11211")
      .withAutoStart();


  @Test
  public void canConnectOverSSL() {
    SSLConfig sslConfigForComponent = SSLConfigurationFactory.getSSLConfigForComponent(
        ((InternalDistributedSystem) sslEnabledServer.getCache().getDistributedSystem())
            .getConfig(),
        SecurableCommunicationChannel.SERVER);

    System.setProperty("javax.net.ssl.trustStore", sslConfigForComponent.getTruststore());
    System.setProperty("javax.net.ssl.trustStoreType", "JKS");

    Jedis localhost = new Jedis("localhost", 11211, true);

    assertThat(localhost.ping()).isEqualTo("PONG");
  }

  @Test
  public void cannotConnectOverCleartext() {
    SSLConfig sslConfigForComponent = SSLConfigurationFactory.getSSLConfigForComponent(
        ((InternalDistributedSystem) sslEnabledServer.getCache().getDistributedSystem())
            .getConfig(),
        SecurableCommunicationChannel.ALL);

    System.setProperty("javax.net.ssl.trustStore", sslConfigForComponent.getTruststore());
    System.setProperty("javax.net.ssl.trustStoreType", "JKS");

    Jedis localhost = new Jedis("localhost", 11211, false);

    assertThatThrownBy(() -> localhost.ping());
  }

}
