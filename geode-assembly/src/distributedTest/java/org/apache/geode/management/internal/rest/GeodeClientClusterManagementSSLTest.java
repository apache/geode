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

package org.apache.geode.management.internal.rest;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.management.builder.ClusterManagementServiceBuilder.buildWithCache;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class GeodeClientClusterManagementSSLTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(2);

  private static MemberVM locator;

  private static ClientVM client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    File keyFile = new File(ClientClusterManagementSSLTest.class.getClassLoader()
        .getResource("ssl/trusted.keystore").getFile());
    Properties sslProps = new Properties();
    sslProps.setProperty(SSL_KEYSTORE, keyFile.getCanonicalPath());
    sslProps.setProperty(SSL_TRUSTSTORE, keyFile.getCanonicalPath());
    sslProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    sslProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    sslProps.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.ALL.getConstant());

    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withProperties(sslProps));

    int port = locator.getPort();
    client = cluster.startClientVM(1, cf -> cf.withLocatorConnection(port)
        .withProperties(sslProps));
  }

  @Test
  public void getServiceUseClientSSLConfig() throws Exception {
    client.invoke(() -> {
      await().untilAsserted(() -> {
        ClusterManagementService service = buildWithCache()
            .setCache(ClusterStartupRule.getClientCache()).build();
        assertThat(service.isConnected()).isTrue();
      });
    });
  }
}
