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
package org.apache.geode.internal.net;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.security.AuthInitialize.SECURITY_PASSWORD;
import static org.apache.geode.security.AuthInitialize.SECURITY_USERNAME;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;

public class SocketCreatorWithSNIProxyTest {

  /*
   * This SNI proxy is not suitable for CI tests. It's for developer testing only.
   */
  @Before
  public void before() {
    System.setProperty(GEMFIRE_PREFIX + "security.sni-proxy",
        "sni-proxy.moscow.cf-app.com:15443");
  }

  /*
   * This test is ignored because it's connecting to a private toolsmith's BOSH environment.
   *
   * To connect to this env, built on a Cloud Cache built on GemFire 1.10, you have to change
   * CURRENT to GEODE_1_10_0 in Version.java
   */
  @Ignore
  @Test
  public void connectToSNIProxy() {
    Properties gemFireProps = new Properties();
    gemFireProps.setProperty(SSL_ENABLED_COMPONENTS, "all");
    gemFireProps.setProperty(SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(SECURITY_CLIENT_AUTH_INIT,
        "org.apache.geode.internal.net.ClientAuthInitialize");
    gemFireProps.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");
    gemFireProps.setProperty(SECURITY_USERNAME, "developer_jBQc5PubzGGdeuM7gXiF8w");
    gemFireProps.setProperty(SECURITY_PASSWORD, "sFGM5tWwJbAZNTK4THFw");

    gemFireProps.setProperty(SSL_KEYSTORE,
        "/Users/bburcham/workspace/pks-networking-env-metadata/Moscow/keystore.jks");
    gemFireProps.setProperty(SSL_KEYSTORE_PASSWORD, "NlKt3rK8evPQO3vVIX0YnHF8lKJhzw");
    gemFireProps.setProperty(SSL_TRUSTSTORE,
        "/Users/bburcham/workspace/pks-networking-env-metadata/Moscow/truststore.jks");
    gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "HcXLHnBYCxxmr3bE0VwCMy6sTyLCYV");

    ClientCache cache = new ClientCacheFactory(gemFireProps)
        .addPoolLocator(
            "2e6683b4-cd40-4d66-90a1-4c13e8f1f31c.locator.moscow-services-subnet.service-instance-880592c5-f4a6-4fd3-8390-1ac7362d8dd7.bosh",
            55221)
        .create();
    Region<String, String> region =
        cache.<String, String>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create("jellyfish");
    region.put("hello", "world");
  }
}
