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
package org.apache.geode.client.sni;

import static com.palantir.docker.compose.execution.DockerComposeExecArgument.arguments;
import static com.palantir.docker.compose.execution.DockerComposeExecOption.options;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import com.palantir.docker.compose.DockerComposeRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;
import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

public class ClientSNIAcceptanceTest {

  private static final URL DOCKER_COMPOSE_PATH =
      ClientSNIAcceptanceTest.class.getResource("docker-compose.yml");

  // Docker compose does not work on windows in CI. Ignore this test on windows
  // Using a RuleChain to make sure we ignore the test before the rule comes into play
  @ClassRule
  public static TestRule ignoreOnWindowsRule = new IgnoreOnWindowsRule();

  @Rule
  public DockerComposeRule docker = DockerComposeRule.builder()
      .file(DOCKER_COMPOSE_PATH.getPath())
      .build();


  private String trustStorePath;

  @Before
  public void before() throws IOException, InterruptedException {
    trustStorePath =
        createTempFileFromResource(ClientSNIAcceptanceTest.class,
            "geode-config/truststore.jks")
                .getAbsolutePath();
    docker.exec(options("-T"), "geode",
        arguments("gfsh", "run", "--file=/geode/scripts/geode-starter.gfsh"));
  }

  @Test
  public void connectToSNIProxyDocker() {
    Properties gemFireProps = new Properties();
    gemFireProps.setProperty(SSL_ENABLED_COMPONENTS, "all");
    gemFireProps.setProperty(SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");

    gemFireProps.setProperty(SSL_TRUSTSTORE, trustStorePath);
    gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");
    gemFireProps.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");

    int proxyPort = docker.containers()
        .container("haproxy")
        .port(15443)
        .getExternalPort();
    ClientCache cache = new ClientCacheFactory(gemFireProps)
        .addPoolLocator("locator", 10334)
        .setPoolSocketFactory(ProxySocketFactories.sni("localhost",
            proxyPort))
        .create();
    Region<String, String> region =
        cache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create("jellyfish");
    region.destroy("hello");
    region.put("hello", "world");
    assertThat(region.get("hello")).isEqualTo("world");
  }
}
