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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.execution.DockerComposeRunArgument;
import com.palantir.docker.compose.execution.DockerComposeRunOption;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.NoAvailableLocatorsException;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;

public class ClientSNIDropProxyAcceptanceTest {

  private static final URL DOCKER_COMPOSE_PATH =
      ClientSNIDropProxyAcceptanceTest.class.getResource("docker-compose.yml");

  // Docker compose does not work on windows in CI. Ignore this test on windows
  // Using a RuleChain to make sure we ignore the test before the rule comes into play
  @ClassRule
  public static NotOnWindowsDockerRule docker =
      new NotOnWindowsDockerRule(() -> DockerComposeRule.builder()
          .file(DOCKER_COMPOSE_PATH.getPath()).build());

  private ClientCache cache;

  private String trustStorePath;
  private int proxyPort;

  @Before
  public void before() throws IOException, InterruptedException {
    trustStorePath =
        createTempFileFromResource(ClientSNIDropProxyAcceptanceTest.class,
            "geode-config/truststore.jks")
                .getAbsolutePath();
    docker.get().exec(options("-T"), "geode",
        arguments("gfsh", "run", "--file=/geode/scripts/geode-starter.gfsh"));
  }

  @After
  public void after() {
    ensureCacheClosed();
  }

  @Test
  public void performSimpleOperationsDropSNIProxy()
      throws IOException,
      InterruptedException {
    final Region<String, Integer> region = getRegion();

    region.put("Roy Hobbs", 9);
    assertThat(region.get("Roy Hobbs")).isEqualTo(9);

    stopProxy();

    assertThatThrownBy(() -> region.get("Roy Hobbs"))
        .isInstanceOf(NoAvailableLocatorsException.class)
        .hasMessageContaining("Unable to connect to any locators in the list");


    restartProxy();

    await().untilAsserted(() -> assertThat(region.get("Roy Hobbs")).isEqualTo(9));

    region.put("Bennie Rodriquez", 30);
    assertThat(region.get("Bennie Rodriquez")).isEqualTo(30);

    region.put("Jake Taylor", 7);
    region.put("Crash Davis", 8);

    region.put("Ricky Vaughn", 99);
    region.put("Ebbie Calvin LaLoosh", 37);

  }

  private void stopProxy() throws IOException, InterruptedException {
    docker.get().containers()
        .container("haproxy")
        .stop();
  }

  private void restartProxy() throws IOException, InterruptedException {
    restartProxyOnPreviousPort();
    // Leave this commented here in case you need it for troubleshooting
    // restartProxyOnDockerComposePort();
  }

  /**
   * Use this variant to (re)start the container on whatever port(s) is specified in
   * docker-compose.yml. Usually that would look something like:
   *
   * ports:
   * - "15443:15443"
   *
   * Leave this unused method here for troubleshooting.
   */
  private void restartProxyOnDockerComposePort() throws IOException, InterruptedException {
    docker.get().containers()
        .container("haproxy")
        .start();
  }

  /**
   * Use this variant to (re)start the container whatever host port it was bound to before
   * it was stopped. Usually you'll want the ports spec in docker-compose.yml to look like
   * this when using this method (allowing Docker to initially choose a random host port
   * to bind to):
   *
   * ports:
   * - "15443"
   */
  private void restartProxyOnPreviousPort() throws IOException, InterruptedException {
    /*
     * docker-compose run needs -d to daemonize the container (fork the process and return control
     * to this process). The first time we ran the HAproxy container, we let it pick the host port
     * to bind on. This time, we want it to bind to that same host port (proxyPort). The syntax
     * for the --publish argument is <host-port>:<internal-port> in this case.
     */
    docker.get().run(
        DockerComposeRunOption.options("-d", "--publish", String.format("%d:15443", proxyPort)),
        "haproxy",
        DockerComposeRunArgument.arguments("haproxy", "-f", "/usr/local/etc/haproxy/haproxy.cfg"));
  }

  public Region<String, Integer> getRegion() {
    Properties gemFireProps = new Properties();
    gemFireProps.setProperty(SSL_ENABLED_COMPONENTS, "all");
    gemFireProps.setProperty(SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");

    gemFireProps.setProperty(SSL_TRUSTSTORE, trustStorePath);
    gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");
    gemFireProps.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");

    proxyPort = docker.get().containers()
        .container("haproxy")
        .port(15443)
        .getExternalPort();

    ensureCacheClosed();

    cache = new ClientCacheFactory(gemFireProps)
        .addPoolLocator("locator-maeve", 10334)
        .setPoolSocketFactory(ProxySocketFactories.sni("localhost",
            proxyPort))
        .setPoolSubscriptionEnabled(true)
        .create();
    return (Region<String, Integer>) cache.<String, Integer>createClientRegionFactory(
        ClientRegionShortcut.PROXY)
        .create("jellyfish");
  }

  /**
   * modifies cache field as a side-effect
   */
  private void ensureCacheClosed() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
  }

}
