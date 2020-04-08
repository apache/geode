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
import org.junit.After;
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

public class DualServerSNIAcceptanceTest {

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

  private Properties gemFireProps;
  private ClientCache cache;

  @Before
  public void before() throws IOException, InterruptedException {
    docker.exec(options("-T"), "geode",
        arguments("gfsh", "run", "--file=/geode/scripts/geode-starter-2.gfsh"));

    final String trustStorePath =
        createTempFileFromResource(ClientSNIAcceptanceTest.class,
            "geode-config/truststore.jks")
                .getAbsolutePath();

    gemFireProps = new Properties();
    gemFireProps.setProperty(SSL_ENABLED_COMPONENTS, "all");
    gemFireProps.setProperty(SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");

    gemFireProps.setProperty(SSL_TRUSTSTORE, trustStorePath);
    gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");
    gemFireProps.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
  }


  @After
  public void after() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
  }

  @Test
  public void dualServerTest() {
    verifyPutAndGet("group-dolores", "region-dolores");
  }

  @Test
  public void dualServerTest2() {
    verifyPutAndGet("group-clementine", "region-clementine");
  }

  private void verifyPutAndGet(final String groupName, final String regionName) {
    int proxyPort = docker.containers()
        .container("haproxy")
        .port(15443)
        .getExternalPort();
    cache = new ClientCacheFactory(gemFireProps)
        .addPoolLocator("locator-maeve", 10334)
        .setPoolServerGroup(groupName)
        .setPoolSocketFactory(ProxySocketFactories.sni("localhost",
            proxyPort))
        .create();
    Region<String, String> region =
        cache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(regionName);
    region.destroy("hello");
    region.put("hello", "world");
    assertThat(region.get("hello")).isEqualTo("world");
  }

}
