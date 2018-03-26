package org.apache.geode.cache.client.internal;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.distributed.Locator;
import org.apache.geode.util.test.TestUtil;

public class LocatorSSLJUnitTest {
  private final String SERVER_KEY_STORE = TestUtil.getResourcePath(LocatorSSLJUnitTest.class, "cacheserver.keystore");
  private final String SERVER_TRUST_STORE = TestUtil.getResourcePath(LocatorSSLJUnitTest.class, "cacheserver.truststore");

  @Test
  public void canStopLocatorWithSSL() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.put(SSL_ENABLED_COMPONENTS, "all");
    properties.put(SSL_KEYSTORE_TYPE, "jks");
    properties.put(SSL_KEYSTORE, SERVER_KEY_STORE);
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_TRUSTSTORE, SERVER_TRUST_STORE);
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");

    Locator locator = Locator.startLocatorAndDS(0, null, properties);
    locator.stop();
  }
}
