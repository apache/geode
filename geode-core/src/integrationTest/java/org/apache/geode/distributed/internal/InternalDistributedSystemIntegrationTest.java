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

package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS_PROPERTY;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Properties;
import java.util.logging.Level;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.ReadsSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.Config;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.metrics.internal.MetricsService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Tests the functionality of the {@link InternalDistributedSystem} class. Mostly checks
 * configuration error checking.
 *
 * @since GemFire 2.1
 */
@Tag("membership")
public class InternalDistributedSystemIntegrationTest {

  /**
   * A connection to a distributed system created by this test
   */
  private InternalDistributedSystem system;

  private InternalDistributedSystem createSystem(Properties props,
      MetricsService.Builder metricsSessionBuilder) {
    system = new InternalDistributedSystem.Builder(props, metricsSessionBuilder)
        .build();
    return system;
  }

  /**
   * Creates a <code>DistributedSystem</code> with the given configuration properties.
   */
  private InternalDistributedSystem createSystem(Properties props) {
    MetricsService.Builder metricsSessionBuilder = mock(MetricsService.Builder.class);
    when(metricsSessionBuilder.build(any())).thenReturn(mock(MetricsService.class));
    return createSystem(props, metricsSessionBuilder);
  }

  /**
   * Disconnects any distributed system that was created by this test
   *
   * @see DistributedSystem#disconnect
   */
  @AfterEach
  public void tearDown() throws Exception {
    if (system != null) {
      system.disconnect();
    }
  }

  @Test
  public void testUnknownArgument() {
    Properties props = new Properties();
    props.put("UNKNOWN", "UNKNOWN");

    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Tests that the default values of properties are what we expect
   */
  @Test
  @ClearSystemProperty(key = "AvailablePort.lowerBound")
  @ClearSystemProperty(key = "AvailablePort.upperBound")
  @ClearSystemProperty(key = "gemfire.membership-port-range")
  @ReadsSystemProperty
  public void testDefaultProperties() {
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributionConfig config = createSystem(props).getConfig();

    assertThat(DistributionConfig.DEFAULT_NAME).isEqualTo(config.getName());

    assertThat(0).isEqualTo(config.getMcastPort());

    assertThat(DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[0])
        .isEqualTo(config.getMembershipPortRange()[0]);
    assertThat(DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[1])
        .isEqualTo(config.getMembershipPortRange()[1]);

    if (System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "mcast-address") == null) {
      assertThat(DistributionConfig.DEFAULT_MCAST_ADDRESS).isEqualTo(config.getMcastAddress());
    }
    if (System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "bind-address") == null) {
      assertThat(DistributionConfig.DEFAULT_BIND_ADDRESS).isEqualTo(config.getBindAddress());
    }

    assertThat(DistributionConfig.DEFAULT_LOG_FILE).isEqualTo(config.getLogFile());

    // default log level gets overrided by the gemfire.properties created for unit tests.
    // assertIndexDetailsEquals(DistributionConfig.DEFAULT_LOG_LEVEL, config.getLogLevel());

    assertThat(DistributionConfig.DEFAULT_STATISTIC_SAMPLING_ENABLED)
        .isEqualTo(config.getStatisticSamplingEnabled());

    assertThat(DistributionConfig.DEFAULT_STATISTIC_SAMPLE_RATE)
        .isEqualTo(config.getStatisticSampleRate());

    assertThat(DistributionConfig.DEFAULT_STATISTIC_ARCHIVE_FILE)
        .isEqualTo(config.getStatisticArchiveFile());

    // ack-wait-threadshold is overridden on VM's command line using a
    // system property. This is not a valid test. Hrm.
    // assertIndexDetailsEquals(DistributionConfig.DEFAULT_ACK_WAIT_THRESHOLD,
    // config.getAckWaitThreshold());

    assertThat(DistributionConfig.DEFAULT_ACK_SEVERE_ALERT_THRESHOLD)
        .isEqualTo(config.getAckSevereAlertThreshold());

    assertThat(DistributionConfig.DEFAULT_CACHE_XML_FILE).isEqualTo(config.getCacheXmlFile());

    assertThat(DistributionConfig.DEFAULT_ARCHIVE_DISK_SPACE_LIMIT)
        .isEqualTo(config.getArchiveDiskSpaceLimit());
    assertThat(DistributionConfig.DEFAULT_ARCHIVE_FILE_SIZE_LIMIT)
        .isEqualTo(config.getArchiveFileSizeLimit());
    assertThat(DistributionConfig.DEFAULT_LOG_DISK_SPACE_LIMIT)
        .isEqualTo(config.getLogDiskSpaceLimit());
    assertThat(DistributionConfig.DEFAULT_LOG_FILE_SIZE_LIMIT)
        .isEqualTo(config.getLogFileSizeLimit());

    assertThat(DistributionConfig.DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION)
        .isEqualTo(config.getEnableNetworkPartitionDetection());
  }

  @Test
  public void testGetName() {
    String name = "testGetName";

    Properties props = new Properties();
    props.put(NAME, name);
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    DistributionConfig config = createSystem(props).getOriginalConfig();
    assertThat(name).isEqualTo(config.getName());
  }

  @Test
  public void testMemberTimeout() {
    Properties props = new Properties();
    int memberTimeout = 100;
    props.put(MEMBER_TIMEOUT, String.valueOf(memberTimeout));
    props.put(MCAST_PORT, "0");

    DistributionConfig config = createSystem(props).getOriginalConfig();
    assertThat(memberTimeout).isEqualTo(config.getMemberTimeout());
  }

  @Test
  public void testMalformedLocators() {
    Properties props = new Properties();

    // Totally bogus locator
    props.put(LOCATORS, "14lasfk^5234");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);

    // missing port
    props.put(LOCATORS, "localhost[");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);

    // Missing ]
    props.put(LOCATORS, "localhost[234ty");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);

    // Malformed port
    props.put(LOCATORS, "localhost[234ty]");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);

    // Malformed port in second locator
    props.put(LOCATORS, "localhost[12345],localhost[sdf3");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Creates a new <code>DistributionConfigImpl</code> with the given locators string.
   *
   * @throws IllegalArgumentException If <code>locators</code> is malformed
   * @since GemFire 4.0
   */
  private void checkLocator(String locator) {
    Properties props = new Properties();
    props.put(LOCATORS, locator);
    new DistributionConfigImpl(props);
  }

  /**
   * Tests that both the traditional syntax ("host[port]") and post bug-32306 syntax ("host:port")
   * can be used with locators.
   *
   * @since GemFire 4.0
   */
  @Test
  public void testLocatorSyntax() throws Exception {
    String localhost = java.net.InetAddress.getLocalHost().getCanonicalHostName();
    checkLocator(localhost + "[12345]");
    checkLocator(localhost + ":12345");

    String bindAddress = getHostAddress(java.net.InetAddress.getLocalHost());
    if (bindAddress.indexOf(':') < 0) {
      checkLocator(localhost + ":" + bindAddress + "[12345]");
    }
    checkLocator(localhost + "@" + bindAddress + "[12345]");
    if (bindAddress.indexOf(':') < 0) {
      checkLocator(localhost + ":" + bindAddress + ":12345");
    }
    if (localhost.indexOf(':') < 0) {
      checkLocator(localhost + ":" + "12345");
    }
  }

  /**
   * Tests that getting the log level is what we expect.
   */
  @Test
  public void testGetLogLevel() {
    Level logLevel = Level.FINER;
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(LOG_LEVEL, logLevel.toString());

    DistributionConfig config = createSystem(props).getConfig();
    assertThat(logLevel.intValue()).isEqualTo(config.getLogLevel());
  }

  @Test
  public void testInvalidLogLevel() {
    Properties props = new Properties();
    props.put(LOG_LEVEL, "blah blah blah");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testGetStatisticSamplingEnabled() {
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(STATISTIC_SAMPLING_ENABLED, "true");
    DistributionConfig config = createSystem(props).getConfig();
    assertThat(config.getStatisticSamplingEnabled()).isTrue();
  }

  @Test
  public void testGetStatisticSampleRate() {
    String rate = String.valueOf(DistributionConfig.MIN_STATISTIC_SAMPLE_RATE);
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(STATISTIC_SAMPLE_RATE, rate);
    DistributionConfig config = createSystem(props).getConfig();
    // The fix for 48228 causes the rate to be 1000 even if we try to set it less
    assertThat(1000).isEqualTo(config.getStatisticSampleRate());
  }

  @Test
  @ClearSystemProperty(key = "AvailablePort.lowerBound")
  @ClearSystemProperty(key = "AvailablePort.upperBound")
  @ClearSystemProperty(key = "gemfire.membership-port-range")
  public void testMembershipPortRange() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(MEMBERSHIP_PORT_RANGE, "45100-45200");
    DistributionConfig config = createSystem(props).getConfig();
    assertThat(45100).isEqualTo(config.getMembershipPortRange()[0]);
    assertThat(45200).isEqualTo(config.getMembershipPortRange()[1]);
  }

  @Test
  @ClearSystemProperty(key = "AvailablePort.lowerBound")
  @ClearSystemProperty(key = "AvailablePort.upperBound")
  @ClearSystemProperty(key = "gemfire.membership-port-range")
  public void testBadMembershipPortRange() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(MEMBERSHIP_PORT_RANGE, "5200-5100");
    assertThatThrownBy(() -> createSystem(props).getConfig())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testGetStatisticArchiveFile() {
    String fileName = "testGetStatisticArchiveFile";
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(STATISTIC_ARCHIVE_FILE, fileName);
    DistributionConfig config = createSystem(props).getConfig();
    assertThat(fileName).isEqualTo(config.getStatisticArchiveFile().getName());
  }

  @Test
  public void testGetCacheXmlFile() {
    String fileName = "blah";
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(CACHE_XML_FILE, fileName);
    DistributionConfig config = createSystem(props).getConfig();
    assertThat(fileName).isEqualTo(config.getCacheXmlFile().getPath());
  }

  @Test
  public void testGetArchiveDiskSpaceLimit() {
    String value = String.valueOf(DistributionConfig.MIN_ARCHIVE_DISK_SPACE_LIMIT);
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(ARCHIVE_DISK_SPACE_LIMIT, value);
    DistributionConfig config = createSystem(props).getConfig();
    assertThat(Integer.parseInt(value)).isEqualTo(config.getArchiveDiskSpaceLimit());
  }

  @Test
  public void testInvalidArchiveDiskSpaceLimit() {
    Properties props = new Properties();
    props.put(ARCHIVE_DISK_SPACE_LIMIT, "blah");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testGetArchiveFileSizeLimit() {
    String value = String.valueOf(DistributionConfig.MIN_ARCHIVE_FILE_SIZE_LIMIT);
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(ARCHIVE_FILE_SIZE_LIMIT, value);
    DistributionConfig config = createSystem(props).getConfig();
    assertThat(Integer.parseInt(value)).isEqualTo(config.getArchiveFileSizeLimit());
  }

  @Test
  public void testInvalidArchiveFileSizeLimit() {
    Properties props = new Properties();
    props.put(ARCHIVE_FILE_SIZE_LIMIT, "blah");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testGetLogDiskSpaceLimit() {
    String value = String.valueOf(DistributionConfig.MIN_LOG_DISK_SPACE_LIMIT);
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(LOG_DISK_SPACE_LIMIT, value);
    DistributionConfig config = createSystem(props).getConfig();
    assertThat(Integer.parseInt(value)).isEqualTo(config.getLogDiskSpaceLimit());
  }

  @Test
  public void testInvalidLogDiskSpaceLimit() {
    Properties props = new Properties();
    props.put(LOG_DISK_SPACE_LIMIT, "blah");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testGetLogFileSizeLimit() {
    String value = String.valueOf(DistributionConfig.MIN_LOG_FILE_SIZE_LIMIT);
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(LOG_FILE_SIZE_LIMIT, value);
    DistributionConfig config = createSystem(props).getConfig();
    assertThat(Integer.parseInt(value)).isEqualTo(config.getLogFileSizeLimit());
  }

  @Test
  public void testInvalidLogFileSizeLimit() {
    Properties props = new Properties();
    props.put(LOG_FILE_SIZE_LIMIT, "blah");
    assertThatThrownBy(() -> createSystem(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testAccessingClosedDistributedSystem() {
    Properties props = new Properties();

    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    InternalDistributedSystem system = createSystem(props);
    system.disconnect();

    assertThatThrownBy(system::getDistributionManager)
        .isInstanceOf(DistributedSystemDisconnectedException.class);
    system.getLogWriter();
  }

  @Test
  @ReadsSystemProperty
  @SetSystemProperty(key = GeodeGlossary.GEMFIRE_PREFIX + LOG_LEVEL, value = "finest")
  public void testPropertySources() throws Exception {
    // TODO: fix this test on Windows: the File renameTo and delete in finally fails on Windows
    String os = System.getProperty("os.name");
    if (os != null) {
      if (os.contains("Windows")) {
        return;
      }
    }
    File propFile = new File(GeodeGlossary.GEMFIRE_PREFIX + "properties");
    boolean propFileExisted = propFile.exists();
    File spropFile = new File("gfsecurity.properties");
    boolean spropFileExisted = spropFile.exists();
    try {
      Properties apiProps = new Properties();
      apiProps.setProperty(GROUPS, "foo, bar");
      {
        if (propFileExisted) {
          propFile.renameTo(new File(GeodeGlossary.GEMFIRE_PREFIX + "properties.sav"));
        }
        Properties fileProps = new Properties();
        fileProps.setProperty("name", "myName");
        FileWriter fw = new FileWriter(GeodeGlossary.GEMFIRE_PREFIX + "properties");
        fileProps.store(fw, null);
        fw.close();
      }
      {
        if (spropFileExisted) {
          spropFile.renameTo(new File("gfsecurity.properties.sav"));
        }
        Properties fileProps = new Properties();
        fileProps.setProperty(STATISTIC_SAMPLE_RATE, "999");
        FileWriter fw = new FileWriter("gfsecurity.properties");
        fileProps.store(fw, null);
        fw.close();
      }
      DistributionConfigImpl dci = new DistributionConfigImpl(apiProps);
      assertThat(dci.getAttributeSource(MCAST_PORT)).isNull();
      assertThat(ConfigSource.api()).isEqualTo(dci.getAttributeSource(GROUPS));
      assertThat(ConfigSource.sysprop()).isEqualTo(dci.getAttributeSource(LOG_LEVEL));
      assertThat(ConfigSource.Type.FILE).isEqualTo(dci.getAttributeSource("name").getType());
      assertThat(ConfigSource.Type.SECURE_FILE)
          .isEqualTo(dci.getAttributeSource(STATISTIC_SAMPLE_RATE).getType());
    } finally {
      propFile.delete();
      if (propFileExisted) {
        new File(GeodeGlossary.GEMFIRE_PREFIX + "properties.sav").renameTo(propFile);
      }
      spropFile.delete();
      if (spropFileExisted) {
        new File("gfsecurity.properties.sav").renameTo(spropFile);
      }
    }
  }

  /**
   * Create a <Code>DistributedSystem</code> with a non-default name.
   */
  @Test
  public void testNonDefaultConnectionName() {
    String name = "BLAH";
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(NAME, name);
    createSystem(props);
  }

  @Test
  public void testNonDefaultLogLevel() {
    Level level = Level.FINE;

    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(LOG_LEVEL, level.toString());
    InternalDistributedSystem system = createSystem(props);
    assertThat(level.intValue()).isEqualTo(system.getConfig().getLogLevel());
    assertThat(level.intValue())
        .isEqualTo(((InternalLogWriter) system.getLogWriter()).getLogWriterLevel());
  }

  @Test
  public void testStartLocator() {
    Properties props = new Properties();
    int unusedPort = getRandomAvailableTCPPort();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(START_LOCATOR, "localhost[" + unusedPort + "],server=false,peer=true");
    deleteStateFile(unusedPort);
    createSystem(props);
    Collection<Locator> locators = Locator.getLocators();
    assertThat(locators).hasSize(1);
    Locator locator = locators.iterator().next();
    // Assert.assertIndexDetailsEquals("127.0.0.1", locator.getBindAddress().getHostAddress());
    // removed this check for ipv6 testing
    assertThat(unusedPort).isEqualTo(locator.getPort());
    deleteStateFile(unusedPort);
  }

  private void deleteStateFile(int port) {
    File stateFile = new File("locator" + port + "state.dat");
    if (stateFile.exists()) {
      stateFile.delete();
    }
  }

  @Test
  public void testValidateProps() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    Config config1 = new DistributionConfigImpl(props, false);
    MetricsService.Builder metricsSessionBuilder = mock(MetricsService.Builder.class);
    when(metricsSessionBuilder.build(any())).thenReturn(mock(MetricsService.class));
    InternalDistributedSystem sys =
        new InternalDistributedSystem.Builder(config1.toProperties(), metricsSessionBuilder)
            .build();
    try {

      props.put(MCAST_PORT, "1");
      Config config2 = new DistributionConfigImpl(props, false);

      assertThatThrownBy(() -> sys.validateSameProperties(config2.toProperties(), true))
          .isInstanceOf(IllegalStateException.class);

    } finally {
      sys.disconnect();
    }
  }

  @Test
  public void testDeprecatedSSLProps() {
    // ssl-* props are copied to cluster-ssl-*.
    Properties props = getCommonProperties();
    props.setProperty(CLUSTER_SSL_ENABLED, "true");
    Config config1 = new DistributionConfigImpl(props, false);
    Properties props1 = config1.toProperties();
    assertThat("true").isEqualTo(props1.getProperty(CLUSTER_SSL_ENABLED));
    Config config2 = new DistributionConfigImpl(props1, false);
    assertThat(config1.sameAs(config2)).isTrue();
    Properties props3 = new Properties(props1);
    props3.setProperty(CLUSTER_SSL_ENABLED, "false");
    Config config3 = new DistributionConfigImpl(props3, false);
    assertThat(config1.sameAs(config3)).isFalse();
  }

  @Test
  public void testEmptySecurityAuthTokenProp() {
    Properties props = getCommonProperties();
    props.setProperty(SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS, "");
    DistributionConfig config1 = new DistributionConfigImpl(props, false);
    assertThat(config1.getSecurityAuthTokenEnabledComponents()).hasSize(0);
    Properties securityProps = config1.getSecurityProps();
    assertThat(securityProps.getProperty(SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS)).isEqualTo("");
    assertThat(config1.getSecurityAuthTokenEnabledComponents()).hasSize(0);
  }

  @Test
  public void testSecurityAuthTokenProp() {
    Properties props = getCommonProperties();
    props.setProperty(SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS, "management");
    DistributionConfig config1 = new DistributionConfigImpl(props, false);
    assertThat(config1.getSecurityAuthTokenEnabledComponents()).containsExactly("MANAGEMENT");
    Properties securityProps = config1.getSecurityProps();
    assertThat(securityProps.getProperty(SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS))
        .isEqualTo("management");
    assertThat(config1.getSecurityAuthTokenEnabledComponents()).containsExactly("MANAGEMENT");
  }

  @Test
  public void testSSLEnabledComponents() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "cluster,server");
    Config config1 = new DistributionConfigImpl(props, false);
    assertThat("cluster,server").isEqualTo(config1.getAttribute(SSL_ENABLED_COMPONENTS));
  }

  @Test()
  public void testSSLEnabledComponentsWrongComponentName() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "testing");
    assertThatThrownBy(() -> new DistributionConfigImpl(props, false))
        .withFailMessage("There is no registered component for the name: testing")
        .isInstanceOf(IllegalArgumentException.class);
  }


  @Test
  public void testSSLEnabledComponentsWithLegacyJMXSSLSettings() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "all");
    props.setProperty(JMX_MANAGER_SSL_ENABLED, "true");
    assertThatThrownBy(() -> new DistributionConfigImpl(props, false))
        .withFailMessage(
            "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testSSLEnabledComponentsWithLegacyGatewaySSLSettings() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "all");
    props.setProperty(GATEWAY_SSL_ENABLED, "true");
    assertThatThrownBy(() -> new DistributionConfigImpl(props, false))
        .withFailMessage(
            "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testSSLEnabledComponentsWithLegacyServerSSLSettings() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "all");
    props.setProperty(SERVER_SSL_ENABLED, "true");
    assertThatThrownBy(() -> new DistributionConfigImpl(props, false))
        .withFailMessage(
            "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testSSLEnabledComponentsWithLegacyHTTPServiceSSLSettings() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "all");
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    assertThatThrownBy(() -> new DistributionConfigImpl(props, false))
        .withFailMessage(
            "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void usesSessionBuilderToCreateMetricsSession() {
    MetricsService metricsSession = mock(MetricsService.class);
    MetricsService.Builder metricsSessionBuilder = mock(MetricsService.Builder.class);
    when(metricsSessionBuilder.build(any())).thenReturn(metricsSession);

    createSystem(getCommonProperties(), metricsSessionBuilder);

    verify(metricsSessionBuilder).build(system);
  }

  @Test
  public void startsMetricsSession() {
    MetricsService metricsSession = mock(MetricsService.class);
    MetricsService.Builder metricsSessionBuilder = mock(MetricsService.Builder.class);
    when(metricsSessionBuilder.build(any())).thenReturn(metricsSession);
    when(metricsSession.getMeterRegistry()).thenReturn(mock(MeterRegistry.class));

    createSystem(getCommonProperties(), metricsSessionBuilder);

    verify(metricsSession).start();
  }

  @Test
  public void getMeterRegistry_returnsMetricsSessionMeterRegistry() {
    MeterRegistry sessionMeterRegistry = mock(MeterRegistry.class);

    MetricsService metricsSession = mock(MetricsService.class);
    when(metricsSession.getMeterRegistry()).thenReturn(sessionMeterRegistry);

    MetricsService.Builder metricsSessionBuilder = mock(MetricsService.Builder.class);
    when(metricsSessionBuilder.build(any())).thenReturn(metricsSession);
    when(metricsSession.getMeterRegistry()).thenReturn(sessionMeterRegistry);

    createSystem(getCommonProperties(), metricsSessionBuilder);

    assertThat(system.getMeterRegistry()).isSameAs(sessionMeterRegistry);
  }

  @Test
  public void connect() {
    String theName = "theName";
    Properties configProperties = new Properties();
    configProperties.setProperty(NAME, theName);

    system = (InternalDistributedSystem) DistributedSystem.connect(configProperties);

    assertThat(system.isConnected()).isTrue();
    assertThat(system.getName()).isEqualTo(theName);
  }

  @Test
  @SetSystemProperty(key = ALLOW_MULTIPLE_SYSTEMS_PROPERTY, value = "true")
  public void connectWithAllowsMultipleSystems() {
    String name1 = "name1";
    Properties configProperties1 = new Properties();
    configProperties1.setProperty(NAME, name1);

    system = (InternalDistributedSystem) DistributedSystem.connect(configProperties1);

    String name2 = "name2";
    Properties configProperties2 = new Properties();
    configProperties2.setProperty(NAME, name2);

    final InternalDistributedSystem system2 =
        (InternalDistributedSystem) DistributedSystem.connect(configProperties2);
    try {
      assertThat(system.isConnected()).isTrue();
      assertThat(system.getName()).isEqualTo(name1);

      assertThat(system2.isConnected()).isTrue();
      assertThat(system2.getName()).isEqualTo(name2);

      assertThat(system2).isNotSameAs(system);
    } finally {
      system2.disconnect();
    }
  }

  private Properties getCommonProperties() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    return props;
  }

  public static String getHostAddress(InetAddress addr) {
    String address = addr.getHostAddress();
    if (addr instanceof Inet4Address || (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress())) {
      int idx = address.indexOf('%');
      if (idx >= 0) {
        address = address.substring(0, idx);
      }
    }
    return address;
  }

}
