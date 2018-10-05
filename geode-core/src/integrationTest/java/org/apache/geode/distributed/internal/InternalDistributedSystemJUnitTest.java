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
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.Config;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests the functionality of the {@link InternalDistributedSystem} class. Mostly checks
 * configuration error checking.
 *
 * @since GemFire 2.1
 */
@Category({MembershipTest.class})
public class InternalDistributedSystemJUnitTest {

  /**
   * A connection to a distributed system created by this test
   */
  private InternalDistributedSystem system;

  /**
   * Creates a <code>DistributedSystem</code> with the given configuration properties.
   */
  protected InternalDistributedSystem createSystem(Properties props) {
    assertFalse(ClusterDistributionManager.isDedicatedAdminVM());
    this.system = (InternalDistributedSystem) DistributedSystem.connect(props);
    return this.system;
  }

  /**
   * Disconnects any distributed system that was created by this test
   *
   * @see DistributedSystem#disconnect
   */
  @After
  public void tearDown() throws Exception {
    if (this.system != null) {
      this.system.disconnect();
    }
  }

  //////// Test methods

  @Test
  public void testUnknownArgument() {
    Properties props = new Properties();
    props.put("UNKNOWN", "UNKNOWN");

    try {
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  /**
   * Tests that the default values of properties are what we expect
   */
  @Test
  public void testDefaultProperties() {
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributionConfig config = createSystem(props).getConfig();

    assertEquals(DistributionConfig.DEFAULT_NAME, config.getName());

    assertEquals(0, config.getMcastPort());

    assertEquals(DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[0],
        config.getMembershipPortRange()[0]);
    assertEquals(DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[1],
        config.getMembershipPortRange()[1]);

    if (System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "mcast-address") == null) {
      assertEquals(DistributionConfig.DEFAULT_MCAST_ADDRESS, config.getMcastAddress());
    }
    if (System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "bind-address") == null) {
      assertEquals(DistributionConfig.DEFAULT_BIND_ADDRESS, config.getBindAddress());
    }

    assertEquals(DistributionConfig.DEFAULT_LOG_FILE, config.getLogFile());

    // default log level gets overrided by the gemfire.properties created for unit tests.
    // assertIndexDetailsEquals(DistributionConfig.DEFAULT_LOG_LEVEL, config.getLogLevel());

    assertEquals(DistributionConfig.DEFAULT_STATISTIC_SAMPLING_ENABLED,
        config.getStatisticSamplingEnabled());

    assertEquals(DistributionConfig.DEFAULT_STATISTIC_SAMPLE_RATE, config.getStatisticSampleRate());

    assertEquals(DistributionConfig.DEFAULT_STATISTIC_ARCHIVE_FILE,
        config.getStatisticArchiveFile());

    // ack-wait-threadshold is overridden on VM's command line using a
    // system property. This is not a valid test. Hrm.
    // assertIndexDetailsEquals(DistributionConfig.DEFAULT_ACK_WAIT_THRESHOLD,
    // config.getAckWaitThreshold());

    assertEquals(DistributionConfig.DEFAULT_ACK_SEVERE_ALERT_THRESHOLD,
        config.getAckSevereAlertThreshold());

    assertEquals(DistributionConfig.DEFAULT_CACHE_XML_FILE, config.getCacheXmlFile());

    assertEquals(DistributionConfig.DEFAULT_ARCHIVE_DISK_SPACE_LIMIT,
        config.getArchiveDiskSpaceLimit());
    assertEquals(DistributionConfig.DEFAULT_ARCHIVE_FILE_SIZE_LIMIT,
        config.getArchiveFileSizeLimit());
    assertEquals(DistributionConfig.DEFAULT_LOG_DISK_SPACE_LIMIT, config.getLogDiskSpaceLimit());
    assertEquals(DistributionConfig.DEFAULT_LOG_FILE_SIZE_LIMIT, config.getLogFileSizeLimit());

    assertEquals(DistributionConfig.DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION,
        config.getEnableNetworkPartitionDetection());
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
    assertEquals(name, config.getName());
  }

  @Test
  public void testMemberTimeout() {
    Properties props = new Properties();
    int memberTimeout = 100;
    props.put(MEMBER_TIMEOUT, String.valueOf(memberTimeout));
    props.put(MCAST_PORT, "0");

    DistributionConfig config = createSystem(props).getOriginalConfig();
    assertEquals(memberTimeout, config.getMemberTimeout());
  }

  @Test
  public void testMalformedLocators() {
    Properties props = new Properties();

    try {
      // Totally bogus locator
      props.put(LOCATORS, "14lasfk^5234");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      // missing port
      props.put(LOCATORS, "localhost[");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      // Missing ]
      props.put(LOCATORS, "localhost[234ty");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      // Malformed port
      props.put(LOCATORS, "localhost[234ty]");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      // Malformed port in second locator
      props.put(LOCATORS, "localhost[12345],localhost[sdf3");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
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
    assertEquals(logLevel.intValue(), config.getLogLevel());
  }

  @Test
  public void testInvalidLogLevel() {
    try {
      Properties props = new Properties();
      props.put(LOG_LEVEL, "blah blah blah");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  @Test
  public void testGetStatisticSamplingEnabled() {
    Properties props = new Properties();
    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.put(STATISTIC_SAMPLING_ENABLED, "true");
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(true, config.getStatisticSamplingEnabled());
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
    assertEquals(1000, config.getStatisticSampleRate());
  }

  @Test
  public void testMembershipPortRange() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(MEMBERSHIP_PORT_RANGE, "45100-45200");
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(45100, config.getMembershipPortRange()[0]);
    assertEquals(45200, config.getMembershipPortRange()[1]);
  }

  @Test
  public void testBadMembershipPortRange() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(MEMBERSHIP_PORT_RANGE, "5200-5100");
    Object exception = null;
    try {
      createSystem(props).getConfig();
    } catch (IllegalArgumentException expected) {
      exception = expected;
    }
    assertNotNull("Expected an IllegalArgumentException", exception);
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
    assertEquals(fileName, config.getStatisticArchiveFile().getName());
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
    assertEquals(fileName, config.getCacheXmlFile().getPath());
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
    assertEquals(Integer.parseInt(value), config.getArchiveDiskSpaceLimit());
  }

  @Test
  public void testInvalidArchiveDiskSpaceLimit() {
    Properties props = new Properties();
    props.put(ARCHIVE_DISK_SPACE_LIMIT, "blah");
    try {
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
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
    assertEquals(Integer.parseInt(value), config.getArchiveFileSizeLimit());
  }

  @Test
  public void testInvalidArchiveFileSizeLimit() {
    Properties props = new Properties();
    props.put(ARCHIVE_FILE_SIZE_LIMIT, "blah");
    try {
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
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
    assertEquals(Integer.parseInt(value), config.getLogDiskSpaceLimit());
  }

  @Test
  public void testInvalidLogDiskSpaceLimit() {
    Properties props = new Properties();
    props.put(LOG_DISK_SPACE_LIMIT, "blah");
    try {
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
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
    assertEquals(Integer.parseInt(value), config.getLogFileSizeLimit());
  }

  @Test
  public void testInvalidLogFileSizeLimit() {
    Properties props = new Properties();
    props.put(LOG_FILE_SIZE_LIMIT, "blah");
    try {
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  @Test
  public void testAccessingClosedDistributedSystem() {
    Properties props = new Properties();

    // a loner is all this test needs
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    InternalDistributedSystem system = createSystem(props);
    system.disconnect();

    try {
      system.getDistributionManager();
      fail("Should have thrown an IllegalStateException");

    } catch (DistributedSystemDisconnectedException ex) {
      // pass...
    }
    try {
      system.getLogWriter();

    } catch (IllegalStateException ex) {
      fail("Shouldn't have thrown an IllegalStateException");
    }
  }

  @Test
  public void testPropertySources() throws Exception {
    // TODO: fix this test on Windows: the File renameTo and delete in finally fails on Windows
    String os = System.getProperty("os.name");
    if (os != null) {
      if (os.indexOf("Windows") != -1) {
        return;
      }
    }
    File propFile = new File(DistributionConfig.GEMFIRE_PREFIX + "properties");
    boolean propFileExisted = propFile.exists();
    File spropFile = new File("gfsecurity.properties");
    boolean spropFileExisted = spropFile.exists();
    try {
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + LOG_LEVEL, "finest");
      Properties apiProps = new Properties();
      apiProps.setProperty(GROUPS, "foo, bar");
      {
        if (propFileExisted) {
          propFile.renameTo(new File(DistributionConfig.GEMFIRE_PREFIX + "properties.sav"));
        }
        Properties fileProps = new Properties();
        fileProps.setProperty("name", "myName");
        FileWriter fw = new FileWriter(DistributionConfig.GEMFIRE_PREFIX + "properties");
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
      assertEquals(null, dci.getAttributeSource(MCAST_PORT));
      assertEquals(ConfigSource.api(), dci.getAttributeSource(GROUPS));
      assertEquals(ConfigSource.sysprop(), dci.getAttributeSource(LOG_LEVEL));
      assertEquals(ConfigSource.Type.FILE, dci.getAttributeSource("name").getType());
      assertEquals(ConfigSource.Type.SECURE_FILE,
          dci.getAttributeSource(STATISTIC_SAMPLE_RATE).getType());
    } finally {
      System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "log-level");
      propFile.delete();
      if (propFileExisted) {
        new File(DistributionConfig.GEMFIRE_PREFIX + "properties.sav").renameTo(propFile);
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
    assertEquals(level.intValue(), system.getConfig().getLogLevel());
    assertEquals(level.intValue(), ((InternalLogWriter) system.getLogWriter()).getLogWriterLevel());
  }

  @Test
  public void testStartLocator() {
    Properties props = new Properties();
    int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(START_LOCATOR, "localhost[" + unusedPort + "],server=false,peer=true");
    deleteStateFile(unusedPort);
    createSystem(props);
    Collection locators = Locator.getLocators();
    Assert.assertEquals(1, locators.size());
    Locator locator = (Locator) locators.iterator().next();
    // Assert.assertIndexDetailsEquals("127.0.0.1", locator.getBindAddress().getHostAddress());
    // removed this check for ipv6 testing
    Assert.assertEquals(unusedPort, locator.getPort().intValue());
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
    InternalDistributedSystem sys = InternalDistributedSystem.newInstance(config1.toProperties());
    try {

      props.put(MCAST_PORT, "1");
      Config config2 = new DistributionConfigImpl(props, false);

      try {
        sys.validateSameProperties(config2.toProperties(), true);
        fail("should have detected different mcast-ports");
      } catch (IllegalStateException iex) {
        // This passes the test
      }

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
    assertEquals("true", props1.getProperty(CLUSTER_SSL_ENABLED));
    Config config2 = new DistributionConfigImpl(props1, false);
    assertEquals(true, config1.sameAs(config2));
    Properties props3 = new Properties(props1);
    props3.setProperty(CLUSTER_SSL_ENABLED, "false");
    Config config3 = new DistributionConfigImpl(props3, false);
    assertEquals(false, config1.sameAs(config3));
  }

  @Test
  public void testSSLEnabledComponents() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "cluster,server");
    Config config1 = new DistributionConfigImpl(props, false);
    assertEquals("cluster,server", config1.getAttribute(SSL_ENABLED_COMPONENTS));
  }

  @Rule
  public ExpectedException illegalArgumentException = ExpectedException.none();

  @Test(expected = IllegalArgumentException.class)
  public void testSSLEnabledComponentsWrongComponentName() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "testing");
    new DistributionConfigImpl(props, false);
    illegalArgumentException.expect(IllegalArgumentException.class);
    illegalArgumentException
        .expectMessage("There is no registered component for the name: testing");
  }


  @Test(expected = IllegalArgumentException.class)
  public void testSSLEnabledComponentsWithLegacyJMXSSLSettings() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "all");
    props.setProperty(JMX_MANAGER_SSL_ENABLED, "true");
    new DistributionConfigImpl(props, false);
    illegalArgumentException.expect(IllegalArgumentException.class);
    illegalArgumentException.expectMessage(
        "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSSLEnabledComponentsWithLegacyGatewaySSLSettings() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "all");
    props.setProperty(GATEWAY_SSL_ENABLED, "true");
    new DistributionConfigImpl(props, false);

    illegalArgumentException.expect(IllegalArgumentException.class);
    illegalArgumentException.expectMessage(
        "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSSLEnabledComponentsWithLegacyServerSSLSettings() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "all");
    props.setProperty(SERVER_SSL_ENABLED, "true");
    new DistributionConfigImpl(props, false);

    illegalArgumentException.expect(IllegalArgumentException.class);
    illegalArgumentException.expectMessage(
        "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSSLEnabledComponentsWithLegacyHTTPServiceSSLSettings() {
    Properties props = getCommonProperties();
    props.setProperty(SSL_ENABLED_COMPONENTS, "all");
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    new DistributionConfigImpl(props, false);

    illegalArgumentException.expect(IllegalArgumentException.class);
    illegalArgumentException.expectMessage(
        "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases");
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

  public static InetAddress getIPAddress() {
    return Boolean.getBoolean("java.net.preferIPv6Addresses") ? getIPv6Address() : getIPv4Address();
  }

  protected static InetAddress getIPv4Address() {
    InetAddress host = null;
    try {
      host = InetAddress.getLocalHost();
      if (host instanceof Inet4Address) {
        return host;
      }
    } catch (UnknownHostException e) {
      String s = "Local host not found";
      throw new RuntimeException(s, e);
    }
    try {
      Enumeration i = NetworkInterface.getNetworkInterfaces();
      while (i.hasMoreElements()) {
        NetworkInterface ni = (NetworkInterface) i.nextElement();
        Enumeration j = ni.getInetAddresses();
        while (j.hasMoreElements()) {
          InetAddress addr = (InetAddress) j.nextElement();
          // gemfire won't form connections using link-local addresses
          if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()
              && (addr instanceof Inet4Address)) {
            return addr;
          }
        }
      }
      String s = "IPv4 address not found";
      throw new RuntimeException(s);
    } catch (SocketException e) {
      String s = "Problem reading IPv4 address";
      throw new RuntimeException(s, e);
    }
  }

  public static InetAddress getIPv6Address() {
    try {
      Enumeration i = NetworkInterface.getNetworkInterfaces();
      while (i.hasMoreElements()) {
        NetworkInterface ni = (NetworkInterface) i.nextElement();
        Enumeration j = ni.getInetAddresses();
        while (j.hasMoreElements()) {
          InetAddress addr = (InetAddress) j.nextElement();
          // gemfire won't form connections using link-local addresses
          if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()
              && (addr instanceof Inet6Address) && !isIPv6LinkLocalAddress((Inet6Address) addr)) {
            return addr;
          }
        }
      }
      String s = "IPv6 address not found";
      throw new RuntimeException(s);
    } catch (SocketException e) {
      String s = "Problem reading IPv6 address";
      throw new RuntimeException(s, e);
    }
  }

  /**
   * Detect LinkLocal IPv6 address where the interface is missing, ie %[0-9].
   *
   * @see InetAddress#isLinkLocalAddress()
   */
  private static boolean isIPv6LinkLocalAddress(Inet6Address addr) {
    byte[] addrBytes = addr.getAddress();
    return ((addrBytes[0] == (byte) 0xfe) && (addrBytes[1] == (byte) 0x80));
  }
}
