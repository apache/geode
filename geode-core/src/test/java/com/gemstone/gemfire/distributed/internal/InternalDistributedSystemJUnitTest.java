/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal;

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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.Config;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests the functionality of the {@link InternalDistributedSystem}
 * class.  Mostly checks configuration error checking.
 *
 * @author David Whitlock
 *
 * @since 2.1
 */
@Category(IntegrationTest.class)
public class InternalDistributedSystemJUnitTest 
  //implements DistributionConfig
{

  /** A connection to a distributed system created by this test */
  private InternalDistributedSystem system;

  /**
   * Creates a <code>DistributedSystem</code> with the given
   * configuration properties.
   */
  protected InternalDistributedSystem createSystem(Properties props) {
    assertFalse(com.gemstone.gemfire.distributed.internal.DistributionManager.isDedicatedAdminVM);
    this.system =
      (InternalDistributedSystem) DistributedSystem.connect(props);
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
  
  ////////  Test methods
  
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
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    DistributionConfig config = createSystem(props).getConfig();

    assertEquals(DistributionConfig.DEFAULT_NAME, config.getName());

    assertEquals(0, config.getMcastPort());
    
    assertEquals(DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[0], config.getMembershipPortRange()[0]);
    assertEquals(DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[1], config.getMembershipPortRange()[1]);

    if (System.getProperty("gemfire.mcast-address") == null) {
      assertEquals(DistributionConfig.DEFAULT_MCAST_ADDRESS, config.getMcastAddress());
    }
    if (System.getProperty("gemfire.bind-address") == null) {
      assertEquals(DistributionConfig.DEFAULT_BIND_ADDRESS, config.getBindAddress());
    }

    assertEquals(DistributionConfig.DEFAULT_LOG_FILE, config.getLogFile());

    //default log level gets overrided by the gemfire.properties created for unit tests.
//    assertEquals(DistributionConfig.DEFAULT_LOG_LEVEL, config.getLogLevel());

    assertEquals(DistributionConfig.DEFAULT_STATISTIC_SAMPLING_ENABLED,
                 config.getStatisticSamplingEnabled());

    assertEquals(DistributionConfig.DEFAULT_STATISTIC_SAMPLE_RATE,
                 config.getStatisticSampleRate());

    assertEquals(DistributionConfig.DEFAULT_STATISTIC_ARCHIVE_FILE,
                 config.getStatisticArchiveFile());

    // ack-wait-threadshold is overridden on VM's command line using a
    // system property.  This is not a valid test.  Hrm.
//     assertEquals(DistributionConfig.DEFAULT_ACK_WAIT_THRESHOLD, config.getAckWaitThreshold());

    assertEquals(DistributionConfig.DEFAULT_ACK_SEVERE_ALERT_THRESHOLD, config.getAckSevereAlertThreshold());
    
    assertEquals(DistributionConfig.DEFAULT_CACHE_XML_FILE, config.getCacheXmlFile());

    assertEquals(DistributionConfig.DEFAULT_ARCHIVE_DISK_SPACE_LIMIT, config.getArchiveDiskSpaceLimit());
    assertEquals(DistributionConfig.DEFAULT_ARCHIVE_FILE_SIZE_LIMIT, config.getArchiveFileSizeLimit());
    assertEquals(DistributionConfig.DEFAULT_LOG_DISK_SPACE_LIMIT, config.getLogDiskSpaceLimit());
    assertEquals(DistributionConfig.DEFAULT_LOG_FILE_SIZE_LIMIT, config.getLogFileSizeLimit());
    
    assertEquals(DistributionConfig.DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION, config.getEnableNetworkPartitionDetection());
  }

  @Test
  public void testGetName() {
    String name = "testGetName";

    Properties props = new Properties();
    props.put(DistributionConfig.NAME_NAME, name);
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");

    DistributionConfig config = createSystem(props).getOriginalConfig();
    assertEquals(name, config.getName());
  }
  
  @Test
  public void testMemberTimeout() {
    Properties props = new Properties();
    int memberTimeout = 100;
    props.put("member-timeout", String.valueOf(memberTimeout));
    props.put("mcast-port", "0");

    DistributionConfig config = createSystem(props).getOriginalConfig();
    assertEquals(memberTimeout, config.getMemberTimeout());
  }

  @Test
  public void testMalformedLocators() {
    Properties props = new Properties();

    try {
      // Totally bogus locator
      props.put(DistributionConfig.LOCATORS_NAME, "14lasfk^5234");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      // missing port
      props.put(DistributionConfig.LOCATORS_NAME, "localhost[");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      // Missing ]
      props.put(DistributionConfig.LOCATORS_NAME, "localhost[234ty");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      // Malformed port
      props.put(DistributionConfig.LOCATORS_NAME, "localhost[234ty]");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      // Malformed port in second locator
      props.put(DistributionConfig.LOCATORS_NAME, "localhost[12345],localhost[sdf3");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  /**
   * Creates a new <code>DistributionConfigImpl</code> with the given
   * locators string.
   *
   * @throws IllegalArgumentException
   *         If <code>locators</code> is malformed
   *
   * @since 4.0
   */
  private void checkLocator(String locator) {
    Properties props = new Properties();
    props.put(DistributionConfig.LOCATORS_NAME, locator);
    new DistributionConfigImpl(props);
  }

  /**
   * Tests that both the traditional syntax ("host[port]") and post
   * bug-32306 syntax ("host:port") can be used with locators.
   *
   * @since 4.0
   */
  @Test
  public void testLocatorSyntax() throws Exception {
    String localhost =
      java.net.InetAddress.getLocalHost().getCanonicalHostName();
    checkLocator(localhost + "[12345]");
    checkLocator(localhost + ":12345");

    String bindAddress = 
      getHostAddress(java.net.InetAddress.getLocalHost());
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
   * Test a configuration with an <code>mcastPort</code> of zero and
   * an empty <code>locators</code>.
   *
   * @deprecated This test creates a "loner" distributed system
   */
  public void _testEmptyLocators() {
    Properties props = new Properties();
    props.put(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
    props.put(DistributionConfig.LOCATORS_NAME, "");
    try {
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  /**
   * Tests that getting the log level is what we expect.
   */
  @Test
  public void testGetLogLevel() {
    Level logLevel = Level.FINER;
    Properties props = new Properties();
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.LOG_LEVEL_NAME, logLevel.toString());

    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(logLevel.intValue(), config.getLogLevel());
  }

  @Test
  public void testInvalidLogLevel() {
    try {
      Properties props = new Properties();
      props.put(DistributionConfig.LOG_LEVEL_NAME, "blah blah blah");
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  @Test
  public void testGetStatisticSamplingEnabled() {
    Properties props = new Properties();
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(true, config.getStatisticSamplingEnabled());
  }

  @Test
  public void testGetStatisticSampleRate() {
    String rate = String.valueOf(DistributionConfig.MIN_STATISTIC_SAMPLE_RATE);
    Properties props = new Properties();
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.STATISTIC_SAMPLE_RATE_NAME, rate);
    DistributionConfig config = createSystem(props).getConfig();
    // The fix for 48228 causes the rate to be 1000 even if we try to set it less
    assertEquals(1000, config.getStatisticSampleRate());
  }

  @Test
  public void testMembershipPortRange() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty(DistributionConfig.MEMBERSHIP_PORT_RANGE_NAME, "5100-5200");
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(5100, config.getMembershipPortRange()[0]);
    assertEquals(5200, config.getMembershipPortRange()[1]);
  }

  @Test
  public void testBadMembershipPortRange() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty(DistributionConfig.MEMBERSHIP_PORT_RANGE_NAME, "5200-5100");
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
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, fileName);
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(fileName, config.getStatisticArchiveFile().getName());
  }

  /**
   * @deprecated This test cannot be run because the
   * gemfire.ack-wait-threshold system property is set on this VM,
   * thus overriding the value passed into the API.
   */
  public void _testGetAckWaitThreshold() {
    String time = String.valueOf(DistributionConfig.MIN_ACK_WAIT_THRESHOLD);
    Properties props = new Properties();
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.ACK_WAIT_THRESHOLD_NAME, time);
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(Integer.parseInt(time), config.getAckWaitThreshold());
  }

  /**
   * @deprecated This test cannot be run because the
   * gemfire.ack-wait-threshold system property is set on this VM,
   * thus overriding the value passed into the API.
   */
  public void _testInvalidAckWaitThreshold() {
    Properties props = new Properties();
    props.put(DistributionConfig.ACK_WAIT_THRESHOLD_NAME, "blah");
    try {
      createSystem(props);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  @Test
  public void testGetCacheXmlFile() {
    String fileName = "blah";
    Properties props = new Properties();
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.CACHE_XML_FILE_NAME, fileName);
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(fileName, config.getCacheXmlFile().getPath());
  }

  @Test
  public void testGetArchiveDiskSpaceLimit() {
    String value = String.valueOf(DistributionConfig.MIN_ARCHIVE_DISK_SPACE_LIMIT);
    Properties props = new Properties();
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.ARCHIVE_DISK_SPACE_LIMIT_NAME, value);
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(Integer.parseInt(value), config.getArchiveDiskSpaceLimit());
  }

  @Test
  public void testInvalidArchiveDiskSpaceLimit() {
    Properties props = new Properties();
    props.put(DistributionConfig.ARCHIVE_DISK_SPACE_LIMIT_NAME, "blah");
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
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.ARCHIVE_FILE_SIZE_LIMIT_NAME, value);
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(Integer.parseInt(value), config.getArchiveFileSizeLimit());
  }

  @Test
  public void testInvalidArchiveFileSizeLimit() {
    Properties props = new Properties();
    props.put(DistributionConfig.ARCHIVE_FILE_SIZE_LIMIT_NAME, "blah");
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
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.LOG_DISK_SPACE_LIMIT_NAME, value);
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(Integer.parseInt(value), config.getLogDiskSpaceLimit());
  }

  @Test
  public void testInvalidLogDiskSpaceLimit() {
    Properties props = new Properties();
    props.put(DistributionConfig.LOG_DISK_SPACE_LIMIT_NAME, "blah");
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
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.LOG_FILE_SIZE_LIMIT_NAME, value);
    DistributionConfig config = createSystem(props).getConfig();
    assertEquals(Integer.parseInt(value), config.getLogFileSizeLimit());
  }

  @Test
  public void testInvalidLogFileSizeLimit() {
    Properties props = new Properties();
    props.put(DistributionConfig.LOG_FILE_SIZE_LIMIT_NAME, "blah");
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

//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
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
    File propFile = new File("gemfire.properties");
    boolean propFileExisted = propFile.exists();
    File spropFile = new File("gfsecurity.properties");
    boolean spropFileExisted = spropFile.exists();
    try {
      System.setProperty("gemfire.log-level", "finest");
      Properties apiProps = new Properties();
      apiProps.setProperty("groups", "foo, bar");
      {
        if (propFileExisted) {
          propFile.renameTo(new File("gemfire.properties.sav"));
        }
        Properties fileProps = new Properties();
        fileProps.setProperty("name", "myName");
        FileWriter fw = new FileWriter("gemfire.properties");
        fileProps.store(fw, null);
        fw.close();
      }
      {
        if (spropFileExisted) {
          spropFile.renameTo(new File("gfsecurity.properties.sav"));
        }
        Properties fileProps = new Properties();
        fileProps.setProperty("statistic-sample-rate", "999");
        FileWriter fw = new FileWriter("gfsecurity.properties");
        fileProps.store(fw, null);
        fw.close();
      }
      DistributionConfigImpl dci = new DistributionConfigImpl(apiProps);
      assertEquals(null, dci.getAttributeSource("mcast-port"));
      assertEquals(ConfigSource.api(), dci.getAttributeSource("groups"));
      assertEquals(ConfigSource.sysprop(), dci.getAttributeSource("log-level"));
      assertEquals(ConfigSource.Type.FILE, dci.getAttributeSource("name").getType());
      assertEquals(ConfigSource.Type.SECURE_FILE, dci.getAttributeSource("statistic-sample-rate").getType());
    } finally {
      System.clearProperty("gemfire.log-level");
      propFile.delete();
      if (propFileExisted) {
        new File("gemfire.properties.sav").renameTo(propFile);
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
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty(DistributionConfig.NAME_NAME, name);
    createSystem(props);
  }

  @Test
  public void testNonDefaultLogLevel() {
    Level level = Level.FINE;

    Properties props = new Properties();
//     int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//     props.setProperty("mcast-port", String.valueOf(unusedPort));
    // a loner is all this test needs
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.put(DistributionConfig.LOG_LEVEL_NAME, level.toString());
    InternalDistributedSystem system = createSystem(props);
    assertEquals(level.intValue(), system.getConfig().getLogLevel());
    assertEquals(level.intValue(), ((InternalLogWriter) system.getLogWriter()).getLogWriterLevel());
  }
  
  @Test
  public void testStartLocator() {
    Properties props = new Properties();
    int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    props.setProperty("mcast-port", "0");
    props.setProperty("start-locator", "localhost[" + unusedPort + "],server=false,peer=true");
    deleteStateFile(unusedPort);
    createSystem(props);
    Collection locators = Locator.getLocators();
    Assert.assertEquals(1, locators.size());
    Locator locator = (Locator) locators.iterator().next();
    Assert.assertTrue(locator.isPeerLocator());
//    Assert.assertFalse(locator.isServerLocator()); server location is forced on while licensing is disabled in GemFire
//    Assert.assertEquals("127.0.0.1", locator.getBindAddress().getHostAddress());  removed this check for ipv6 testing
    Assert.assertEquals(unusedPort, locator.getPort());
    deleteStateFile(unusedPort);
  }

  private void deleteStateFile(int port) {
    File stateFile = new File("locator"+port+"state.dat");
    if (stateFile.exists()) {
      stateFile.delete();
    }
  }
  
  @Test
  public void testValidateProps() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    Config config1 = new DistributionConfigImpl(props, false);
    InternalDistributedSystem sys = InternalDistributedSystem.newInstance(config1.toProperties());
    try {

    props.put("mcast-port", "1");
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
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("ssl-enabled", "true");
    Config config1 = new DistributionConfigImpl(props, false);
    Properties props1 = config1.toProperties();
    // For the deprecated ssl-* properties a decision was made
    // to not include them in the result of "toProperties".
    // The cause of this is: com.gemstone.gemfire.internal.AbstractConfig.isDeprecated(String)
    // and its use in toProperties.
    // The other thing that is done is the ssl-* props are copied to cluster-ssl-*.
    // The following two assertions demonstrate this.
    assertEquals(null, props1.getProperty("ssl-enabled"));
    assertEquals("true", props1.getProperty("cluster-ssl-enabled"));
    Config config2 = new DistributionConfigImpl(props1, false);
    assertEquals(true, config1.sameAs(config2));
    Properties props3 = new Properties(props1);
    props3.setProperty("ssl-enabled", "false");
    Config config3 = new DistributionConfigImpl(props3, false);
    assertEquals(false, config1.sameAs(config3));
  }
  public static String getHostAddress(InetAddress addr) {
    String address = addr.getHostAddress();
    if (addr instanceof Inet4Address
        || (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()))
    {
      int idx = address.indexOf('%');
      if (idx >= 0) {
        address = address.substring(0, idx);
      }
    }
    return address;
  }

  public static InetAddress getIPAddress() {
    return Boolean.getBoolean("java.net.preferIPv6Addresses") ? getIPv6Address()
                                                      : getIPv4Address();
  }
  
  protected static InetAddress getIPv4Address() {
    InetAddress host = null;
    try {
      host = InetAddress.getLocalHost();
      if (host instanceof Inet4Address) {
        return host;
      }
    }
    catch (UnknownHostException e) {
      String s = "Local host not found";
      throw new RuntimeException(s, e);
    }
    try {
      Enumeration i = NetworkInterface.getNetworkInterfaces();
      while (i.hasMoreElements()) {
        NetworkInterface ni = (NetworkInterface)i.nextElement();
        Enumeration j = ni.getInetAddresses();
        while (j.hasMoreElements()) {
          InetAddress addr = (InetAddress)j.nextElement();
          // gemfire won't form connections using link-local addresses
          if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()
                                         && (addr instanceof Inet4Address)) {
            return addr;
          }
        }
      }
      String s = "IPv4 address not found";
      throw new RuntimeException(s);
    }
    catch (SocketException e) {
      String s = "Problem reading IPv4 address";
      throw new RuntimeException(s, e);
    }
  }

  public static InetAddress getIPv6Address() {
    try {
      Enumeration i = NetworkInterface.getNetworkInterfaces();
      while (i.hasMoreElements()) {
        NetworkInterface ni = (NetworkInterface)i.nextElement();
        Enumeration j = ni.getInetAddresses();
        while (j.hasMoreElements()) {
          InetAddress addr = (InetAddress)j.nextElement();
          // gemfire won't form connections using link-local addresses
          if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()
               && (addr instanceof Inet6Address)
               && !isIPv6LinkLocalAddress((Inet6Address) addr)) {
            return addr;
          }
        }
      }
      String s = "IPv6 address not found";
      throw new RuntimeException(s);
    }
    catch (SocketException e) {
      String s = "Problem reading IPv6 address";
      throw new RuntimeException(s, e);
    }
  }

  /**
   * Detect LinkLocal IPv6 address where the interface is missing, ie %[0-9].
   * @see InetAddress#isLinkLocalAddress()
   */
  private static boolean isIPv6LinkLocalAddress(Inet6Address addr) {
    byte[] addrBytes = addr.getAddress();
    return ((addrBytes[0] == (byte) 0xfe) && (addrBytes[1] == (byte) 0x80));
  }
}
