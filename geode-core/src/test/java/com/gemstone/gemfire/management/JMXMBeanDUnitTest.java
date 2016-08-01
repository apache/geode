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
package com.gemstone.gemfire.management;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.gemstone.gemfire.distributed.AbstractLauncher;
import com.gemstone.gemfire.distributed.LocatorLauncher;
import com.gemstone.gemfire.distributed.ServerLauncher;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.net.SSLEnabledComponent;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.rules.DistributedRestoreSystemProperties;
import com.gemstone.gemfire.test.junit.rules.serializable.SerializableTemporaryFolder;
import com.gemstone.gemfire.util.test.TestUtil;

public class JMXMBeanDUnitTest extends DistributedTestCase {

  private Host host;
  private VM locator;
  private VM jmxClient;
  private String serverHostName;
  private int locatorPort;
  private int jmxPort;

  private static LocatorLauncher locatorLauncher;

  @Rule
  public DistributedRestoreSystemProperties distributedRestoreSystemProperties = new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void before() {
    host = Host.getHost(0);
    locator = host.getVM(0);
    jmxClient = host.getVM(1);

    int[] randomAvailableTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    locatorPort = randomAvailableTCPPorts[0];
    jmxPort = randomAvailableTCPPorts[1];
    serverHostName = NetworkUtils.getServerHostName(host);
  }

  @After
  public void after() throws InterruptedException {
    Invoke.invokeInEveryVM(() -> {
      if (locatorLauncher != null) {
        try {
          assertEquals(AbstractLauncher.Status.STOPPED, locatorLauncher.stop().getStatus());
        } finally {
          locatorLauncher = null;
        }
      }
    });
  }

  @Test
  public void testJMXOverSSLWithoutJMXAlias() {
    locator.invoke("Configure and start Locator", () -> {
      System.setProperty("javax.ssl.debug", "true");
      configureAndStartLocator(locatorPort, jmxPort, serverHostName, true, false, true);
    });

    jmxClient.invoke("Configure and start JMX Client", () -> {
      System.setProperty("javax.ssl.debug", "true");
      connectAndValidateAsJmxClient(jmxPort, serverHostName, true, true);
    });
  }

  @Test
  public void testJMXOverSSLWithJMXAlias() throws Exception {
    locator.invoke("Configure and start Locator", () -> {
      System.setProperty("javax.ssl.debug", "true");
      configureAndStartLocator(locatorPort, jmxPort, serverHostName, true, false, true);
    });

    jmxClient.invoke("Configure and start JMX Client", () -> {
      System.setProperty("javax.ssl.debug", "true");
      connectAndValidateAsJmxClient(jmxPort, serverHostName, true, true);
    });
  }

  @Test
  public void testJMXOverSSL() {
    locator.invoke("Configure and start Locator", () -> {
      System.setProperty("javax.ssl.debug", "true");
      configureAndStartLocator(locatorPort, jmxPort, serverHostName, true, false, false);
    });

    jmxClient.invoke("Configure and start JMX Client", () -> {
      System.setProperty("javax.ssl.debug", "true");
      connectAndValidateAsJmxClient(jmxPort, serverHostName, true);
    });
  }

  @Test
  public void testJMXOverLegacySSL() {
    locator.invoke("Configure and start Locator", () -> {
      System.setProperty("javax.ssl.debug", "true");
      configureAndStartLocator(locatorPort, jmxPort, serverHostName, true, true, false);
    });

    jmxClient.invoke("Configure and start JMX Client", () -> {
      System.setProperty("javax.ssl.debug", "true");
      connectAndValidateAsJmxClient(jmxPort, serverHostName, true);
    });
  }

  @Test
  @Ignore
  //To be fixed in GEODE-1716
  public void testJMXOverNonSSL() throws Exception {
    locator.invoke("Configure and start Locator", () -> {
      configureAndStartLocator(locatorPort, jmxPort, serverHostName, false);
    });

    jmxClient.invoke("Configure and start JMX Client", () -> {
      connectAndValidateAsJmxClient(jmxPort, serverHostName, false);
    });

  }

  @Test
  public void testJMXOverNonSSLWithClientUsingIncorrectPort() throws Exception {
    locator.invoke("Configure and start Locator", () -> {
      configureAndStartLocator(locatorPort, jmxPort, serverHostName, false);
    });

    assertThatThrownBy(() -> jmxClient.invoke("Configure and start JMX Client", () -> {
      connectAndValidateAsJmxClient(9999, serverHostName, false);
    })).hasCauseExactlyInstanceOf(IOException.class).hasRootCauseExactlyInstanceOf(java.net.ConnectException.class);

  }


  private void connectAndValidateAsJmxClient(final int jmxPort, final String serverHostName, final boolean useSSL) throws Exception {
    connectAndValidateAsJmxClient(jmxPort, serverHostName, useSSL, false);
  }

  private void connectAndValidateAsJmxClient(final int jmxPort, final String serverHostName, final boolean useSSL, final boolean useMulti) throws Exception {
    // JMX RMI

    Map<String,Object> environment = new HashMap();

    if (useSSL) {
      System.setProperty("javax.net.ssl.keyStore", useMulti ? getMultiKeyKeystore() : getSimpleSingleKeyKeystore());
      System.setProperty("javax.net.ssl.keyStoreType", "JKS");
      System.setProperty("javax.net.ssl.keyStorePassword", "password");
      System.setProperty("javax.net.ssl.trustStore", useMulti ? getMultiKeyTruststore() : getSimpleSingleKeyKeystore());
      System.setProperty("javax.net.ssl.trustStoreType", "JKS");
      System.setProperty("javax.net.ssl.trustStorePassword", "password");
      System.setProperty("com.sun.management.jmxremote.ssl", "true");
      System.setProperty("com.sun.management.jmxremote.registry.ssl", "true");
      environment.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
    }

    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + serverHostName + ":" + jmxPort + "/jndi/rmi://" + serverHostName + ":" + jmxPort + "/jmxrmi");
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, environment);


    try {
      MBeanServerConnection mbeanServerConnection = jmxConnector.getMBeanServerConnection();

      ObjectName mbeanName = new ObjectName("GemFire:service=System,type=Distributed");

      //Get MBean proxy instance that will be used to make calls to registered MBean
      DistributedSystemMXBean distributedSystemMXBean = JMX.newMBeanProxy(mbeanServerConnection, mbeanName, DistributedSystemMXBean.class, true);

      assertEquals(1, distributedSystemMXBean.getMemberCount());
      assertEquals(1, distributedSystemMXBean.getLocatorCount());

    } finally {
      jmxConnector.close();
    }
  }

  private void configureAndStartServer(final int locatorPort, final String serverHostName) throws IOException {
    final String memberName = getUniqueName() + "-server";
    final File workingDirectory = temporaryFolder.newFolder(memberName);

    Properties properties = configureServerProperties(new Properties(), serverHostName + "[" + locatorPort + "]");
    ServerLauncher.Builder builder = new ServerLauncher.Builder();

    //    properties.stringPropertyNames().stream().map((name) -> builder.set(name, properties.getProperty(name)));
    for (String propertyName : properties.stringPropertyNames()) {
      builder.set(propertyName, properties.getProperty(propertyName));
    }
    builder.setServerPort(0)
           .setHostNameForClients(serverHostName)
           .setServerBindAddress(serverHostName)
           .setWorkingDirectory(workingDirectory.getCanonicalPath())
           .setMemberName(memberName)
           .build()
           .start();
  }

  private void configureAndStartLocator(final int locatorPort, final int jmxPort, final String serverHostName, final boolean useSSL) throws IOException {
    configureAndStartLocator(locatorPort, jmxPort, serverHostName, useSSL, false, false);
  }

  private void configureAndStartLocator(final int locatorPort,
                                        final int jmxPort,
                                        final String serverHostName,
                                        final boolean useSSL,
                                        final boolean useLegacySSL,
                                        final boolean useMultiKeyKeystore) throws IOException {
    DistributedTestUtils.deleteLocatorStateFile();

    final String memberName = getUniqueName() + "-locator";
    final File workingDirectory = temporaryFolder.newFolder(memberName);

    Properties properties = configureLocatorProperties(new Properties(), jmxPort, serverHostName, useSSL, useLegacySSL, useMultiKeyKeystore);
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder();

    //    properties.stringPropertyNames().stream().map((name) -> builder.set(name, properties.getProperty(name)));
    for (String propertyName : properties.stringPropertyNames()) {
      builder.set(propertyName, properties.getProperty(propertyName));
    }
    locatorLauncher = builder.setBindAddress(serverHostName)
                             .setHostnameForClients(serverHostName)
                             .setMemberName(memberName)
                             .setPort(locatorPort)
                             .setWorkingDirectory(workingDirectory.getCanonicalPath())
                             .build();
    locatorLauncher.start();

  }

  private Properties configureServerProperties(final Properties properties, final String locators) {
    configureCommonProperties(properties);
    properties.setProperty(LOCATORS, locators);
    return properties;
  }

  private Properties configureJMXSSLProperties(final Properties properties, final boolean isLegacy, final boolean useMultiKey) {
    if (isLegacy) {
      properties.setProperty(JMX_MANAGER_SSL_CIPHERS, "any");
      properties.setProperty(JMX_MANAGER_SSL_PROTOCOLS, "any");
      properties.setProperty(JMX_MANAGER_SSL_ENABLED, "true");
      properties.setProperty(JMX_MANAGER_SSL_KEYSTORE, getSimpleSingleKeyKeystore());
      properties.setProperty(JMX_MANAGER_SSL_KEYSTORE_PASSWORD, "password");
      properties.setProperty(JMX_MANAGER_SSL_TRUSTSTORE, getSimpleSingleKeyKeystore());
      properties.setProperty(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD, "password");
    } else {
      {
        properties.setProperty(CLUSTER_SSL_ENABLED, "true");
        properties.setProperty(CLUSTER_SSL_CIPHERS, "any");
        properties.setProperty(CLUSTER_SSL_PROTOCOLS, "any");
        properties.setProperty(CLUSTER_SSL_KEYSTORE_PASSWORD, "password");
        properties.setProperty(CLUSTER_SSL_TRUSTSTORE_PASSWORD, "password");
        properties.setProperty(CLUSTER_SSL_KEYSTORE, getSimpleSingleKeyKeystore());
        properties.setProperty(CLUSTER_SSL_TRUSTSTORE, getSimpleSingleKeyKeystore());
        properties.setProperty(SSL_ENABLED_COMPONENTS, SSLEnabledComponent.JMX.toString());

        if (useMultiKey) {
          properties.setProperty(CLUSTER_SSL_KEYSTORE, getMultiKeyKeystore());
          properties.setProperty(CLUSTER_SSL_TRUSTSTORE, getMultiKeyTruststore());
          properties.setProperty(JMX_MANAGER_SSL_ALIAS, "jmxkey");
        }
      }
    }
    return properties;
  }

  private String getSimpleSingleKeyKeystore() {
    return TestUtil.getResourcePath(getClass(), "/ssl/trusted.keystore");
  }

  private String getMultiKeyKeystore() {
    return TestUtil.getResourcePath(getClass(), "/com/gemstone/gemfire/internal/net/multiKey.jks");
  }

  private String getMultiKeyTruststore() {
    return TestUtil.getResourcePath(getClass(), "/com/gemstone/gemfire/internal/net/multiKeyTrust.jks");
  }

  private Properties configureLocatorProperties(final Properties properties,
                                                final int jmxPort,
                                                final String serverHostName,
                                                final boolean useSSL,
                                                final boolean useLegacySSL,
                                                final boolean useMultiKeyKeystore) {
    configureCommonProperties(properties);
    properties.setProperty(JMX_MANAGER, "true");
    properties.setProperty(JMX_MANAGER_START, "true");
    properties.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    properties.setProperty(JMX_MANAGER_BIND_ADDRESS, serverHostName);
    properties.setProperty(JMX_MANAGER_HOSTNAME_FOR_CLIENTS, serverHostName);

    if (useSSL) {
      configureJMXSSLProperties(properties, useLegacySSL, useMultiKeyKeystore);
    }
    return properties;
  }

  private Properties configureCommonProperties(final Properties properties) {
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    return properties;
  }
}
