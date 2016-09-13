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

package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.internal.Assert.assertTrue;
import static org.apache.geode.util.test.TestUtil.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.security.SecurableComponent;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(DistributedTest.class)
public class ConnectToLocatorSSLDUnitTest extends JUnit4DistributedTestCase {
  protected VM locator = null;
  protected File jks = null;
  protected File securityPropsFile = null;

  @Rule
  public TemporaryFolder folder = new SerializableTemporaryFolder();

  @Before
  public void before() throws Exception {
    final Host host = Host.getHost(0);
    this.locator = host.getVM(0);
    this.jks = new File(getResourcePath(getClass(), "/ssl/trusted.keystore"));
    securityPropsFile = folder.newFile("security.properties");
  }

  @After
  public void after() throws Exception {
    securityPropsFile.delete();
    CliUtil.isGfshVM = false;
  }

  @Test
  public void testConnectToLocatorWithSSL() throws Exception{
    Properties securityProps = new Properties();
    securityProps.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.LOCATOR.getConstant());
    securityProps.setProperty(SSL_KEYSTORE, jks.getCanonicalPath());
    securityProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    securityProps.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    securityProps.setProperty(SSL_TRUSTSTORE, jks.getCanonicalPath());
    securityProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    securityProps.setProperty(SSL_PROTOCOLS, "TLSv1.2,TLSv1.1");

    setUpLocatorAndConnect(securityProps);
  }

  @Test
  public void testConnectToLocatorWithLegacyClusterSSL() throws Exception{
    Properties securityProps = new Properties();
    securityProps.setProperty(CLUSTER_SSL_ENABLED, "true");
    securityProps.setProperty(CLUSTER_SSL_KEYSTORE, jks.getCanonicalPath());
    securityProps.setProperty(CLUSTER_SSL_KEYSTORE_PASSWORD, "password");
    securityProps.setProperty(CLUSTER_SSL_KEYSTORE_TYPE, "JKS");
    securityProps.setProperty(CLUSTER_SSL_TRUSTSTORE, jks.getCanonicalPath());
    securityProps.setProperty(CLUSTER_SSL_TRUSTSTORE_PASSWORD, "password");

    setUpLocatorAndConnect(securityProps);
  }

  @Test
  public void testConnectToLocatorWithLegacyJMXSSL() throws Exception{
    Properties securityProps = new Properties();
    securityProps.setProperty(JMX_MANAGER_SSL_ENABLED, "true");
    securityProps.setProperty(JMX_MANAGER_SSL_KEYSTORE, jks.getCanonicalPath());
    securityProps.setProperty(JMX_MANAGER_SSL_KEYSTORE_PASSWORD, "password");
    securityProps.setProperty(JMX_MANAGER_SSL_KEYSTORE_TYPE, "JKS");
    securityProps.setProperty(JMX_MANAGER_SSL_TRUSTSTORE, jks.getCanonicalPath());
    securityProps.setProperty(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD, "password");

    setUpLocatorAndConnect(securityProps);
  }

  public void setUpLocatorAndConnect(Properties securityProps) throws Exception{
    // set up locator with cluster-ssl-*
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int locatorPort = ports[0];
    int jmxPort = ports[1];

    locator.invoke(()->{
      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.put(JMX_MANAGER, "true");
      props.put(JMX_MANAGER_START, "true");
      props.put(JMX_MANAGER_PORT, jmxPort+"");
      props.putAll(securityProps);
      Locator.startLocatorAndDS(locatorPort, folder.newFile("locator.log"), props);
    });

    // saving the securityProps to a file
    OutputStream out = new FileOutputStream(securityPropsFile);
    securityProps.store(out, "");

    // run gfsh connect command in this vm
    CliUtil.isGfshVM = true;
    String shellId = getClass().getSimpleName();
    HeadlessGfsh gfsh = new HeadlessGfsh(shellId, 30, folder.newFolder("gfsh_files").getCanonicalPath());

    // connect to the locator with the saved property file
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.CONNECT);
    command.addOption(CliStrings.CONNECT__LOCATOR, "localhost[" + locatorPort + "]");
    command.addOption(CliStrings.CONNECT__SECURITY_PROPERTIES, securityPropsFile.getCanonicalPath());

    gfsh.executeCommand(command.toString());
    CommandResult result = (CommandResult)gfsh.getResult();
    assertEquals(Status.OK,result.getStatus());
    assertTrue(result.getContent().toString().contains("Successfully connected to"));
  }

}
