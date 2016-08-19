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

package com.gemstone.gemfire.test.dunit.rules;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static com.gemstone.gemfire.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static com.gemstone.gemfire.test.dunit.Host.getHost;
import static com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase.disconnectAllFromDS;
import static com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase.disconnectFromDS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.rules.ExternalResource;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.jayway.awaitility.Awaitility;


public class LocatorServerConfigurationRule extends ExternalResource implements
    Serializable {

  private int locatorPort = 0;
  
  private boolean locatorInitialized = false;
  
  private JUnit4CacheTestCase testCase;

  public LocatorServerConfigurationRule(JUnit4CacheTestCase testCase) {
    this.testCase = testCase;
  }

  Host host = getHost(0);
  VM locator = host.getVM(0);

  @Override
  protected void before() {
    // Add initialization requirement if any.
    disconnectAllFromDS();
  }

  @Override
  protected void after() {
    disconnectAllFromDS();
  }


  /**
   * Returns getHost(0).getVM(0) as a locator instance with the given
   * configuration properties.
   * 
   * @param locatorProperties
   * @return VM locator vm
   * @throws IOException
   */
  public VM getLocatorVM(Properties locatorProperties) throws IOException {
    initLocator(locatorProperties);
    this.locatorInitialized = true;
    return locator;
  }

  /**
   * Returns a node VM with given configuration properties.
   * 
   * @param index
   *          valid 1 to 3 (returns getHist(0).getVM(index)
   * @param nodeProperties
   * @return VM node vm
   */
  public VM getNodeVM(int index, Properties nodeProperties) {
    assertTrue("Locator not initialized. Initialize locator by calling getLocatorVM()", this.locatorInitialized);
    assertTrue("VM with index 0 is used for locator service.", (index != 0));
    VM nodeVM = host.getVM(index);
    initNode(nodeVM, nodeProperties);
    return nodeVM;
  }

  private void initLocator(Properties locatorProperties) throws IOException {
    final int[] ports = getRandomAvailableTCPPorts(1);
    locatorPort = ports[0];

    if (!locatorProperties.containsKey(MCAST_PORT)) {
      locatorProperties.setProperty(MCAST_PORT, "0");
    }

    locatorPort = locator.invoke(() -> {
      InternalLocator locator = (InternalLocator)Locator.startLocatorAndDS(0,
          null, locatorProperties);
      locatorPort = locator.getPort();
      locator.resetInternalLocatorFileNamesWithCorrectPortNumber(locatorPort);

      if (locatorProperties.containsKey(ENABLE_CLUSTER_CONFIGURATION)) {
        Awaitility.await().atMost(65, TimeUnit.SECONDS)
            .until(() -> assertTrue(locator.isSharedConfigurationRunning()));
      }
      return locatorPort;
    });
  }

  private void initNode(VM nodeVM, Properties props) {
    if (!props.containsKey(MCAST_PORT)) {
      props.setProperty(MCAST_PORT, "0");
    }

    props.setProperty(LOCATORS, getHostName() + ":" + locatorPort);

    nodeVM.invoke(() -> {
      testCase.getSystem(props);
      assertNotNull(testCase.getCache());
    });
  }

  private String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    }
    catch (UnknownHostException ignore) {
      return "localhost";
    }
  }

}
