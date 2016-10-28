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

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.dunit.Host.getHost;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.junit.After;
import org.junit.Before;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;


/**
 * this rule can help you start up locator/server in different VMs you can multiple locators/servers
 * combination
 */
public class LocatorServerStartupRule extends ExternalResource implements Serializable {

  // these are only avaialbe in each VM
  public static ServerStarterRule serverStarter;
  public static LocatorStarterRule locatorStarter;
  public int[] ports = new int[4];
  private Host host = getHost(0);

  @Before
  public void before() {
    after();
  }

  @After
  public void after() {
    stop();
    Invoke.invokeInEveryVM("Stop each VM", () -> stop());
  }

  /**
   * Returns getHost(0).getVM(0) as a locator instance with the given configuration properties.
   * 
   * @param locatorProperties
   *
   * @return VM locator vm
   *
   * @throws IOException
   */
  public VM getLocatorVM(int index, Properties locatorProperties) throws IOException {
    VM locatorVM = host.getVM(index);
    locatorProperties.setProperty(NAME, "locator-" + index);
    int locatorPort = locatorVM.invoke(() -> {
      locatorStarter = new LocatorStarterRule(locatorProperties);
      locatorStarter.startLocator();
      return locatorStarter.locator.getPort();
    });
    ports[index] = locatorPort;
    return locatorVM;
  }

  /**
   * starts a cache server that does not connect to a locator
   * 
   * @return VM node vm
   */

  public VM getServerVM(int index, Properties properties) {
    return getServerVM(index, properties, 0);
  }

  /**
   * starts a cache server that connect to the locator running at the given port.
   * 
   * @param index
   * @param properties
   * @param locatorPort
   * @return
   */
  public VM getServerVM(int index, Properties properties, int locatorPort) {
    VM nodeVM = getNodeVM(index);
    properties.setProperty(NAME, "server-" + index);
    int port = nodeVM.invoke(() -> {
      serverStarter = new ServerStarterRule(properties);
      serverStarter.startServer(locatorPort);
      return serverStarter.server.getPort();
    });
    ports[index] = port;
    return nodeVM;
  }



  /**
   * this will simply returns the node
   * 
   * @param index
   * @return
   */
  public VM getNodeVM(int index) {
    return host.getVM(index);
  }

  public int getPort(int index) {
    return ports[index];
  }


  public final void stop() {
    if (serverStarter != null) {
      serverStarter.after();
    }
    if (locatorStarter != null) {
      locatorStarter.after();
    }
  }

}
