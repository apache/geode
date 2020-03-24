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
package org.apache.geode.cache.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

public class SocketFactoryDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(2);
  private int locatorPort;
  private int serverPort;

  @Before
  public void createCluster() {
    locatorPort = cluster.startLocatorVM(0).getPort();
    serverPort = cluster.startServerVM(1, locatorPort).getPort();
  }

  @Test
  public void customSocketFactoryUsedForLocators() throws IOException {
    ClientCache client = new ClientCacheFactory()
        // Add a locator with the wrong hostname
        .addPoolLocator("notarealhostname", locatorPort)
        // Set a socket factory that switches the hostname back
        .setPoolSocketFactory(ProxySocketFactories.plainText("localhost"))
        .create();

    // Verify the socket factory switched the hostname so we can connect
    verifyConnection(client);
  }

  @Test
  public void customSocketFactoryUsedForServers() {
    ClientCache client = new ClientCacheFactory()
        // Add a locator with the wrong hostname
        .addPoolServer("notarealhostname", serverPort)
        // Set a socket factory that switches the hostname back
        .setPoolSocketFactory(ProxySocketFactories.plainText("localhost"))
        .create();


    // Verify the socket factory switched the hostname so we can connect
    verifyConnection(client);
  }

  private void verifyConnection(ClientCache client) {
    // Verify connectivity with servers
    Object functionResult =
        FunctionService.onServers(client).execute(new TestFunction()).getResult();

    assertThat(functionResult).isEqualTo(Arrays.asList("test"));
  }


  public static class TestFunction implements Function<Void>, DataSerializable {
    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public void execute(FunctionContext<Void> context) {
      context.getResultSender().lastResult("test");

    }
  }
}
