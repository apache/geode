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
package com.gemstone.gemfire.internal.cache.execute;

import java.util.Arrays;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Base class for function tests that use client server. This
 * class takes care of setting up the client server topology.
 */
public abstract class FunctionServiceClientBase extends FunctionServiceBase {

  public ClientCache createServersAndClient(int numberOfServers) {
    int[] ports = new int[numberOfServers];
    Host host = Host.getHost(0);
    for(int i =0; i < numberOfServers; i++) {
      VM vm = host.getVM(i);
      ports[i] = createServer(vm);
    }

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    Arrays.stream(ports).forEach(port -> {
      clientCacheFactory.addPoolServer("localhost", port);
    });
    configureClient(clientCacheFactory);
    return getClientCache(clientCacheFactory);
  }

  protected Integer createServer(final VM vm) {
    return vm.invoke(() -> {
      CacheServer server = getCache().addCacheServer();
      server.setPort(0);
      server.start();
      return server.getPort();
    });
  }

  public void configureClient(ClientCacheFactory cacheFactory) {
    //do nothing
  }
}
