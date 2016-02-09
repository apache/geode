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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class ProductUseLogDUnitTest extends DistributedTestCase {

  public ProductUseLogDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties p = super.getDistributedSystemProperties();
    p.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    return p;
  }
  
  public void testMembershipMonitoring() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    // use a locator so we will monitor server load and record member->server mappings
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Properties p = new Properties();
    p.put(DistributionConfig.START_LOCATOR_NAME, "localhost["+locatorPort+"],peer=false");
    p.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    InternalDistributedSystem system = getSystem(p);
    
    InternalLocator locator = (InternalLocator)Locator.getLocator();
    // server location is forced on for product use logging
    assertTrue(locator.isServerLocator());
    
    File logFile = new File("locator"+locatorPort+"views.log");
    // the locator should have already created this file
    assertTrue(logFile.exists());
    
    assertTrue(logFile.exists());
    vm0.invoke(new SerializableRunnable("get system") {
      public void run() {
        InternalDistributedSystem system = getSystem();
        Cache cache = CacheFactory.create(system);
        CacheServer server = cache.addCacheServer();
        server.setPort(0);
        try {
          server.start();
        } catch (IOException e) {
          Assert.fail("failed to start server", e);
        }
      }
    });

    // wait for the server info to be received and logged 
//    pause(2 * BridgeServerImpl.FORCE_LOAD_UPDATE_FREQUENCY * 1000);

    system.disconnect();

    String logContents = readFile(logFile);
    assertTrue("expected " + logFile + " to contain a View", logContents.contains("View"));
    assertTrue("expected " + logFile + " to contain 'server summary'", logContents.contains("server summary"));
  }
  
  private String readFile(File file) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(file));
    StringBuilder sb = new StringBuilder(10000);
    String line;
    while ( (line = reader.readLine()) != null ) {
      sb.append(line).append(File.separator);
    }
    return sb.toString();
  }

  @Override
  protected final void preTearDown() throws Exception {
    disconnectAllFromDS();
  }
}
