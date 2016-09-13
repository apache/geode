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
package com.gemstone.gemfire.internal.cache;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/** A test of 46438 - missing response to an update attributes message */
@Category(DistributedTest.class)
public class ConnectDisconnectDUnitTest extends JUnit4CacheTestCase {
  
  private IgnoredException ex;

  // see bugs #50785 and #46438
  @Test
  public void testManyConnectsAndDisconnects() throws Throwable {
//    invokeInEveryVM(new SerializableRunnable() {
//
//      @Override
//      public void run() {
//        Log.setLogWriterLevel("info");
//      }
//    });

// uncomment these lines to use stand-alone locators
//     int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(4);
//     setLocatorPorts(ports);

    for(int i = 0; i < 20; i++) {
      LogWriterUtils.getLogWriter().info("Test run: " + i);
      runOnce();
      tearDown();
      setUp();
    }
  }
  
  
  static int LOCATOR_PORT;
  static String LOCATORS_STRING;

  static int[] locatorPorts;
  
  public void setLocatorPorts(int[] ports) {
    DistributedTestUtils.deleteLocatorStateFile(ports);
    String locators = "";
    for (int i=0; i<ports.length; i++) {
      if (i > 0) {
        locators += ",";
      }
      locators += "localhost["+ports[i]+"]";
    }
    final String locators_string = locators;
    for (int i=0; i<ports.length; i++) {
      final int port = ports[i];
      Host.getHost(0).getVM(i).invoke(new SerializableRunnable("set locator port") {
        public void run() {
          LOCATOR_PORT = port;
          LOCATORS_STRING = locators_string;
        }
      });
    }
    locatorPorts = ports;
  }
  
  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    if (locatorPorts != null) {
      DistributedTestUtils.deleteLocatorStateFile(locatorPorts);
    }
  }

  /**
   * This test creates 4 vms and starts a cache in each VM. If that doesn't hang, it destroys the DS in all
   * vms and recreates the cache.
   * @throws Throwable 
   */
  public void runOnce() throws Throwable {
    
    int numVMs = 4;
    
    VM[] vms = new VM[numVMs];
    
    for(int i= 0; i < numVMs; i++) {
//      if(i == 0) {
//        vms[i] = Host.getHost(0).getVM(4);
//      } else {
        vms[i] = Host.getHost(0).getVM(i);
//      }
    }
    
    AsyncInvocation[] asyncs = new AsyncInvocation[numVMs];
    for(int i= 0; i < numVMs; i++) {
      asyncs[i] = vms[i].invokeAsync(new SerializableRunnable("Create a cache") {
        @Override
        public void run() {
//          try {
//            JGroupMembershipManager.setDebugJGroups(true);
          getCache();
//          } finally {
//            JGroupMembershipManager.setDebugJGroups(false);
//          }
        }
      });
    }
    
    
    for(int i= 0; i < numVMs; i++) {
      asyncs[i].getResult();
//      try {
//        asyncs[i].getResult(30 * 1000);
//      } catch(TimeoutException e) { 
//        getLogWriter().severe("DAN DEBUG - we have a hang");
//        dumpAllStacks();
//        fail("DAN - WE HIT THE ISSUE",e);
//        throw e;
//      }
    }
    
    disconnectAllFromDS();
  }


  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(CONSERVE_SOCKETS, "false");
    if (LOCATOR_PORT > 0) {
      props.setProperty(START_LOCATOR, "localhost[" + LOCATOR_PORT + "]");
      props.setProperty(LOCATORS, LOCATORS_STRING);
    }
    return props;
  }
  
  
}
