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
package com.gemstone.gemfire.pdx;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class DistributedSystemIdDUnitTest extends JUnit4DistributedTestCase {
  
  @Override
  public void preSetUp() {
    disconnectAllFromDS(); // GEODE-558 test fails due to infection from another test
  }

  @Test
  public void testMatchingIds() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int locatorPort = createLocator(vm0, "1");
    createSystem(vm1, "1", locatorPort);
    createSystem(vm2, "1", locatorPort);
    
    checkId(vm0, 1);
    checkId(vm1, 1);
    checkId(vm2, 1);
  }
  
  @Test
  public void testInfectiousId() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    int locatorPort = createLocator(vm0, "1");
    createSystem(vm1, "-1", locatorPort);
    
    checkId(vm1, 1);
  }
  
  @Test
  public void testMismatch() {
    IgnoredException.addIgnoredException("Rejected new system node");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    int locatorPort = createLocator(vm0, "1");
    
    try {
      createSystem(vm1, "2", locatorPort);
      fail("Should have gotten an exception");
    } catch(Exception expected) {

    }
    
    checkId(vm0, 1);
  }
  
  @Test
  public void testInvalid() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    try {
      createLocator(vm0, "256");
      fail("Should have gotten an exception");
    } catch(Exception expected) {
    }

    try {
      createLocator(vm0, "aardvark");
      fail("Should have gotten an exception");
    } catch(Exception expected) {
    }
  }

  private void createSystem(VM vm, final String dsId, final int locatorPort) {

    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DISTRIBUTED_SYSTEM_ID, dsId);
        props.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        getSystem(props);
        return null;
      }
    };
    vm.invoke(createSystem);
  }
  
  private int createLocator(VM vm, final String dsId) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        Properties props = new Properties();
        props.setProperty(DISTRIBUTED_SYSTEM_ID, dsId);
        props.setProperty(MCAST_PORT, "0");
        props.setProperty(LOCATORS, "localhost[" + port + "]");
        props.setProperty(START_LOCATOR, "localhost[" + port + "]");
        getSystem(props);
        return port;
      }
    };
    return (Integer) vm.invoke(createSystem);
  }
  
  private void checkId(VM vm, final int dsId) {
    
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        DistributionManager dm = (DistributionManager) InternalDistributedSystem.getAnyInstance().getDistributionManager();
        assertEquals(dsId, dm.getDistributedSystemId());
        return null;
      }
    };
    vm.invoke(createSystem);
  }
}
