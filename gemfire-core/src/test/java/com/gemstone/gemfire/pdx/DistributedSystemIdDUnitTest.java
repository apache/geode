/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

import java.io.File;
import java.util.Properties;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * @author dsmith
 *
 */
public class DistributedSystemIdDUnitTest extends DistributedTestCase {
  
  public DistributedSystemIdDUnitTest(String name) {
    super(name);
  }

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
  
  

  public void testInfectousId() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    int locatorPort = createLocator(vm0, "1");
    createSystem(vm1, "-1", locatorPort);
    
    checkId(vm1, 1);
  }
  
  public void testMismatch() {
    addExpectedException("Rejected new system node");
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
        props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, dsId);
        props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locatorPort + "]");
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
        props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, dsId);
        props.setProperty("mcast-port", "0");
        props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
        props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "]");
        getSystem(props);
//        Locator locator = Locator.startLocatorAndDS(port, File.createTempFile("locator", ""), props);
//        system = (InternalDistributedSystem) locator.getDistributedSystem();
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
