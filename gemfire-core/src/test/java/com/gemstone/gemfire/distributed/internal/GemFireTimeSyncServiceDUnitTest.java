/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.distributed.internal;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.util.Properties;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.protocols.GemFireTimeSync;
import com.gemstone.org.jgroups.stack.Protocol;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * @author shobhit
 *
 */
public class GemFireTimeSyncServiceDUnitTest extends DistributedTestCase {
  
  /**
   * @param name
   */
  public GemFireTimeSyncServiceDUnitTest(String name) {
    super(name);
  }

  /**
   * After coordinator's sudden death and restart, this test verifies if
   * {@link GemFireTimeSync} service thread is stopped in old coordinator or
   * not.
   */
  public void testCoordinatorSyncThreadCancellation() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // Locator for this Test DS.
    VM vm1 = host.getVM(1); // Peer member.
    VM vm2 = host.getVM(2); // Peer member.

    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(host); 
    
    final Properties props = new Properties();
    props.setProperty("locators", host0 + "[" + locatorPort + "]");
    props.setProperty("mcast-port", "0");
    props.setProperty("jmx-manager", "false");
    props.setProperty("enable-network-partition-detection", "true");
    props.setProperty("log-level", getDUnitLogLevel());
    props.put("member-timeout", "2000");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    try {
      // Start distributed system in vm0(locator) vm1(data node).
      vm0.invoke(new CacheSerializableRunnable("Starting vm0") {
        @Override
        public void run2() {
  
          // Disconnect from any existing DS.
          try {
            system.disconnect();
          } catch (Exception ex) {
            // Let it go.
          }
          File myLocatorLogFile = new File("locator-"+locatorPort+".log"); 
  
          try {
            Locator.startLocatorAndDS(locatorPort, myLocatorLogFile, props);
          } catch (IOException e) {
            fail("New locator startup failed on port: "+locatorPort, e);
          }
  
        }
      });
   
      // Add a new member to trigger VIEW_CHANGE.
      vm1.invoke(new CacheSerializableRunnable("Starting vm1") {
        @Override
        public void run2() {
          // Disconnect from any existing DS.
          disconnectFromDS();
          
          DistributedSystem.connect(props);
        }
      });
      
      // Add a new member to trigger VIEW_CHANGE.
      vm2.invoke(new CacheSerializableRunnable("Starting vm1") {
        @Override
        public void run2() {
          // Disconnect from any existing DS.
          disconnectFromDS();
          
          DistributedSystem.connect(props);
        }
      });

      // Make current coordinator die.
      vm0.invoke(new CacheSerializableRunnable("Shutdown my locator") {
        
        @Override
        public void run2() throws CacheException {
          Locator loc = InternalLocator.getLocators().iterator().next();
          InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
          JChannel jchannel = MembershipManagerHelper.getJChannel(system);
          Protocol prot = jchannel.getProtocolStack().findProtocol("GemFireTimeSync");
          GemFireTimeSync gts = (GemFireTimeSync)prot;
  
          // Verify if Time Service is running.
          assertFalse(gts.isServiceThreadCancelledForTest());
  
          loc.stop();
          system.disconnect();
        }
      });
  
      // VM1 must become new coordinator.
      vm1.invoke(new CacheSerializableRunnable("Verify vm1 coordinator") {
        @Override
        public void run2() {
          InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
          DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
          
          assertTrue(coord.equals(system.getDistributedMember()));
        }
      });

      // VM1 must become new coordinator.
      vm1.invoke(new CacheSerializableRunnable("Verify vm1 coordinator") {
        @Override
        public void run2() {
          InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
          DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
          
          assertTrue(coord.equals(system.getDistributedMember()));
  
          JChannel jchannel = MembershipManagerHelper.getJChannel(system);
          Protocol prot = jchannel.getProtocolStack().findProtocol("GemFireTimeSync");
          GemFireTimeSync gts = (GemFireTimeSync)prot;
  
          int maxAttempts =20;
          while (maxAttempts > 0 && gts.isServiceThreadCancelledForTest()) {
            maxAttempts--;
            pause(100);
          }
  
          // Verify if Time Service is running.
          assertFalse(gts.isServiceThreadCancelledForTest());
        }
      });

      vm0.invoke(new CacheSerializableRunnable("Restart my locator and verify it's coordinator again") {
        
        @Override
        public void run2() throws CacheException {
          try {
            Locator.startLocatorAndDS(locatorPort, null, props);
          } catch (IOException e) {
            fail("Restart of new locator failed on port: "+locatorPort, e);
          }
          
          InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
          DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
          
          assertTrue(coord.equals(system.getDistributedMember()));
          
          JChannel jchannel = MembershipManagerHelper.getJChannel(system);
          Protocol prot = jchannel.getProtocolStack().findProtocol("GemFireTimeSync");
          GemFireTimeSync gts = (GemFireTimeSync)prot;
  
          int maxAttempts = 20;
          while (maxAttempts > 0 && gts.isServiceThreadCancelledForTest()) {
            maxAttempts--;
            pause(100);
          }
          assertFalse(gts.isServiceThreadCancelledForTest());
        }
      });
  
      // After receiving VIEW_CHANGE locator should cancel its GemFireTimeSync service thread.
      vm1.invoke(new CacheSerializableRunnable("Verify vm1 is not coordinator") {
        
        @Override
        public void run2() throws CacheException {
          InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
  
          JChannel jchannel = MembershipManagerHelper.getJChannel(system);
          Protocol prot = jchannel.getProtocolStack().findProtocol("GemFireTimeSync");
          GemFireTimeSync gts = (GemFireTimeSync)prot;
  
          // Verify if Time Service is NOT running and service thread is cancelled.
          assertTrue(gts.isServiceThreadCancelledForTest());
        }
      });
      
    } catch (Exception ex) {
      fail("Test failed!", ex);
    } finally {
      // Shutdown locator and clean vm0 for other tests.
      vm0.invoke(new CacheSerializableRunnable("Shutdown locator") {
        
        @Override
        public void run2() throws CacheException {
          try {
            InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
            if (system != null)
              system.disconnect();
          } catch (Exception e) {
            fail("Stoping locator failed", e);
          }
        }
      });
      vm1.invoke(new CacheSerializableRunnable("Shutdown vm1") {
        
        @Override
        public void run2() throws CacheException {
          try {
            InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
            if (system != null)
              system.disconnect();
          } catch (Exception e) {
            fail("Stoping vm1 failed", e);
          }
        }
      });
      
      vm2.invoke(new CacheSerializableRunnable("Shutdown vm2") {
        
        @Override
        public void run2() throws CacheException {
          try {
            InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
            if (system != null)
              system.disconnect();
          } catch (Exception e) {
            fail("Stoping vm2 failed", e);
          }
        }
      });
    }
  }
}
