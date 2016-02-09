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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Config;
import com.gemstone.gemfire.internal.admin.Alert;
import com.gemstone.gemfire.internal.admin.AlertListener;
import com.gemstone.gemfire.internal.admin.ApplicationVM;
import com.gemstone.gemfire.internal.admin.DLockInfo;
import com.gemstone.gemfire.internal.admin.EntryValueNode;
import com.gemstone.gemfire.internal.admin.GemFireVM;
import com.gemstone.gemfire.internal.admin.GfManagerAgent;
import com.gemstone.gemfire.internal.admin.GfManagerAgentConfig;
import com.gemstone.gemfire.internal.admin.GfManagerAgentFactory;
import com.gemstone.gemfire.internal.admin.StatResource;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * This class tests the functionality of the {@linkplain com.gemstone.gemfire.internal.admin internal
 * admin} API.
 */
public class ConsoleDistributionManagerDUnitTest 
  extends CacheTestCase implements AlertListener {

  protected GfManagerAgent agent = null;
  private static boolean firstTime = true;
  
  public ConsoleDistributionManagerDUnitTest(String name) {
    super(name);
  }

//  private volatile Alert lastAlert = null;

  public void alert(Alert alert) {
    LogWriterUtils.getLogWriter().info("DEBUG: alert=" + alert);
//    this.lastAlert = alert;
  }

  public void setUp() throws Exception {
    boolean finishedSetup = false;
    IgnoredException.addIgnoredException("Error occurred while reading system log");
    try {
      if (firstTime) {
        disconnectFromDS(); //make sure there's no ldm lying around
        try { Thread.sleep(5 * 1000); } catch (InterruptedException ie) { fail("interrupted");}
        firstTime = false;
      }

      DistributionManager.isDedicatedAdminVM = true;

      super.setUp();
      populateCache();

      RemoteTransportConfig transport = null;
      {
        boolean created = !isConnectedToDS();
        InternalDistributedSystem ds = getSystem();
        transport = new RemoteTransportConfig(ds.getConfig(), DistributionManager.ADMIN_ONLY_DM_TYPE);
        if (created) {
          disconnectFromDS();
        }
      }
      // create a GfManagerAgent in the master vm.
      this.agent = GfManagerAgentFactory.
        getManagerAgent(new GfManagerAgentConfig(null, transport, LogWriterUtils.getLogWriter(), Alert.SEVERE, this, null));
      if (!agent.isConnected()) {
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return agent.isConnected();
          }
          public String description() {
            return null;
          }
        };
        Wait.waitForCriterion(ev, 60 * 1000, 200, true);
      }
      finishedSetup = true;
    } finally {
      if (!finishedSetup) {
        try {
          this.agent.disconnect();
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Throwable ignore) {
        }
        try {
          super.preTearDown();
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Throwable ignore) {
        }
        try {
          disconnectFromDS();
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Throwable ignore) {
        }
        DistributionManager.isDedicatedAdminVM = false;
      }
    }
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    this.agent.disconnect();
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    try {
      disconnectFromDS(); //make sure there's no ldm lying around
    } finally {
      DistributionManager.isDedicatedAdminVM = false;
    }
  }

  public void testGetDistributionVMType() {
    DM dm = this.agent.getDM();
    InternalDistributedMember ipaddr = dm.getId();
    assertEquals(DistributionManager.ADMIN_ONLY_DM_TYPE, ipaddr.getVmKind());
  }

  public void testAgent() {
    assertEquals("expected empty peer array", 0, agent.listPeers().length);
    int systemCount = 0;
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      systemCount += host.getSystemCount();
    }
    // note that JoinLeaveListener is not tested since it would require
    // this test to start and stop systems.
    agent.disconnect();
    assertTrue("agent should have been disconnected", !agent.isConnected());
  }

  public void testApplications() throws Exception {
    {
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          ApplicationVM[] apps = agent.listApplications();
          return apps.length >= 4;
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 60 * 1000, 200, true);
    }

    //final Serializable controllerId = getSystem().getDistributionManager().getId(); //can't do this...
    ApplicationVM[] apps = agent.listApplications();
    for (int i=0; i<apps.length; i++) {
      //if (apps[i].getId().equals(controllerId)) {
      //  continue; // skip this one; its the locator vm
      //}
      InetAddress host = apps[i].getHost();
      String appHostName = host.getHostName();
      try {
        InetAddress appHost = InetAddress.getByName(appHostName);
        assertEquals(appHost, host);
      } catch (UnknownHostException ex) {
        fail("Lookup of address for host " + appHostName
             + " failed because " + ex);
      }

      StatResource[] stats = apps[i].getStats(null);
      assertTrue(stats.length > 0);

      Config conf = apps[i].getConfig();
      String[] attNames = conf.getAttributeNames();
      boolean foundIt = false;      
      for (int j=0; j<attNames.length; j++) {
        if (attNames[j].equals(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME)) {
          foundIt = true;
          assertEquals(conf.getAttribute(attNames[j]), "true");
          break;
        }
      }
      assertTrue(foundIt);

      String[] logs = apps[i].getSystemLogs();
      assertTrue(logs.length > 0);
//      assertTrue(logs[0].length() > 0);

      {
        VM vm = findVMForAdminObject(apps[i]);
//         getLogWriter().info("DEBUG: found VM " + vm +
//                             " which corresponds to ApplicationVM "
//                             + apps[i] + " in testApplications");
        assertNotNull(vm);
        String lockName = "cdm_testlock" + i;
        assertTrue(acquireDistLock(vm, lockName));
        DLockInfo[] locks = apps[i].getDistributedLockInfo();
        assertTrue(locks.length > 0);
        boolean foundLock = false;
        for (int j=0; j<locks.length; j++) {
          if (locks[j].getLockName().equals(lockName)) {
            foundLock = true;
            assertTrue(locks[j].isAcquired());
          }
        }
        assertTrue(foundLock);
      }
      
      Region[] roots = apps[i].getRootRegions();
      if (roots.length == 0) {
        LogWriterUtils.getLogWriter().info("DEBUG: testApplications: apps[" + i + "]=" + apps[i] + " did not have a root region");
      } else {
        Region root = roots[0];
        assertNotNull(root);
        assertEquals("root", root.getName());
        assertEquals("/root", root.getFullPath());
        RegionAttributes attributes = root.getAttributes();
        assertNotNull(attributes);
        if (attributes.getStatisticsEnabled()) {
          assertNotNull(root.getStatistics());
        }
        final Set subregions = root.subregions(false);
        assertEquals(3, subregions.size());
        assertEquals(2, root.keys().size());
        Region.Entry entry = root.getEntry("cacheObj1");
        assertNotNull(entry);
        if (attributes.getStatisticsEnabled()) {
          assertNotNull(entry.getStatistics());
        }
        assertTrue(entry.getValue().equals("null"));

        /// test lightweight inspection;
        entry = root.getEntry("cacheObj2");
        assertNotNull(entry);
        Object val = entry.getValue();
        assertTrue(val instanceof String);
        assertTrue(((String)val).indexOf("java.lang.StringBuffer") != -1);

        /// test physical inspection
        //getLogWriter().info("DEBUG: Starting test of physical cache value inspection");
        apps[i].setCacheInspectionMode(GemFireVM.PHYSICAL_CACHE_VALUE);
        entry = root.getEntry("cacheObj2");
        assertNotNull(entry);
        val = entry.getValue();
        assertTrue(val instanceof EntryValueNode);
        EntryValueNode node = (EntryValueNode)val;
        String type = node.getType();
        assertTrue(type.indexOf("java.lang.StringBuffer") != -1);
        assertTrue(!node.isPrimitiveOrString());
        EntryValueNode[] fields = node.getChildren();
        assertNotNull(fields);
        LogWriterUtils.getLogWriter().warning("The tests use StringBuffers for values which might be implmented differently in jdk 1.5");
       // assertTrue(fields.length > 0);
        
        /// test destruction in the last valid app
        int lastIdx = apps.length-1;

        /*if (i == lastIdx ||
          (i == lastIdx-1 && apps[lastIdx].getId().equals(controllerId))) {*/

        if (i == lastIdx) {
          //getLogWriter().info("DEBUG: starting region destroy from admin apis");
          final int expectedSize = subregions.size() - 1;
          final Region r = (Region)subregions.iterator().next();
          final Region rr = root;
          r.destroyRegion();
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              Set s = rr.subregions(false);
              return s.size() == expectedSize;
            }
            public String description() {
              return "Waited 20 seconds for region " + r.getFullPath() + "to be destroyed.";
            }
          };
          Wait.waitForCriterion(ev, 20 * 1000, 200, true);
        }
      }
    }
  }

  private void populateCache() {

    AttributesFactory fact = new AttributesFactory();
    fact.setScope(Scope.DISTRIBUTED_NO_ACK);
    final RegionAttributes rAttr = fact.create();

    final SerializableRunnable populator = new SerializableRunnable(){
        public void run() {
          try {
            createRegion("cdm-testSubRegion1", rAttr);
            createRegion("cdm-testSubRegion2", rAttr);
            createRegion("cdm-testSubRegion3", rAttr);
            remoteCreateEntry("", "cacheObj1", null);          
            StringBuffer  val = new StringBuffer("userDefValue1");
            remoteCreateEntry("", "cacheObj2", val);
          } catch (CacheException ce) {
            fail("Exception while populating cache:\n" + ce);
          }
        }
    };
    
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);      

      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(populator);
      }
    }
  }



  /**
   * Puts (or creates) a value in a region named <code>regionName</code>
   * named <code>entryName</code>.
   */
  protected void remoteCreateEntry(String regionName,
                                          String entryName,
                                          Object value)
    throws CacheException {
    
    Region root = getRootRegion();
    Region region = root.getSubregion(regionName);
    region.create(entryName, value);
    
    
    LogWriterUtils.getLogWriter().info("Put value " + value + " in entry " +
                        entryName + " in region '" +
                        region.getFullPath() +"'");
    
  }
  
//  private long getConId(VM vm) {
//      return vm.invokeLong(this.getClass(), "remoteGetConId");
//  }
  /**
   * Accessed via reflection.  DO NOT REMOVE
   * @return
   */
  protected static long remoteGetConId() {
    return InternalDistributedSystem.getAnyInstance().getId();
  }

  private boolean acquireDistLock(VM vm, String lockName) {
    return vm.invokeBoolean(this.getClass(), "remoteAcquireDistLock",
                            new Object[]{lockName});
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   * @param lockName
   * @return
   */
  protected static boolean remoteAcquireDistLock(String lockName) {
    String serviceName = "cdmtest_service";
    DistributedLockService service =
      DistributedLockService.getServiceNamed(serviceName);
    if (service == null) {
      service = DistributedLockService.
        create(serviceName, InternalDistributedSystem.getAnyInstance());
    }
    assertNotNull(service);
    try {
      return service.lock(lockName, 1000, 3000);
    } catch (Exception e) {
      throw new RuntimeException("DEBUG: remoteAcquireDistLock", e);
      //return false;
    }
  }

  private VM findVMForAdminObject(GemFireVM adminObj) {
    for (int i=0; i<Host.getHostCount(); i++) {
      Host host = Host.getHost(i);
      for (int j=0; j<host.getVMCount(); j++) {
        VM vm = host.getVM(j);
        InternalDistributedMember id = getJavaGroupsIdForVM(vm);
        if (adminObj.getId().equals(id)) {
          return vm;
        }
      }
    }
    return null;
  }

  private InternalDistributedMember getJavaGroupsIdForVM(VM vm) {
    return (InternalDistributedMember)vm.invoke(this.getClass(), "remoteGetJavaGroupsIdForVM");
  }
  
  /**
   * INVOKED VIA REFLECTION
   * 
   * @return
   */
  protected static InternalDistributedMember remoteGetJavaGroupsIdForVM() {
    InternalDistributedSystem sys = 
      InternalDistributedSystem.getAnyInstance();
    return sys.getDistributionManager().getDistributionManagerId();
    
  }
}
