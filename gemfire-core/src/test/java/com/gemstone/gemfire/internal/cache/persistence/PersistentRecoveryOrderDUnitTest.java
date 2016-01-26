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
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Ignore;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.persistence.ConflictingPersistentDataException;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.cache.persistence.PersistentReplicatesOfflineException;
import com.gemstone.gemfire.cache.persistence.RevokedPersistentDataException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.LockServiceDestroyedException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.AbstractUpdateOperation.AbstractUpdateMessage;
import com.gemstone.gemfire.internal.cache.DestroyRegionOperation.DestroyRegionMessage;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.RequestImageMessage;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionHolder;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This is a test of how persistent distributed
 * regions recover. This test makes sure that when
 * multiple VMs are persisting the same region, they recover
 * with the latest data during recovery.
 * 
 * @author dsmith
 *
 */
public class PersistentRecoveryOrderDUnitTest extends PersistentReplicatedTestBase {
  public PersistentRecoveryOrderDUnitTest(String name) {
    super(name);
  }

  public static void resetAckWaitThreshold() {
    if (SAVED_ACK_WAIT_THRESHOLD != null) {
      System.setProperty("gemfire.ack_wait_threshold", SAVED_ACK_WAIT_THRESHOLD);
    }
  }
  
  /**
   * Tests to make sure that a persistent region will wait
   * for any members that were online when is crashed before starting up.
   * @throws Throwable
   */
  public void testWaitForLatestMember() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    
    putAnEntry(vm0);
    
    getLogWriter().info("closing region in vm0");
    closeRegion(vm0);
    
    updateTheEntry(vm1);
    
    getLogWriter().info("closing region in vm1");
    closeRegion(vm1);
    
    
    //This ought to wait for VM1 to come back
    getLogWriter().info("Creating region in VM0");
    AsyncInvocation future = createPersistentRegionAsync(vm0);
    
    waitForBlockedInitialization(vm0);
    
    assertTrue(future.isAlive());
    
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    
    future.join(MAX_WAIT);
    if(future.isAlive()) {
      fail("Region not created within " + MAX_WAIT);
    }
    if(future.exceptionOccurred()) {
      throw new Exception(future.getException());
    }
    
    checkForEntry(vm0);
    checkForEntry(vm1);
    
    checkForRecoveryStat(vm1, true);
    checkForRecoveryStat(vm0, false);
  }
  
  /**
   * Tests to make sure that we stop waiting for a member
   * that we revoke.
   * @throws Throwable
   */
  public void testRevokeAMember() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    
    putAnEntry(vm0);
    
    vm0.invoke(new SerializableRunnable("Check for waiting regions") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        PersistentMemberManager mm = cache.getPersistentMemberManager();
        Map<String, Set<PersistentMemberID>> waitingRegions = mm.getWaitingRegions();
        assertEquals(0, waitingRegions.size());
      }
    });
    
    getLogWriter().info("closing region in vm0");
    closeRegion(vm0);
    
    updateTheEntry(vm1);
    
    getLogWriter().info("closing region in vm1");
    closeCache(vm1);
    
    
    //This ought to wait for VM1 to come back
    getLogWriter().info("Creating region in VM0");
    AsyncInvocation future = createPersistentRegionAsync(vm0);
    
    waitForBlockedInitialization(vm0);
    
    assertTrue(future.isAlive());
    
    vm2.invoke(new SerializableRunnable("Revoke the member") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
          getLogWriter().info("waiting members=" + missingIds);
          assertEquals(1, missingIds.size());
          PersistentID missingMember = missingIds.iterator().next();
          adminDS.revokePersistentMember(
              missingMember.getUUID());
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
    
    future.join(MAX_WAIT);
    if(future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    
    if(future.exceptionOccurred()) {
      throw new Exception(future.getException());
    }
    
    checkForRecoveryStat(vm0, true);
    
    
    
    
    //Check to make sure we recovered the old
    //value of the entry.
    SerializableRunnable checkForEntry = new SerializableRunnable("check for the entry") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        assertEquals("B", region.get("A"));
      }
    };
    vm0.invoke(checkForEntry);
    
    //Now, we should not be able to create a region
    //in vm1, because the this member was revoked
    getLogWriter().info("Creating region in VM1");
    ExpectedException e = addExpectedException(RevokedPersistentDataException.class.getSimpleName(), vm1);
    try {
      createPersistentRegion(vm1);
      fail("We should have received a split distributed system exception");
    } catch(RuntimeException expected) {
      if(!(expected.getCause() instanceof RevokedPersistentDataException)) {
        throw expected;
      }
    } finally {
      e.remove();
    }
    
    closeCache(vm1);
    //Restart vm0
    closeCache(vm0);
    createPersistentRegion(vm0);
    
    //Make sure we still get a RevokedPersistentDataException
    //TODO - RVV - This won't work until we actually persist the revoked
    //members. I want to refactor to use disk store id before we do that.
//    getLogWriter().info("Creating region in VM1");
//    e = addExpectedException(RevokedPersistentDataException.class.getSimpleName(), vm1);
//    try {
//      createPersistentRegion(vm1);
//      fail("We should have received a split distributed system exception");
//    } catch(RuntimeException expected) {
//      if(!(expected.getCause() instanceof RevokedPersistentDataException)) {
//        throw expected;
//      }
//      //Do nothing
//    } finally {
//      e.remove();
//    }
  }
  
  /**
   * Tests to make sure that we can revoke a member
   * before initialization, and that member will stay revoked
   * @throws Throwable
   */
  public void testRevokeAHostBeforeInitialization() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    
    putAnEntry(vm0);
    
    vm0.invoke(new SerializableRunnable("Check for waiting regions") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        PersistentMemberManager mm = cache.getPersistentMemberManager();
        Map<String, Set<PersistentMemberID>> waitingRegions = mm.getWaitingRegions();
        assertEquals(0, waitingRegions.size());
      }
    });
    
    getLogWriter().info("closing region in vm0");
    closeRegion(vm0);
    
    updateTheEntry(vm1);
    
    getLogWriter().info("closing region in vm1");
    closeRegion(vm1);
    
    final File dirToRevoke = getDiskDirForVM(vm1);
    vm2.invoke(new SerializableRunnable("Revoke the member") {
      
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
        config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
        adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
        adminDS.connect();
        adminDS.revokePersistentMember(InetAddress.getLocalHost(), dirToRevoke.getCanonicalPath());
        } catch(Exception e) {
          fail("Unexpected exception", e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
    
    //This shouldn't wait, because we revoked the member
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    
    checkForRecoveryStat(vm0, true);
    
    //Check to make sure we recovered the old
    //value of the entry.
    SerializableRunnable checkForEntry = new SerializableRunnable("check for the entry") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        assertEquals("B", region.get("A"));
      }
    };
    vm0.invoke(checkForEntry);
    
    //Now, we should not be able to create a region
    //in vm1, because the this member was revoked
    getLogWriter().info("Creating region in VM1");
    ExpectedException e = addExpectedException(RevokedPersistentDataException.class.getSimpleName(), vm1);
    try {
      createPersistentRegion(vm1);
      fail("We should have received a split distributed system exception");
    } catch(RuntimeException expected) {
      if(!(expected.getCause() instanceof RevokedPersistentDataException)) {
        throw expected;
      }
      //Do nothing
    } finally {
      e.remove();
    }
  }

  /**
   * Test which members show up in the list of members we're waiting on.
   * @throws Throwable
   */
  public void testWaitingMemberList() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    createPersistentRegion(vm2);
    
    putAnEntry(vm0);
    
    vm0.invoke(new SerializableRunnable("Check for waiting regions") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        PersistentMemberManager mm = cache.getPersistentMemberManager();
        Map<String, Set<PersistentMemberID>> waitingRegions = mm.getWaitingRegions();
        assertEquals(0, waitingRegions.size());
      }
    });
    
    getLogWriter().info("closing region in vm0");
    closeRegion(vm0);
    
    updateTheEntry(vm1);
    
    getLogWriter().info("closing region in vm1");
    closeRegion(vm1);
    
    updateTheEntry(vm2, "D");
    
    getLogWriter().info("closing region in vm2");
    closeRegion(vm2);
    
    
    //These ought to wait for VM2 to come back
    getLogWriter().info("Creating region in VM0");
    AsyncInvocation future0 = createPersistentRegionAsync(vm0);
    
    waitForBlockedInitialization(vm0);
    assertTrue(future0.isAlive());
    
    getLogWriter().info("Creating region in VM1");
    final AsyncInvocation future1 = createPersistentRegionAsync(vm1);
    waitForBlockedInitialization(vm1);
    assertTrue(future1.isAlive());
    
    vm3.invoke(new SerializableRunnable("check waiting members") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
          getLogWriter().info("waiting members=" + missingIds);
          assertEquals(1, missingIds.size());
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
    
    vm1.invoke(new SerializableRunnable("close cache") {
      
      public void run() {
        getCache().close();
      }
    });
    
    waitForCriterion(new WaitCriterion() {
      
      public boolean done() {
        return !future1.isAlive();
      }
      
      public String description() {
        return "Waiting for blocked initialization to terminate because the cache was closed.";
      }
    }, 30000, 500, true );

    //Now we should be missing 2 members
    vm3.invoke(new SerializableRunnable("check waiting members again") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          final AdminDistributedSystem connectedDS = adminDS;
          waitForCriterion(new WaitCriterion() {

            public String description() {
              return "Waiting for waiting members to have 2 members";
            }

            public boolean done() {
              Set<PersistentID> missingIds;
              try {
                missingIds = connectedDS.getMissingPersistentMembers();
              } catch (AdminException e) {
                throw new RuntimeException(e);
              }
              return 2 == missingIds.size();
            }
            
          }, MAX_WAIT, 500, true);
          
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }
  
  /**
   * Use Case
   * AB are alive
   * A crashes.
   * B crashes.
   * B starts up. It should not wait for A.
   * @throws Throwable
   */
  public void testDontWaitForOldMember() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    
    putAnEntry(vm0);
    
    closeRegion(vm0);
    
    updateTheEntry(vm1);
    
    closeRegion(vm1);
    
    
    //This shouldn't wait for vm0 to come back
    createPersistentRegion(vm1);
    
    checkForEntry(vm1);
    
    checkForRecoveryStat(vm1, true);
  }
  
  /**
   * Tests that if two members crash simultaneously, they
   * negotiate which member should initialize with what is
   * on disk and which member should copy data from that member.
   * @throws Throwable
   */
  public void testSimultaneousCrash() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    putAnEntry(vm0);
    updateTheEntry(vm1);
    
    
    //Copy the regions as they are with both
    //members online.
    backupDir(vm0);
    backupDir(vm1);
    
    //destroy the members
    closeCache(vm0);
    closeCache(vm1);
    
    //now restore from backup
    restoreBackup(vm0);
    restoreBackup(vm1);
    
  //This ought to wait for VM1 to come back
    getLogWriter().info("Creating region in VM0");
    AsyncInvocation future = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertTrue(future.isAlive());
    
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    
    future.join(MAX_WAIT);
    if(future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if(future.exceptionOccurred()) {
      throw new Exception(future.getException());
    }
    
    checkForEntry(vm0);
    checkForEntry(vm1);
  }
  
  /**
   * Tests that persistent members pass along the list
   * of crashed members to later persistent members.
   * Eg.
   * AB are running
   * A crashes
   * C is tarted
   * B crashes
   * C crashes
   * AC are started, they should figure out who
   * has the latest data, without needing B. 
   */
  public void testTransmitCrashedMembers() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    
    putAnEntry(vm0);
    
    closeRegion(vm0);
   
    //VM 2 should be told about the fact
    //that VM1 has crashed.
    createPersistentRegion(vm2);
    
    updateTheEntry(vm1);
    
    closeRegion(vm1);
    
    closeRegion(vm2);
    
    
    //This ought to wait for VM1 to come back
    AsyncInvocation future = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertTrue(future.isAlive());
    
    //VM2 has the most recent data, it should start
    createPersistentRegion(vm2);
    
    //VM0 should be informed that VM2 is older, so it should start
    future.getResult(MAX_WAIT);
    
    checkForEntry(vm0);
    checkForEntry(vm2);
  }

  /**
   * Tests that a persistent region cannot recover from 
   * a non persistent region.
   */
  public void testRecoverFromNonPeristentRegion() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPersistentRegion(vm0);
    createNonPersistentRegion(vm1);
    
    putAnEntry(vm0);
    
    closeRegion(vm0);
    
    try {
      updateTheEntry(vm1);
      fail("expected PersistentReplicatesOfflineException not thrown");
    } catch (Exception expected) {
      if (!(expected.getCause() instanceof PersistentReplicatesOfflineException)) {
        throw expected;
      }
    }
    
    //This should initialize from vm1
    createPersistentRegion(vm0);
    
    checkForRecoveryStat(vm0, true);
    
    updateTheEntry(vm1);
    checkForEntry(vm0);
    checkForEntry(vm1);
  }

  public void testFinishIncompleteInitializationNoSend() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    //Add a hook which will disconnect the DS before sending a prepare message
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          
          @Override
          public void beforeSendMessage(DistributionManager dm,
              DistributionMessage message) {
           if(message instanceof PrepareNewPersistentMemberMessage) {
             DistributionMessageObserver.setInstance(null);
             getSystem().disconnect();
           }
          }
          
          @Override
          public void afterProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            
          }
        });
      }
    });
    createPersistentRegion(vm0);
    
    putAnEntry(vm0);
    updateTheEntry(vm0);
    
    try {
      createPersistentRegion(vm1);
    } catch(Exception e) {
      if(!(e.getCause() instanceof DistributedSystemDisconnectedException)) {
        throw e;
      }
    }
    
    closeRegion(vm0);
    
    //This wait for VM0 to come back
    AsyncInvocation async1 = createPersistentRegionAsync(vm1);
    
    waitForBlockedInitialization(vm1);
    
    createPersistentRegion(vm0);
    
    async1.getResult();
    
    checkForEntry(vm1);
    
    vm0.invoke(new SerializableRunnable("check for offline members") {
      public void run() {
        Cache cache = getCache();
        DistributedRegion region = (DistributedRegion) cache.getRegion(REGION_NAME);
        PersistentMembershipView view = region.getPersistenceAdvisor().getMembershipView();
        DiskRegion dr = region.getDiskRegion();
        
        assertEquals(Collections.emptySet(), dr.getOfflineMembers());
        assertEquals(1, dr.getOnlineMembers().size());
      }
    });
  }

  HashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> getAllMemberToVersion(RegionVersionVector rvv) {
    HashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> allMemberToVersion = new HashMap(rvv.getMemberToVersion());
    RegionVersionHolder localHolder = rvv.getLocalExceptions().clone();
    localHolder.setVersion(rvv.getCurrentVersion());
    allMemberToVersion.put((DiskStoreID)rvv.getOwnerId(), localHolder);
    return allMemberToVersion;
  }

  protected Object getEntry(VM vm, final String key) {
    SerializableCallable getEntry = new SerializableCallable("get entry") {
      
      public Object call() throws Exception {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        return region.get(key);
      }
    };
    
    return (vm.invoke(getEntry));
  }

  protected RegionVersionVector getRVV(VM vm) throws IOException, ClassNotFoundException {
    SerializableCallable createData = new SerializableCallable("getRVV") {

      public Object call() throws Exception {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion(REGION_NAME);
        RegionVersionVector rvv = region.getVersionVector();
        rvv = rvv.getCloneForTransmission();
        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);

        //Using gemfire serialization because 
        //RegionVersionVector is not java serializable
        DataSerializer.writeObject(rvv, hdos);
        return hdos.toByteArray();
      }
    };
    byte[] result= (byte[]) vm.invoke(createData);
    ByteArrayInputStream bais = new ByteArrayInputStream(result);
    return DataSerializer.readObject(new DataInputStream(bais));
  }
  
  protected AsyncInvocation createPersistentRegionAsync(final VM vm, final boolean diskSynchronous) {
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File dir = getDiskDirForVM(vm);
        dir.mkdirs();
        dsf.setDiskDirs(new File[] {dir});
        dsf.setMaxOplogSize(1);
        DiskStore ds = dsf.create(REGION_NAME);
        RegionFactory rf = new RegionFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDiskSynchronous(diskSynchronous);
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.create(REGION_NAME);
      }
    };
    return vm.invokeAsync(createRegion);
  }

  public void testPersistConflictOperations() throws Throwable {
    doTestPersistConflictOperations(true);
  }
  
  public void testPersistConflictOperationsAsync() throws Throwable {
    doTestPersistConflictOperations(false);
  }

  /**
   * vm0 and vm1 are peers, each holds a DR. 
   * They do put to the same key for different value at the same time. 
   * Use DistributionMessageObserver.beforeSendMessage to hold on the 
   * distribution message. One of the member will persist the conflict version
   * tag, while another member will persist both of the 2 operations.
   * Overall, their RVV should match after the operations.  
   */
  public void doTestPersistConflictOperations(boolean diskSync) throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    //Add a hook which will disconnect the DS before sending a prepare message
    SerializableRunnable addObserver = new SerializableRunnable() {
      public void run() {
        // System.setProperty("disk.TRACE_WRITES", "true");
        // System.setProperty("disk.TRACE_RECOVERY", "true");
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          
          @Override
          public void beforeSendMessage(DistributionManager dm,
              DistributionMessage message) {
            if (message instanceof AbstractUpdateMessage) {
              try {
                Thread.sleep(2000);
                getCache().getLogger().info("testPersistConflictOperations, beforeSendMessage");
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }
          
          @Override
          public void afterProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if (message instanceof AbstractUpdateMessage) {
              getCache().getLogger().info("testPersistConflictOperations, beforeSendMessage");
              DistributionMessageObserver.setInstance(null);
            }
          }
        });
      }
    };
    vm0.invoke(addObserver);
    vm1.invoke(addObserver);

    AsyncInvocation future0 = createPersistentRegionAsync(vm0, diskSync);
    AsyncInvocation future1 = createPersistentRegionAsync(vm1, diskSync);
    future0.join(MAX_WAIT);
    future1.join(MAX_WAIT);
//    createPersistentRegion(vm0);
//    createPersistentRegion(vm1);
    
    AsyncInvocation ins0 = vm0.invokeAsync(new SerializableRunnable("change the entry") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", "vm0");
      }
    });
    AsyncInvocation ins1 = vm1.invokeAsync(new SerializableRunnable("change the entry") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", "vm1");
      }
    });
    ins0.join(MAX_WAIT);
    ins1.join(MAX_WAIT);

    RegionVersionVector rvv0 = getRVV(vm0);
    RegionVersionVector rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);

    
    Object value0 = getEntry(vm0, "A");
    Object value1 = getEntry(vm1, "A");
    assertEquals(value0, value1);

    closeRegion(vm0);
    closeRegion(vm1);
    
    // recover
    future1 = createPersistentRegionAsync(vm1, diskSync);
    future0 = createPersistentRegionAsync(vm0, diskSync);
    future1.join(MAX_WAIT);
    future0.join(MAX_WAIT);
    
    value0 = getEntry(vm0, "A");
    value1 = getEntry(vm1, "A");
    assertEquals(value0, value1);
    
    rvv0 = getRVV(vm0);
    rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);

    // round 2: async disk write
    vm0.invoke(addObserver);
    vm1.invoke(addObserver);

    ins0 = vm0.invokeAsync(new SerializableRunnable("change the entry at vm0") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        for (int i=0; i<1000; i++) {
          region.put("A", "vm0-"+i);
        }
      }
    });
    ins1 = vm1.invokeAsync(new SerializableRunnable("change the entry at vm1") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        for (int i=0; i<1000; i++) {
          region.put("A", "vm1-"+i);
        }
      }
    });
    ins0.join(MAX_WAIT);
    ins1.join(MAX_WAIT);

    rvv0 = getRVV(vm0);
    rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);
    
    value0 = getEntry(vm0, "A");
    value1 = getEntry(vm1, "A");
    assertEquals(value0, value1);
    
    closeCache(vm0);
    closeCache(vm1);
    
    // recover again
    future1 = createPersistentRegionAsync(vm1, diskSync);
    future0 = createPersistentRegionAsync(vm0, diskSync);
    future1.join(MAX_WAIT);
    future0.join(MAX_WAIT);
    
    value0 = getEntry(vm0, "A");
    value1 = getEntry(vm1, "A");
    assertEquals(value0, value1);
    
    rvv0 = getRVV(vm0);
    rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);
  }
  
  private void assertSameRVV(RegionVersionVector rvv1,
      RegionVersionVector rvv2) {
    if(!rvv1.sameAs(rvv2)) {
      fail("Expected " + rvv1 + " but was " + rvv2);
    }
  }
  /**
   * Tests that even non persistent regions can transmit the list
   * of crashed members to other persistent regions, So that the persistent
   * regions can negotiate who has the latest data during recovery.
   */
  public void testTransmitCrashedMembersWithNonPeristentRegion() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createPersistentRegion(vm0);
    createNonPersistentRegion(vm1);
    
    putAnEntry(vm0);
    
    closeRegion(vm0);
   
    //VM 2 should not do a GII from vm1,
    // it should wait for vm0
    AsyncInvocation future = createPersistentRegionWithWait(vm2);
    
    createPersistentRegion(vm0);
    
    future.getResult(MAX_WAIT);
    
    closeRegion(vm0);
    
    updateTheEntry(vm1);
    
    closeRegion(vm1);
    
    closeRegion(vm2);
    
    
    //VM2 has the most recent data, it should start
    createPersistentRegion(vm2);
    
    //VM0 should be informed that it has older data than VM2, so
    //it should initialize from vm2
    createPersistentRegion(vm0);
    
    
    checkForEntry(vm0);
    checkForEntry(vm2);
  }
  
  public void testSplitBrain() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPersistentRegion(vm0);
    
    putAnEntry(vm0);
    
    closeRegion(vm0);
    
    createPersistentRegion(vm1);
    
    updateTheEntry(vm1);
    
    closeRegion(vm1);
    
    
    //VM0 doesn't know that VM1 ever existed
    //so it will start up.
    createPersistentRegion(vm0);

    ExpectedException e = addExpectedException(ConflictingPersistentDataException.class.getSimpleName(), vm1);
    try {
      //VM1 should not start up, because we should detect that vm1
      //was never in the same distributed system as vm0
      createPersistentRegion(vm1);
      fail("Should have thrown an exception, vm1 is from a 'different' distributed system");
    } catch(RuntimeException ok) {
      if(!(ok.getCause() instanceof ConflictingPersistentDataException)) {
        throw ok;
      }
    } finally {
      e.remove();
    }
  }

  private static final AtomicBoolean sawRequestImageMessage = new AtomicBoolean(false);
  
  /**
   * Test to make sure that if if a member crashes
   * while a GII is in progress, we wait
   * for the member to come back for starting.
   */
  public void testCrashDuringGII() throws Throwable { 
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    
    putAnEntry(vm0);
    
    getLogWriter().info("closing region in vm0");
    closeRegion(vm0);
    
    updateTheEntry(vm1);
    
    getLogWriter().info("closing region in vm1");
    closeRegion(vm1);
    
    
    //This ought to wait for VM1 to come back
    getLogWriter().info("Creating region in VM0");
    AsyncInvocation future = createPersistentRegionAsync(vm0);
    
    waitForBlockedInitialization(vm0);
    
    assertTrue(future.isAlive());
    
    //Add a hook which will disconnect from the distributed
    //system when the initial image message shows up.
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        sawRequestImageMessage.set(false);
        
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
           if(message instanceof RequestImageMessage) {
             DistributionMessageObserver.setInstance(null);
             disconnectFromDS();
             synchronized (sawRequestImageMessage) {
               sawRequestImageMessage.set(true);
               sawRequestImageMessage.notifyAll();
             }
           }
          }
          
          @Override
          public void afterProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            
          }
        });
      }
    });
    
    createPersistentRegion(vm1);
    
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        synchronized (sawRequestImageMessage) {
          try {
            while (!sawRequestImageMessage.get()) {
              sawRequestImageMessage.wait();
            }
          } catch (InterruptedException ex) {
          }
        }
      }
    });

    waitForBlockedInitialization(vm0);
    
    assertTrue(future.isAlive());
    
    //Now create the region again. The initialization should
    //work (the observer was cleared when we disconnected from the DS.
    createPersistentRegion(vm1);;
    
    future.join(MAX_WAIT);
    if(future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if(future.exceptionOccurred()) {
      throw new Exception(future.getException());
    }
    
    checkForEntry(vm0);
    checkForEntry(vm1);
    
    checkForRecoveryStat(vm1, true);
    checkForRecoveryStat(vm0, false);
  }

  /**
   * Test to make sure we don't leak any persistent ids if a member does GII
   * while a distributed destroy is in progress
   */
  public void testGIIDuringDestroy() throws Throwable { 
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    
    //Add a hook which will disconnect from the distributed
    //system when the initial image message shows up.
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof DestroyRegionMessage) {
              createPersistentRegionAsync(vm2);
              try {
                Thread.sleep(10000);
              } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              } finally {
                DistributionMessageObserver.setInstance(null);
              }
            }
          }
          
          @Override
          public void afterProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            
          }

          @Override
          public void beforeSendMessage(DistributionManager dm,
              DistributionMessage msg) {
          }
        });
      }
    });
    
    createPersistentRegion(vm1);
    
    vm0.invoke(new SerializableRunnable("Destroy region") {
      
      public void run() {
        Cache cache =getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.destroyRegion();
      }
    });
    
    
    vm1.invoke(new SerializableRunnable("check destroyed") {
      
      public void run() {
        Cache cache =getCache();
        assertNull(cache.getRegion(REGION_NAME));
      }
    });
    
    vm2.invoke(new SerializableRunnable("Wait for region creation") {
      
      public void run() {
       final  Cache cache = getCache();
        waitForCriterion(new WaitCriterion() {

          public String description() {
            return "Waiting for creation of region " + REGION_NAME;
          }

          public boolean done() {
            Region region = cache.getRegion(REGION_NAME);
            return region !=null;
          }
          
        }, MAX_WAIT, 100, true);
      }
    });
    
    vm2.invoke(new SerializableRunnable("Check offline members") {
      
      public void run() {
       final  Cache cache = getCache();
       DistributedRegion region = (DistributedRegion) cache.getRegion(REGION_NAME);
       PersistenceAdvisor persistAdvisor = region.getPersistenceAdvisor();
       assertEquals(Collections.emptySet(), persistAdvisor.getMembershipView().getOfflineMembers());
      }
    });
  }
  
  public void testCrashDuringPreparePersistentId() throws Throwable { 
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    //Add a hook which will disconnect from the distributed
    //system when the initial image message shows up.
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
           if(message instanceof PrepareNewPersistentMemberMessage) {
             DistributionMessageObserver.setInstance(null);
             disconnectFromDS();
           }
          }
          
          @Override
          public void afterProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            
          }
        });
      }
    });
    createPersistentRegion(vm0);
    
    putAnEntry(vm0);
    
    updateTheEntry(vm0);
    
    AsyncInvocation async1 = createPersistentRegionAsync(vm1);
    
    //Wait for vm 1 to get stuck waiting for vm0, because vm0 has crashed
    waitForBlockedInitialization(vm1);
    
//    closeCache(vm0);
    closeCache(vm1);
    
    try {
      async1.getResult();
      fail("Should have seen a CacheClosedException");
    } catch (Exception e) {
      if (! (e.getCause().getCause() instanceof CacheClosedException)) {
        throw e;
      }
    }
    
    createPersistentRegion(vm0);
    
    createPersistentRegion(vm1);;
    
    checkForEntry(vm0);
    checkForEntry(vm1);
  }
  
  public void testSplitBrainWithNonPersistentRegion() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPersistentRegion(vm1);
    
    putAnEntry(vm1);
    updateTheEntry(vm1);
    
    closeRegion(vm1);
    
    createNonPersistentRegion(vm0);
    
    ExpectedException e = addExpectedException(IllegalStateException.class.getSimpleName(), vm1);
    try {
      createPersistentRegion(vm1);
      fail("Should have received an IllegalState exception");
    } catch(Exception expected) {
      if(!(expected.getCause() instanceof IllegalStateException)) {
        throw expected;
      }
    } finally {
      e.remove();
    }
    
    closeRegion(vm0);
    
    createPersistentRegion(vm1);
    
    checkForEntry(vm1);
    
    checkForRecoveryStat(vm1, true);
  }

  public void testMissingEntryOnDisk() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
   
    //Add a hook which will perform some updates while the region is initializing
    vm0.invoke(new SerializableRunnable() {
     
      public void run() {
       
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
         
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
           if(message instanceof RequestImageMessage) {
             Cache cache = getCache();
             Region region = cache.getRegion(REGION_NAME);
             if (region == null) {
               getLogWriter().severe("removing listener for PersistentRecoveryOrderDUnitTest because region was not found: " + REGION_NAME);
               Object old = DistributionMessageObserver.setInstance(null);
               if (old != this) {
                 getLogWriter().severe("removed listener was not the invoked listener", new Exception("stack trace"));
               }
               return;
             }
             region.put("A", "B");
             region.destroy("A");
             region.put("A", "C");
           }
          }
         
          @Override
          public void afterProcessMessage(DistributionManager dm,
              DistributionMessage message) {
           
          }
        });
      }
    });
    createPersistentRegion(vm0);
   
    createPersistentRegion(vm1);
   
    checkForEntry(vm1);
   
    closeRegion(vm0);
   
    closeRegion(vm1);
   
    //This should work
    createPersistentRegion(vm1);
   
    checkForEntry(vm1);
  }
  
  /**
   * Tests to make sure that we stop waiting for a member
   * that we revoke.
   * @throws Throwable
   */
  public void testCompactFromAdmin() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createPersistentRegionWithoutCompaction(vm0);
    createPersistentRegionWithoutCompaction(vm1);
    
    vm1.invoke(new SerializableRunnable("Create some data") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        Region region = cache.getRegion(REGION_NAME);
        for(int i =0; i < 1024; i++) {
          region.put(i, new byte[1024]);
        }
        
        for(int i =2; i < 1024; i++) {
          assertTrue(region.destroy(i) != null);
        }
        DiskStore store = cache.findDiskStore(REGION_NAME);
        store.forceRoll();
      }
    });
//    vm1.invoke(new SerializableRunnable("compact") {
//      public void run() {
//        Cache cache = getCache();
//        DiskStore ds = cache.findDiskStore(REGION_NAME);
//        assertTrue(ds.forceCompaction());
//      }
//    });
//
//    vm0.invoke(new SerializableRunnable("compact") {
//      public void run() {
//        Cache cache = getCache();
//        DiskStore ds = cache.findDiskStore(REGION_NAME);
//        assertTrue(ds.forceCompaction());
//      }
//    });
    vm2.invoke(new SerializableRunnable("Compact") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          Map<DistributedMember, Set<PersistentID>> missingIds = adminDS.compactAllDiskStores();
          assertEquals(2, missingIds.size());
          for(Set<PersistentID> value : missingIds.values()) {
            assertEquals(1, value.size());
          }
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
    
    SerializableRunnable compactVM = new SerializableRunnable("compact") {
      public void run() {
        Cache cache = getCache();
        DiskStore ds = cache.findDiskStore(REGION_NAME);
        assertFalse(ds.forceCompaction());
      }
    };
    
    vm0.invoke(compactVM);
    vm1.invoke(compactVM);
  }
  
  public void testCloseDuringRegionOperation() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    
    //Try to make sure there are some operations in flight while closing the cache
    SerializableCallable createData0 = new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        
        int i =0;
        while(true) {
          try {
            region.put(0, i);
            i++;
          } catch(RegionDestroyedException e) {
            break;
          } catch(CacheClosedException e) {
            break;
          }
        }
        return i-1;
      }
    };
    
    SerializableCallable createData1 = new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        
        int i =0;
        while(true) {
          try {
            region.put(1, i);
            i++;
          } catch(RegionDestroyedException e) {
            break;
          } catch(CacheClosedException e) {
            break;
          }
        }
        return i-1;
      }
    };
    
    AsyncInvocation asyncCreate0 = vm0.invokeAsync(createData0);
    AsyncInvocation asyncCreate1 = vm1.invokeAsync(createData1);
    
    Thread.sleep(500);
    
    AsyncInvocation close0 = closeCacheAsync(vm0);
    AsyncInvocation close1 = closeCacheAsync(vm1);
    
    //wait for the close to finish
    close0.getResult();
    close1.getResult();
    
    Integer lastSuccessfulInt0 = (Integer) asyncCreate0.getResult();
    Integer lastSuccessfulInt1 = (Integer) asyncCreate1.getResult();
    System.err.println("Cache was closed on 0->" + lastSuccessfulInt0 + ",1->" + lastSuccessfulInt1);
    
    AsyncInvocation create1 = createPersistentRegionAsync(vm0);
    AsyncInvocation create2 = createPersistentRegionAsync(vm1);
    
    create1.getResult();
    create2.getResult();
    
    checkConcurrentCloseValue(vm0, vm1, 0, lastSuccessfulInt0);
    checkConcurrentCloseValue(vm0, vm1, 1, lastSuccessfulInt1);
  }
  
  @Ignore("Disabled due to bug #52240")
  public void DISABLED_testCloseDuringRegionOperationWithTX() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createInternalPersistentRegionAsync(vm0).getResult();
    createInternalPersistentRegionAsync(vm1).getResult();
    createInternalPersistentRegionAsync(vm2).getResult();
    
    
    AsyncInvocation asyncCreate0 = createDataAsyncTX(vm0, 0);
    AsyncInvocation asyncCreate1 = createDataAsyncTX(vm0, 1);
    AsyncInvocation asyncCreate2 = createDataAsyncTX(vm0, 2);
    
    Thread.sleep(500);
    
    AsyncInvocation close0 = closeCacheAsync(vm0);
    AsyncInvocation close1 = closeCacheAsync(vm1);
    AsyncInvocation close2 = closeCacheAsync(vm2);
    
    //wait for the close to finish
    close0.getResult();
    close1.getResult();
    close2.getResult();
    
    Integer lastSuccessfulInt0 = (Integer) asyncCreate0.getResult();
    Integer lastSuccessfulInt1 = (Integer) asyncCreate1.getResult();
    Integer lastSuccessfulInt2 = (Integer) asyncCreate2.getResult();
    System.err.println("Cache was closed on 0->" + lastSuccessfulInt0 + ",1->" + lastSuccessfulInt1 + ",2->" + lastSuccessfulInt2);
    
    AsyncInvocation create0 = createInternalPersistentRegionAsync(vm0);
    AsyncInvocation create1 = createInternalPersistentRegionAsync(vm1);
    AsyncInvocation create2 = createInternalPersistentRegionAsync(vm2);
    
    create0.getResult();
    create1.getResult();
    create2.getResult();
    
    checkConcurrentCloseValue(vm0, vm1, 0, lastSuccessfulInt0);
    checkConcurrentCloseValue(vm0, vm1, 1, lastSuccessfulInt1);
    checkConcurrentCloseValue(vm0, vm1, 2, lastSuccessfulInt2);
  }

  public AsyncInvocation createDataAsyncTX(VM vm1, final int member) {
    SerializableCallable createData1 = new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        
        int i =0;
        TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
        while(true) {
          try {
            txManager.begin();
            region.put(member, i);
            txManager.commit();
            i++;
          } catch(RegionDestroyedException e) {
            break;
          } catch(CacheClosedException e) {
            break;
          } catch(IllegalArgumentException e) {
            if(!e.getMessage().contains("Invalid txLockId")) {
              throw e;
            }
            break;
          } catch(LockServiceDestroyedException e) {
            break;
          }
        }
        return i-1;
      }
    };
    AsyncInvocation asyncCreate1 = vm1.invokeAsync(createData1);
    return asyncCreate1;
  }
  
  /**
   * Tests to make sure that after we get a conflicting
   * persistent data exception, we can still recover.
   * 
   * This is bug XX.
   */
  public void testRecoverAfterConflict() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    putAnEntry(vm0);
    getLogWriter().info("closing region in vm0");
    closeCache(vm0);
    
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    putAnEntry(vm1);
    
    getLogWriter().info("Creating region in VM0");
    ExpectedException ex = addExpectedException("ConflictingPersistentDataException", vm0);
    try {
      //this should cause a conflict
      createPersistentRegion(vm0);
      fail("Should have received a ConflictingPersistentDataException");
    } catch(RuntimeException e) {
      if(!(e.getCause() instanceof ConflictingPersistentDataException)) {
        throw e;
      }
    } finally {
      ex.remove();
    }
    
    getLogWriter().info("closing region in vm1");
    closeCache(vm1);
    
    //This should work now
    createPersistentRegion(vm0);
    
    updateTheEntry(vm0);
    
    ex = addExpectedException("ConflictingPersistentDataException", vm1);
    //Now make sure vm1 gets a conflict
    getLogWriter().info("Creating region in VM1");
    try {
      //this should cause a conflict
      createPersistentRegion(vm1);
      fail("Should have received a ConflictingPersistentDataException");
    } catch(RuntimeException e) {
      if(!(e.getCause() instanceof ConflictingPersistentDataException)) {
        throw e;
      }
    } finally {
      ex.remove();
    }
  }

  private void checkConcurrentCloseValue(VM vm0, VM vm1,
      final int key, int lastSuccessfulInt) {
    SerializableCallable getValue = new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        int value = (Integer) region.get(key);
        return value;
      }
    };
    
    int vm1Value  = (Integer) vm0.invoke(getValue);
    int vm2Value = (Integer) vm1.invoke(getValue);
    assertEquals(vm1Value, vm2Value);
    assertTrue("value = " + vm1Value + ", lastSuccessfulInt=" + lastSuccessfulInt, 
        vm1Value == lastSuccessfulInt || vm1Value == lastSuccessfulInt+1);
  }

  private void checkForEntry(VM vm) {
    SerializableRunnable checkForEntry = new SerializableRunnable("check for the entry") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        assertEquals("C", region.get("A"));
      }
    };
    vm.invoke(checkForEntry);
  }
  
  protected void updateTheEntry(VM vm1) {
    updateTheEntry(vm1, "C");
  }

  protected void updateTheEntry(VM vm1, final String value) {
    vm1.invoke(new SerializableRunnable("change the entry") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", value);
      }
    });
  }

  protected void putAnEntry(VM vm0) {
    vm0.invoke(new SerializableRunnable("Put an entry") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", "B");
      }
    });
  }

  @Override
  public Properties getDistributedSystemProperties() {
    getLogWriter().info("Looking for ack-wait-threshold");
    String s = System.getProperty("gemfire.ack-wait-threshold");
    if (s != null) {
      SAVED_ACK_WAIT_THRESHOLD = s;
      getLogWriter().info("removing system property gemfire.ack-wait-threshold");
      System.getProperties().remove("gemfire.ack-wait-threshold");
    }
    Properties props = super.getDistributedSystemProperties();
    props.put(DistributionConfig.ACK_WAIT_THRESHOLD_NAME, "5");
    return props;
  }

  private void checkForRecoveryStat(VM vm, final boolean localRecovery) {
    vm.invoke(new SerializableRunnable("check disk region stat") {
      
      public void run() {
        Cache cache = getCache();
        DistributedRegion region = (DistributedRegion) cache.getRegion(REGION_NAME);
        DiskRegionStats stats = region.getDiskRegion().getStats();
        if(localRecovery) {
          assertEquals(1, stats.getLocalInitializations());
          assertEquals(0, stats.getRemoteInitializations());
        }
        else {
          assertEquals(0, stats.getLocalInitializations());
          assertEquals(1, stats.getRemoteInitializations());
        }
        
      }
    });
  }
  protected AsyncInvocation createInternalPersistentRegionAsync(final VM vm) {
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File dir = getDiskDirForVM(vm);
        dir.mkdirs();
        dsf.setDiskDirs(new File[] {dir});
        dsf.setMaxOplogSize(1);
        DiskStore ds = dsf.create(REGION_NAME);
        InternalRegionArguments internalArgs = new InternalRegionArguments();
        internalArgs.setIsUsedForMetaRegion(true);
        internalArgs.setMetaRegionWithTransactions(true);
        AttributesFactory rf = new AttributesFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDiskSynchronous(true);
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        try {
          ((GemFireCacheImpl)cache).createVMRegion(REGION_NAME, rf.create(), internalArgs);
        } catch (ClassNotFoundException e) {
          fail("error", e);
        } catch (IOException e) {
          fail("error", e);
        }
      }
    };
    return vm.invokeAsync(createRegion);
  }
}
