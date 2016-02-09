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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CertifiableTestCacheListener;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This is a Dunit test for PartitionedRegion cleanup on Node Failure through
 * Membership listener. This class contains following 2 tests: <br>
 * (1) testMetaDataCleanupOnSinglePRNodeFail - Test for PartitionedRegion
 * metadata cleanup for single failed node.</br> (2)
 * testMetaDataCleanupOnMultiplePRNodeFail - Test for PartitionedRegion metadata
 * cleanup for multiple failed nodes.</br>
 *
 * @author tnegi, modified by Tushar (for bug#35713)
 */
public class PartitionedRegionHAFailureAndRecoveryDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  /** to store references of 4 vms */
  VM vmArr[] = new VM[4];

  /**
   * Constructor for PartitionedRegionHAFailureAndRecoveryDUnitTest.
   *
   * @param name
   */
  public PartitionedRegionHAFailureAndRecoveryDUnitTest(String name) {

    super(name);
  }

  /**
   * Test for PartitionedRegion metadata cleanup for single node failure. <br>
   * <u>This test does the following:<u></br> <br>
   * (1)Creates 4 Vms </br> <br>
   * (2)Randomly create different number of PartitionedRegion on all 4 VMs</br><br>
   * (3)Disconenct vm0 from the distributed system</br> <br>
   * (4) Validate Failed node's config metadata </br> <br>
   * (5) Validate Failed node's bucket2Node Region metadata. </br>
   */

  public void testMetaDataCleanupOnSinglePRNodeFail() throws Throwable
  {
    // create the VM's
    createVMs();
    // create the partitionedRegion on diffrent nodes.
    final int startIndexForRegion = 0;
    final int endIndexForRegion = 4;
    final int localMaxMemory = 200;
    final int redundancy = 1;
    createPartitionRegionAsynch("testMetaDataCleanupOnSinglePRNodeFail_",
        startIndexForRegion, endIndexForRegion, localMaxMemory, redundancy, -1);
    LogWriterUtils.getLogWriter()
        .info(
            "testMetaDataCleanupOnSinglePRNodeFail() - PartitionedRegion's created at all VM nodes");
    
    // Add a listener to the config meta data
    addConfigListeners();

    // disconnect vm0.
    DistributedMember dsMember = (DistributedMember)vmArr[0].invoke(this, "disconnectMethod");

    LogWriterUtils.getLogWriter().info(
        "testMetaDataCleanupOnSinglePRNodeFail() - VM = " + dsMember
            + " disconnected from the distributed system ");
    
    // validate that the metadata clean up is done at all the VM's.
    vmArr[1].invoke(validateNodeFailMetaDataCleanUp(dsMember));
    vmArr[2].invoke(validateNodeFailMetaDataCleanUp(dsMember));
    vmArr[3].invoke(validateNodeFailMetaDataCleanUp(dsMember));
    LogWriterUtils.getLogWriter()
        .info(
            "testMetaDataCleanupOnSinglePRNodeFail() - Validation of Failed node config metadata complete");

    // validate that bucket2Node clean up is done at all the VM's.
    vmArr[1].invoke(validateNodeFailbucket2NodeCleanUp(dsMember));
    vmArr[2].invoke(validateNodeFailbucket2NodeCleanUp(dsMember));
    vmArr[3].invoke(validateNodeFailbucket2NodeCleanUp(dsMember));

    LogWriterUtils.getLogWriter()
        .info(
            "testMetaDataCleanupOnSinglePRNodeFail() - Validation of Failed node bucket2Node Region metadata complete");

    LogWriterUtils.getLogWriter()
        .info(
            "testMetaDataCleanupOnSinglePRNodeFail() Completed Successfuly ..........");
  }

  private void addConfigListeners()
  {
    
    final SerializableRunnable addListener = new SerializableRunnable("add PRConfig listener") {
      private static final long serialVersionUID = 1L;
      public void run()
      {
        Cache c = getCache();
        Region rootReg = PartitionedRegionHelper.getPRRoot(c);
//        Region allPRs = PartitionedRegionHelper.getPRConfigRegion(rootReg, c);
        rootReg.getAttributesMutator().addCacheListener(new CertifiableTestCacheListener(LogWriterUtils.getLogWriter()));
      }
    };
  
    for (int count = 0; count < this.vmArr.length; count++) {
      VM vm = this.vmArr[count];
      vm.invoke(addListener);
    }
  }
  
  private void clearConfigListenerState(VM[] vmsToClear) 
  {
    final SerializableRunnable clearListener = new SerializableRunnable("clear the listener state") {
      private static final long serialVersionUID = 1L;
      public void run()
      {
        try {
          Cache c = getCache();
          Region rootReg = PartitionedRegionHelper.getPRRoot(c);
//          Region allPRs = PartitionedRegionHelper.getPRConfigRegion(rootReg, c);
          CacheListener[] cls = rootReg.getAttributes().getCacheListeners();
          assertEquals(2, cls.length);
          CertifiableTestCacheListener ctcl = (CertifiableTestCacheListener) cls[1];
          ctcl.clearState();
        } 
        catch (CancelException possible) {
          // If a member has been disconnected, we may get a CancelException
          // in which case the config listener state has been cleared (in a big way)
        }
      }
    };
  
    for (int count = 0; count < vmsToClear.length; count++) {
      VM vm = vmsToClear[count];
      vm.invoke(clearListener);
    }
  }

  static private final String WAIT_PROPERTY = 
    "PartitionedRegionHAFailureAndRecoveryDUnitTest.maxWaitTime";
  static private final int WAIT_DEFAULT = 10000;
  

  /**
   * Test for PartitionedRegion metadata cleanup for multiple node failure. <br>
   * <u>This test does the following:<u></br> <br>
   * (1)Creates 4 Vms </br> <br>
   * (2)Randomly create different number of PartitionedRegion on all 4 VMs</br><br>
   * (3) Disconenct vm0 and vm1 from the distributed system</br> <br>
   * (4) Validate all Failed node's config metadata </br> <br>
   * (5) Validate all Failed node's bucket2Node Region metadata. </br>
   */
  public void testMetaDataCleanupOnMultiplePRNodeFail() throws Throwable
  {
    // create the VM's
    createVMs();
    // create the partitionedRegion on diffrent nodes.
    final int startIndexForRegion = 0;
    final int endIndexForRegion = 4;
    final int localMaxMemory = 200;
    final int redundancy = 1;
    createPartitionRegionAsynch("testMetaDataCleanupOnMultiplePRNodeFail_",
        startIndexForRegion, endIndexForRegion, localMaxMemory, redundancy, -1);
    LogWriterUtils.getLogWriter()
        .info(
            "testMetaDataCleanupOnMultiplePRNodeFail() - PartitionedRegion's created at all VM nodes");
    
    addConfigListeners();

    // disconnect vm0
    DistributedMember dsMember = (DistributedMember)vmArr[0].invoke(this, "disconnectMethod");

    LogWriterUtils.getLogWriter().info(
        "testMetaDataCleanupOnMultiplePRNodeFail() - VM = " + dsMember
            + " disconnected from the distributed system ");

    // validate that the metadata clean up is done at all the VM's for first
    // failed node.
    vmArr[1].invoke(validateNodeFailMetaDataCleanUp(dsMember));
    vmArr[2].invoke(validateNodeFailMetaDataCleanUp(dsMember));
    vmArr[3].invoke(validateNodeFailMetaDataCleanUp(dsMember));
    
    // validate that bucket2Node clean up is done at all the VM's for all failed
    // nodes.
    vmArr[1].invoke(validateNodeFailbucket2NodeCleanUp(dsMember));
    vmArr[2].invoke(validateNodeFailbucket2NodeCleanUp(dsMember));
    vmArr[3].invoke(validateNodeFailbucket2NodeCleanUp(dsMember));
    
    // Clear state of listener, skipping the vmArr[0] which was disconnected 
    VM[] vmsToClear = new VM[] {vmArr[1], vmArr[2], vmArr[3]};
    clearConfigListenerState(vmsToClear);

    //  disconnect vm1
    DistributedMember dsMember2 = (DistributedMember)vmArr[1].invoke(this, "disconnectMethod");

    LogWriterUtils.getLogWriter().info(
        "testMetaDataCleanupOnMultiplePRNodeFail() - VM = " + dsMember2
            + " disconnected from the distributed system ");

//  Thread.sleep(5000);
//    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();
//    try {
//      Thread.sleep(maxWaitTime);
//    }
//    catch (InterruptedException e) {
//      fail("interrupted");
//    }
  
    // validate that the metadata clean up is done at all the VM's for first
    // failed node.
    vmArr[2].invoke(validateNodeFailMetaDataCleanUp(dsMember));
    vmArr[3].invoke(validateNodeFailMetaDataCleanUp(dsMember));

    // validate that the metadata clean up is done at all the VM's for second
    // failed node.
    vmArr[2].invoke(validateNodeFailMetaDataCleanUp(dsMember2));
    vmArr[3].invoke(validateNodeFailMetaDataCleanUp(dsMember2));

    LogWriterUtils.getLogWriter()
        .info(
            "testMetaDataCleanupOnMultiplePRNodeFail() - Validation of Failed nodes config metadata complete");

    vmArr[2].invoke(validateNodeFailbucket2NodeCleanUp(dsMember2));
    vmArr[3].invoke(validateNodeFailbucket2NodeCleanUp(dsMember2));

    LogWriterUtils.getLogWriter()
        .info(
            "testMetaDataCleanupOnMultiplePRNodeFail() - Validation of Failed nodes bucket2Node Region metadata complete");

    LogWriterUtils.getLogWriter()
        .info(
            "testMetaDataCleanupOnMultiplePRNodeFail() Completed Successfuly ..........");
  }

  /**
   * Returns CacheSerializableRunnable to validate the Failed node config
   * metadata.
   *
   * @param dsMember
   *          Failed DistributedMember
   * @return CacheSerializableRunnable
   */

  public CacheSerializableRunnable validateNodeFailMetaDataCleanUp(
      final DistributedMember dsMember)
  {
    SerializableRunnable validator = new CacheSerializableRunnable(
        "validateNodeFailMetaDataCleanUp") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region rootReg = PartitionedRegionHelper.getPRRoot(cache);
//        Region allPRs = PartitionedRegionHelper.getPRConfigRegion(rootReg, cache);
        CacheListener[] cls = rootReg.getAttributes().getCacheListeners();
        assertEquals(2, cls.length);
        CertifiableTestCacheListener ctcl = (CertifiableTestCacheListener) cls[1];
        
        LogWriterUtils.getLogWriter().info("Listener update (" + ctcl.updates.size() + "): " + ctcl.updates) ;
        LogWriterUtils.getLogWriter().info("Listener destroy: (" + ctcl.destroys.size() + "): " + ctcl.destroys) ;

        Iterator itrator = rootReg.keySet().iterator();
        for (Iterator itr = itrator; itr.hasNext();) {
          String prName = (String)itr.next();
          ctcl.waitForUpdated(prName);

          Object obj = rootReg.get(prName);
          if (obj != null) {
            PartitionRegionConfig prConf = (PartitionRegionConfig)obj;
            Set<Node> nodeList = prConf.getNodes();
            Iterator itr2 = nodeList.iterator();
            while (itr2.hasNext()) {
              DistributedMember member = ((Node)itr2.next()).getMemberId();
              if (member.equals(dsMember)) {
                fail("Failed DistributedMember's = " + member
                    + " global meta data not cleared. For PR Region = "
                    + prName);
              }
            }
          }
        }
      }
    };
    return (CacheSerializableRunnable)validator;
  }

  /**
   *  Returns CacheSerializableRunnable to validate the Failed node bucket2Node metadata.
   * @param dsMember Failed DistributedMember
   * @return CacheSerializableRunnable
   */

  public CacheSerializableRunnable validateNodeFailbucket2NodeCleanUp(
      final DistributedMember dsMember)
  {
    SerializableRunnable createPRs = new CacheSerializableRunnable(
        "validateNodeFailbucket2NodeCleanUp") {

      public void run2() throws CacheException
      {
        getCache();
        Map prIDmap = PartitionedRegion.prIdToPR;
        Iterator itr = prIDmap.values().iterator();
        while (itr.hasNext()) {
          Object o = itr.next();
          if (o == PartitionedRegion.PRIdMap.DESTROYED) {
            continue;
          }
          PartitionedRegion prRegion = (PartitionedRegion) o;

          Iterator bukI = prRegion.getRegionAdvisor().getBucketSet().iterator();
          while(bukI.hasNext()) {
            Integer bucketId = (Integer) bukI.next();
            Set bucketOwners = prRegion.getRegionAdvisor().getBucketOwners(bucketId.intValue());
            if (bucketOwners.contains(dsMember)) {
              fail("Failed DistributedMember's = " + dsMember 
                  + " bucket [" + prRegion.bucketStringForLogs(bucketId.intValue()) 
                  + "] meta-data not cleared for partitioned region " + prRegion); 
            }
          }
        }
      }
    };
    return (CacheSerializableRunnable)createPRs;
  }


  /**
   *  Function to create 4 Vms on a given host.
   */
  private void createVMs()
  {
    Host host = Host.getHost(0);
    for (int i = 0; i < 4; i++) {
      vmArr[i] = host.getVM(i);
    }
  }
  /**
   * Function for disconnecting a member from distributed system.
   * @return Disconnected DistributedMember
   */
  public DistributedMember disconnectMethod()
  {
    DistributedMember dsMember = ((InternalDistributedSystem)getCache()
        .getDistributedSystem()).getDistributionManager().getId();
    getCache().getDistributedSystem().disconnect();
    LogWriterUtils.getLogWriter().info("disconnectMethod() completed ..");
    return dsMember;
  }
  
  /**
   * This function creates multiple partition regions on specified nodes.
   */
  private void createPartitionRegionAsynch(final String regionPrefix,
      final int startIndexForRegion, final int endIndexForRegion,
      final int localMaxMemory, final int redundancy, final int recoveryDelay) throws Throwable
  {
    final AsyncInvocation[] async = new AsyncInvocation[vmArr.length];
    for (int count = 0; count < vmArr.length; count++) {
      VM vm = vmArr[count];
      async[count] = vm.invokeAsync(getCreateMultiplePRregion(regionPrefix, endIndexForRegion,
          redundancy, localMaxMemory, recoveryDelay));
    }
    for (int count2 = 0; count2 < async.length; count2++) {
        ThreadUtils.join(async[count2], 30 * 1000);
     }
    
    for (int count2 = 0; count2 < async.length; count2++) {
      if (async[count2].exceptionOccurred()) {
        Assert.fail("exception during " + count2, async[count2].getException());
      }
    }  
  }
  
  /**
   * Test for peer recovery of buckets when a member is removed from the distributed system 
   * @throws Throwable
   */
  public void testRecoveryOfSingleMemberFailure() throws Throwable
  {
    final String uniqName = getUniqueName();
    // create the VM's
    createVMs();
    final int redundantCopies = 2;
    final int numRegions = 1;

    // Create PR on man VMs
    createPartitionRegionAsynch(uniqName,  0, numRegions, 20, redundantCopies, 0);

    // Create some buckets, pick one and get one of the members hosting it
    final DistributedMember bucketHost = 
      (DistributedMember) this.vmArr[0].invoke(new SerializableCallable("Populate PR-" + getUniqueName()) {
      public Object call() throws Exception {
        PartitionedRegion r = (PartitionedRegion) getCache().getRegion(uniqName + "0");
        // Create some buckets
        int i=0;
        final int bucketTarget = 2;
        while (r.getRegionAdvisor().getBucketSet().size() < bucketTarget) {
          if (i > r.getTotalNumberOfBuckets()) {
            fail("Expected there to be " + bucketTarget + " buckets after " + i + " iterations");
          }
          Object k = new Integer(i++);
          r.put(k, k.toString());
        }
        
        // Grab a bucket id
        Integer bucketId = r.getRegionAdvisor().getBucketSet().iterator().next();
        assertNotNull(bucketId);

        // Find a host for the bucket 
        Set bucketOwners = r.getRegionAdvisor().getBucketOwners(bucketId.intValue());
        assertEquals(bucketOwners.size(), redundantCopies + 1);
        DistributedMember bucketOwner = (DistributedMember) bucketOwners.iterator().next();
        assertNotNull(bucketOwner);
        LogWriterUtils.getLogWriter().info("Selected distributed member " + bucketOwner + " to disconnect because it hosts bucketId " + bucketId);
        return bucketOwner;
      }
    });
    assertNotNull(bucketHost);
    
    // Disconnect the selected host 
    Map stillHasDS = Invoke.invokeInEveryVM(new SerializableCallable("Disconnect provided bucketHost") {
      public Object call() throws Exception {
        if (getSystem().getDistributedMember().equals(bucketHost)) {
          LogWriterUtils.getLogWriter().info("Disconnecting distributed member " + getSystem().getDistributedMember());
          disconnectFromDS();
          return Boolean.FALSE;
        }
        return Boolean.TRUE;
      }
    });
    
    // Wait for each PR instance on each VM to finish recovery of redundancy
    // for the selected bucket
    final int MAX_SECONDS_TO_WAIT = 120;
    for (int count = 0; count < vmArr.length; count++) {
      VM vm = vmArr[count];
      // only wait on the remaining VMs (prevent creating a new distributed system on the disconnected VM)
      if (((Boolean)stillHasDS.get(vm)).booleanValue()) { 
        vm.invoke(new SerializableRunnable("Wait for PR region recovery") {
          public void run() {
            for (int i=0; i<numRegions; i++) { 
              Region r = getCache().getRegion(uniqName + i);
              assertTrue(r instanceof PartitionedRegion);
              PartitionedRegion pr = (PartitionedRegion) r;
              PartitionedRegionStats prs = pr.getPrStats();
              // Wait for recovery
              final long start = NanoTimer.getTime();
              for(;;) {
                if (prs.getLowRedundancyBucketCount() == 0) {
                  break;  // buckets have been recovered from this VM's point of view
                }
                if (TimeUnit.NANOSECONDS.toSeconds(NanoTimer.getTime() - start) > MAX_SECONDS_TO_WAIT) {
                  fail("Test waited more than " + MAX_SECONDS_TO_WAIT + " seconds for redundancy recover");
                }
                try {
                  TimeUnit.MILLISECONDS.sleep(250);
                }
                catch (InterruptedException e) {
                  Assert.fail("Interrupted, ah!", e);
                }
              }
            }
          }
        });
      }
    } // VM loop
    
    // Validate all buckets have proper redundancy
    for (int count = 0; count < vmArr.length; count++) {
      VM vm = vmArr[count];
      // only validate buckets on remaining VMs 
      // (prevent creating a new distributed system on the disconnected VM)
      if (((Boolean)stillHasDS.get(vm)).booleanValue()) { 
        vm.invoke(new SerializableRunnable("Validate all bucket redundancy") {
          public void run() {
            for (int i=0; i<numRegions; i++) { // region loop
              PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(uniqName + i);
              Iterator bucketIdsWithStorage = pr.getRegionAdvisor().getBucketSet().iterator();
              while (bucketIdsWithStorage.hasNext()) { // bucketId loop
                final int bucketId = ((Integer) bucketIdsWithStorage.next()).intValue();
                do { // retry loop
                  try {
                    List owners = pr.getBucketOwnersForValidation(bucketId);
                    assertEquals(pr.getRedundantCopies() + 1, owners.size());
                    break; // retry loop
                  } catch (ForceReattemptException retryIt) {
                    LogWriterUtils.getLogWriter().info("Need to retry validation for bucket in PR " + pr, retryIt);
                  }
                } while (true); // retry loop
              } // bucketId loop
            } // region loop
          }
        });
      }
    } // VM loop    
  } // end redundancy recovery test
}
