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
package org.apache.geode.internal.cache;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache30.CertifiableTestCacheListener;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * Tests for PartitionedRegion cleanup on Node Failure through Membership listener.<br>
 *
 * (1) testMetaDataCleanupOnSinglePRNodeFail - Test for PartitionedRegion metadata cleanup for
 * single failed node.<br>
 *
 * (2) testMetaDataCleanupOnMultiplePRNodeFail - Test for PartitionedRegion metadata cleanup for
 * multiple failed nodes.
 */

public class PartitionedRegionHAFailureAndRecoveryDUnitTest extends CacheTestCase {

  private static final int VM_COUNT = 4;

  private VM vms[];
  private String uniqueName;
  private int localMaxMemory;
  private int redundancy;
  private int recoveryDelay;

  @Before
  public void setUp() {
    vms = new VM[4];
    for (int i = 0; i < 4; i++) {
      vms[i] = getHost(0).getVM(i);
    }

    uniqueName = getUniqueName();

    localMaxMemory = 200;
    redundancy = 1;
    recoveryDelay = -1;
  }

  /**
   * Test for PartitionedRegion metadata cleanup for single node failure.<br>
   *
   * (1)Creates 4 Vms<br>
   *
   * (2)Randomly create different number of PartitionedRegion on all 4 VMs<br>
   *
   * (3)Disconnect vm0 from the distributed system<br>
   *
   * (4) Validate Failed node's config metadata<br>
   *
   * (5) Validate Failed node's bucket2Node Region metadata.
   */
  @Test
  public void testMetaDataCleanupOnSinglePRNodeFail() throws Exception {
    createPartitionRegionsInAllVMs(uniqueName, 4, localMaxMemory, recoveryDelay, redundancy);

    // Add a listener to the config meta data
    addConfigListenerInAllVMs();

    // disconnect vm0.
    InternalDistributedMember member = vms[0].invoke(() -> disconnect());

    // validate that the metadata clean up is done at all the VM's.
    vms[1].invoke(() -> validateNodeFailMetaDataCleanUp(member));
    vms[2].invoke(() -> validateNodeFailMetaDataCleanUp(member));
    vms[3].invoke(() -> validateNodeFailMetaDataCleanUp(member));

    // validate that bucket2Node clean up is done at all the VM's.
    vms[1].invoke(() -> validateNodeFailBucket2NodeCleanUp(member));
    vms[2].invoke(() -> validateNodeFailBucket2NodeCleanUp(member));
    vms[3].invoke(() -> validateNodeFailBucket2NodeCleanUp(member));
  }

  /**
   * Test for PartitionedRegion metadata cleanup for multiple node failure.<br>
   *
   * (1)Creates 4 Vms<br>
   *
   * (2)Randomly create different number of PartitionedRegion on all 4 VMs<br>
   *
   * (3) Disconnect vm0 and vm1 from the distributed system<br>
   *
   * (4) Validate all Failed node's config metadata<br>
   *
   * (5) Validate all Failed node's bucket2Node Region metadata.
   */
  @Test
  public void testMetaDataCleanupOnMultiplePRNodeFail() throws Exception {
    createPartitionRegionsInAllVMs(uniqueName, 4, localMaxMemory, recoveryDelay, redundancy);

    addConfigListenerInAllVMs();

    // disconnect vm0
    InternalDistributedMember member0 = vms[0].invoke(() -> disconnect());

    // validate that the metadata clean up is done at all the VM's for first failed node.
    vms[1].invoke(() -> validateNodeFailMetaDataCleanUp(member0));
    vms[2].invoke(() -> validateNodeFailMetaDataCleanUp(member0));
    vms[3].invoke(() -> validateNodeFailMetaDataCleanUp(member0));

    // validate that bucket2Node clean up is done at all the VM's for all failed nodes.
    vms[1].invoke(() -> validateNodeFailBucket2NodeCleanUp(member0));
    vms[2].invoke(() -> validateNodeFailBucket2NodeCleanUp(member0));
    vms[3].invoke(() -> validateNodeFailBucket2NodeCleanUp(member0));

    // Clear state of listener, skipping the vms[0] which was disconnected
    vms[1].invoke(() -> clearConfigListenerState());
    vms[2].invoke(() -> clearConfigListenerState());
    vms[3].invoke(() -> clearConfigListenerState());

    // disconnect vm1
    InternalDistributedMember dsMember2 = vms[1].invoke(() -> disconnect());

    // validate that the metadata clean up is done at all the VM's for first failed node.
    vms[2].invoke(() -> validateNodeFailMetaDataCleanUp(member0));
    vms[3].invoke(() -> validateNodeFailMetaDataCleanUp(member0));

    // validate that the metadata clean up is done at all the VM's for second failed node.
    vms[2].invoke(() -> validateNodeFailMetaDataCleanUp(dsMember2));
    vms[3].invoke(() -> validateNodeFailMetaDataCleanUp(dsMember2));

    vms[2].invoke(() -> validateNodeFailBucket2NodeCleanUp(dsMember2));
    vms[3].invoke(() -> validateNodeFailBucket2NodeCleanUp(dsMember2));
  }

  /**
   * Test for peer recovery of buckets when a member is removed from the distributed system
   */
  @Test
  public void testRecoveryOfSingleMemberFailure() throws Exception {
    localMaxMemory = 20;
    redundancy = 2;
    recoveryDelay = 0;

    // Create PR in all VMs
    createPartitionRegionsInAllVMs(uniqueName, 1, localMaxMemory, recoveryDelay, redundancy);

    // Create some buckets, pick one and get one of the members hosting it
    DistributedMember bucketHost = vms[0].invoke(() -> {
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion(uniqueName + "0");

      // Create some buckets
      for (int k = 0; region.getRegionAdvisor().getBucketSet().size() < 2; k++) {
        assertThat(k).isLessThanOrEqualTo(region.getTotalNumberOfBuckets());
        region.put(k, k);
      }

      // Grab a bucket id
      int bucketId = region.getRegionAdvisor().getBucketSet().iterator().next();
      assertThat(bucketId).isNotNull();

      // Find a host for the bucket
      Set<InternalDistributedMember> bucketOwners =
          region.getRegionAdvisor().getBucketOwners(bucketId);
      assertThat(bucketOwners).hasSize(redundancy + 1);

      InternalDistributedMember bucketOwner = bucketOwners.iterator().next();
      assertThat(bucketOwner).isNotNull();
      return bucketOwner;
    });

    assertThat(bucketHost).isNotNull();

    // Disconnect the selected host
    Map<VM, Boolean> stillHasDS = invokeInEveryVM(() -> {
      if (getSystem().getDistributedMember().equals(bucketHost)) {
        disconnectFromDS();
        return FALSE;
      }
      return TRUE;
    });

    // Wait for each member to finish recovery of redundancy for the selected bucket
    int count = 0;
    for (int i = 0; i < 4; i++) {
      if (awaitRedundancyRecovery(stillHasDS, vms[i])) {
        count++;
      }
    }
    assertThat(count).isEqualTo(3);

    // Validate all buckets have proper redundancy
    for (int i = 0; i < 4; i++) {
      validateBucketRedundancy(stillHasDS, vms[i]);
    }
  }

  private boolean awaitRedundancyRecovery(Map<VM, Boolean> stillHasDS, VM vm) {
    // only wait on the remaining VMs
    if (stillHasDS.get(vm)) {
      vm.invoke(() -> {
        Region region = getCache().getRegion(uniqueName + "0");
        assertTrue(region instanceof PartitionedRegion);
        PartitionedRegion partitionedRegion = (PartitionedRegion) region;
        PartitionedRegionStats prs = partitionedRegion.getPrStats();

        // Wait for recovery
        await()
            .untilAsserted(() -> assertThat(prs.getLowRedundancyBucketCount()).isEqualTo(0));
      });
      return true;
    } else {
      return false;
    }
  }

  private void validateBucketRedundancy(Map<VM, Boolean> stillHasDS, VM vm) {
    // only validate buckets on remaining VMs
    if (stillHasDS.get(vm)) {
      vm.invoke(() -> {
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(uniqueName + "0");
        for (int bucketId : pr.getRegionAdvisor().getBucketSet()) {
          assertThatBucketHasRedundantCopies(pr, bucketId);
        }
      });
    }

  }

  private void assertThatBucketHasRedundantCopies(PartitionedRegion pr, int bucketId) {
    boolean forceReattempt;
    do {
      forceReattempt = false;
      try {
        List owners = pr.getBucketOwnersForValidation(bucketId);
        assertThat(owners).hasSize(pr.getRedundantCopies() + 1);
      } catch (ForceReattemptException ignored) {
        forceReattempt = true;
      }
    } while (forceReattempt);
  }

  private void clearConfigListenerState() {
    Region rootReg = PartitionedRegionHelper.getPRRoot(getCache());
    CacheListener[] cls = rootReg.getAttributes().getCacheListeners();
    assertThat(cls).hasSize(2);

    CertifiableTestCacheListener ctcl = (CertifiableTestCacheListener) cls[1];
    ctcl.clearState();
  }

  private void validateNodeFailMetaDataCleanUp(final DistributedMember expectedMember) {
    Region rootReg = PartitionedRegionHelper.getPRRoot(getCache());
    CacheListener[] cls = rootReg.getAttributes().getCacheListeners();
    assertThat(cls).hasSize(2);

    CertifiableTestCacheListener ctcl = (CertifiableTestCacheListener) cls[1];

    for (Object regionNameObject : rootReg.keySet()) {
      String regionName = (String) regionNameObject;
      ctcl.waitForUpdated(regionName);

      Object prConfigObject = rootReg.get(regionName);
      if (prConfigObject != null) {
        PartitionRegionConfig prConfig = (PartitionRegionConfig) prConfigObject;
        Set<Node> nodes = prConfig.getNodes();
        for (Node node : nodes) {
          DistributedMember member = node.getMemberId();
          assertThat(member).isNotEqualTo(expectedMember);
        }
      }
    }
  }

  private void validateNodeFailBucket2NodeCleanUp(final InternalDistributedMember member) {
    getCache();
    Map prIdToPR = PartitionedRegion.getPrIdToPR();
    for (Object prRegionObject : prIdToPR.values()) {
      if (prRegionObject == PartitionedRegion.PRIdMap.DESTROYED) {
        continue;
      }
      PartitionedRegion prRegion = (PartitionedRegion) prRegionObject;

      for (int bucketId : prRegion.getRegionAdvisor().getBucketSet()) {
        Set<InternalDistributedMember> bucketOwners =
            prRegion.getRegionAdvisor().getBucketOwners(bucketId);
        assertThat(bucketOwners).doesNotContain(member);
      }
    }
  }

  private InternalDistributedMember disconnect() {
    InternalDistributedMember member = getCache().getMyId();
    getCache().close();
    return member;
  }

  private void createPartitionRegionsInAllVMs(final String regionNamePrefix,
      final int numberOfRegions, final int localMaxMemory, final int recoveryDelay,
      final int redundancy) {
    for (int count = 0; count < VM_COUNT; count++) {
      VM vm = vms[count];
      vm.invoke(() -> {
        createPartitionRegions(regionNamePrefix, numberOfRegions, localMaxMemory, recoveryDelay,
            redundancy);
      });
    }
  }

  private void createPartitionRegions(final String regionNamePrefix, final int numberOfRegions,
      final int localMaxMemory, final int recoveryDelay, final int redundancy) {
    for (int i = 0; i < numberOfRegions; i++) {
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
      partitionAttributesFactory.setRedundantCopies(redundancy);
      partitionAttributesFactory.setRecoveryDelay(recoveryDelay);

      RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

      regionFactory.create(regionNamePrefix + i);
    }
  }

  private void addConfigListenerInAllVMs() {
    for (int count = 0; count < 4; count++) {
      VM vm = vms[count];
      vm.invoke(() -> {
        addConfigListener();
      });
    }
  }

  private void addConfigListener() {
    Region partitionedRootRegion = PartitionedRegionHelper.getPRRoot(getCache());
    partitionedRootRegion.getAttributesMutator()
        .addCacheListener(new CertifiableTestCacheListener(getLogWriter()));
  }

}
