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
package org.apache.geode.internal.cache.partitioned;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_JMX;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.addVMEventListener;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.removeVMEventListener;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.CancelException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.InitialImageOperation.RequestImageMessage;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.VMEventListener;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Extracted from {@link PersistentPartitionedRegionRegressionTest}.
 */
@SuppressWarnings("serial,unused")
public class PersistentPartitionHangsDuringRestartRegressionTest implements Serializable {

  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();

  /** WAIT_TO_BOUNCE latch is never counted down -- it prevents responding to RequestImageMessage */
  private static final CountDownLatch WAIT_TO_BOUNCE = new CountDownLatch(99);

  private static volatile CountDownLatch beforeBounceLatch;
  private static volatile CountDownLatch afterBounceLatch;

  private final transient VMEventListener vmEventListener = new VMEventListener() {
    @Override
    public void afterBounceVM(VM vm) {
      afterBounceLatch.countDown();
    }
  };

  private String partitionedRegionName;

  private VM vmController;
  private VM vm0;
  private VM vm1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();

  @Before
  public void setUp() {
    beforeBounceLatch = new CountDownLatch(1);
    afterBounceLatch = new CountDownLatch(1);

    vmController = getController();
    vm0 = getVM(0);
    vm1 = getVM(1);

    String uniqueName = getClass().getSimpleName() + "-" + testName.getMethodName();
    partitionedRegionName = uniqueName + "-partitionedRegion";
  }

  @After
  public void tearDown() {
    removeVMEventListener(vmEventListener);
    for (VM vm : toArray(getAllVMs(), getController())) {
      vm.invoke(() -> {
        DistributionMessageObserver.setInstance(null);
        InternalResourceManager.setResourceObserver(null);
      });
    }
  }

  /**
   * RegressionTest for bug 42226. <br>
   * 1. Member A has the bucket <br>
   * 2. Member B starts creating the bucket. It tells member A that it hosts the bucket <br>
   * 3. Member A crashes <br>
   * 4. Member B destroys the bucket and throws a partition offline exception, because it wasn't
   * able to complete initialization. <br>
   * 5. Member A recovers, and gets stuck waiting for member B.
   *
   * <p>
   * TRAC 42226: recycled VM hangs during re-start while waiting for Partition to come online (after
   * Controller VM sees unexpected PartitionOffLineException while doing ops)
   */
  @Test
  public void doesNotWaitForPreviousInstanceOfOnlineServer() throws Exception {
    vm0.invoke(() -> {
      createPartitionedRegion(1, 0, 1, true);
      // Make sure we create a bucket
      createData(0, 1, "a", partitionedRegionName);
    });

    vm0.invoke(() -> {
      // notify controller and then wait to bounce
      DistributionMessageObserver.setInstance(new WaitToBounceWhenImageRequested());
    });

    addVMEventListener(vmEventListener);

    try (IgnoredException ie = addIgnoredException(PartitionOfflineException.class)) {
      // This should recover redundancy, which should cause vm0 to bounce/disconnect
      AsyncInvocation createPRAsync = vm1.invokeAsync(() -> createPartitionedRegion(1, 0, 1, true));

      beforeBounceLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
      vm0.bounceForcibly();
      createPRAsync.await();

      // Make sure get a partition offline exception
      vm1.invoke(() -> {
        assertThatThrownBy(() -> createData(0, 1, "a", partitionedRegionName))
            .isInstanceOf(PartitionOfflineException.class);
      });
    }

    afterBounceLatch.await(TIMEOUT_MILLIS, MILLISECONDS);

    // This should recreate the bucket
    vm0.invoke(() -> createPartitionedRegion(1, 0, 1, true));

    vm1.invoke(() -> checkData(0, 1, "a", partitionedRegionName));
  }

  private void createPartitionedRegion(final int redundancy, final int recoveryDelay,
      final int numBuckets, final boolean synchronous) throws InterruptedException {
    CountDownLatch recoveryDone = new CountDownLatch(1);

    if (redundancy > 0) {
      ResourceObserver observer = new ResourceObserverAdapter() {
        @Override
        public void recoveryFinished(Region region) {
          recoveryDone.countDown();
        }
      };

      InternalResourceManager.setResourceObserver(observer);
    } else {
      recoveryDone.countDown();
    }

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundancy);
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setTotalNumBuckets(numBuckets);
    partitionAttributesFactory.setLocalMaxMemory(500);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskSynchronous(synchronous);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(partitionedRegionName);

    recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS);
  }

  private void createData(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region<Integer, String> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      region.put(i, value);
    }
  }

  private void checkData(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region<Integer, String> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      assertThat(region.get(i)).isEqualTo(value);
    }
  }

  /**
   * Prevent GEODE-6232 by disabling JMX which is not needed in this test.
   */
  private InternalCache getCache() {
    Properties config = new Properties();
    config.setProperty(DISABLE_JMX, "true");
    config.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    InternalCache cache = cacheRule.getOrCreateCache(config);
    assertThat(cache.getInternalDistributedSystem().getResourceListeners()).isEmpty();
    return cache;
  }

  private class WaitToBounceWhenImageRequested extends DistributionMessageObserver
      implements Serializable {
    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof RequestImageMessage) {
        RequestImageMessage requestImageMessage = (RequestImageMessage) message;
        // Don't bounce until we see a bucket
        if (requestImageMessage.regionPath.contains("_B_")) {
          DistributionMessageObserver.setInstance(null);
          addIgnoredException(CancelException.class);
          vmController.invoke(() -> beforeBounceLatch.countDown());
          try {
            WAIT_TO_BOUNCE.await(TIMEOUT_MILLIS, MILLISECONDS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }
}
