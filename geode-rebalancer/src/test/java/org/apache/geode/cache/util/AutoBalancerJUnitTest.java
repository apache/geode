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
package org.apache.geode.cache.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.util.AutoBalancer.AuditScheduler;
import org.apache.geode.cache.util.AutoBalancer.CacheOperationFacade;
import org.apache.geode.cache.util.AutoBalancer.GeodeCacheFacade;
import org.apache.geode.cache.util.AutoBalancer.OOBAuditor;
import org.apache.geode.cache.util.AutoBalancer.SizeBasedOOBAuditor;
import org.apache.geode.cache.util.AutoBalancer.TimeProvider;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.InternalPRInfo;
import org.apache.geode.internal.cache.partitioned.LoadProbe;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * UnitTests for AutoBalancer. All collaborators should be mocked.
 */
@Category(IntegrationTest.class)
public class AutoBalancerJUnitTest {
  Mockery mockContext;

  CacheOperationFacade mockCacheFacade;
  OOBAuditor mockAuditor;
  AuditScheduler mockScheduler;
  TimeProvider mockClock;

  @Before
  public void setupMock() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };

    mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
    mockAuditor = mockContext.mock(OOBAuditor.class);
    mockScheduler = mockContext.mock(AuditScheduler.class);
    mockClock = mockContext.mock(TimeProvider.class);
  }

  @After
  public void validateMock() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testLockStatExecuteInSequence() throws InterruptedException {
    final Sequence sequence = mockContext.sequence("sequence");
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        inSequence(sequence);
        will(returnValue(true));
        oneOf(mockCacheFacade).incrementAttemptCounter();
        inSequence(sequence);
        oneOf(mockCacheFacade).getTotalTransferSize();
        inSequence(sequence);
        will(returnValue(0L));
      }
    });

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    balancer.getOOBAuditor().execute();
  }

  @Test
  public void testAcquireLockAfterReleasedRemotely() throws InterruptedException {
    final Sequence sequence = mockContext.sequence("sequence");
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        inSequence(sequence);
        will(returnValue(false));
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        inSequence(sequence);
        will(returnValue(true));
        oneOf(mockCacheFacade).incrementAttemptCounter();
        oneOf(mockCacheFacade).getTotalTransferSize();
        will(returnValue(0L));
      }
    });

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    balancer.getOOBAuditor().execute();
    balancer.getOOBAuditor().execute();
  }

  @Test
  public void testFailExecuteIfLockedElsewhere() throws InterruptedException {
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        will(returnValue(false));
        // no other methods, rebalance, will be called
      }
    });

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    balancer.getOOBAuditor().execute();
  }

  @Test(expected = IllegalStateException.class)
  public void testNoCacheError() {
    AutoBalancer balancer = new AutoBalancer();
    OOBAuditor auditor = balancer.getOOBAuditor();
    auditor.execute();
  }

  @Test
  public void testOOBWhenBelowSizeThreshold() {
    final long totalSize = 1000L;

    final Map<PartitionedRegion, InternalPRInfo> details = new HashMap<>();
    mockContext.checking(new Expectations() {
      {
        allowing(mockCacheFacade).getRegionMemberDetails();
        will(returnValue(details));
        // first run
        oneOf(mockCacheFacade).getTotalDataSize(details);
        will(returnValue(totalSize));
        oneOf(mockCacheFacade).getTotalTransferSize();
        // half of threshold limit
        will(returnValue((AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT * totalSize / 100) / 2));

        // second run
        oneOf(mockCacheFacade).getTotalTransferSize();
        // nothing to transfer
        will(returnValue(0L));
      }
    });

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    Properties config = getBasicConfig();
    config.put(AutoBalancer.MINIMUM_SIZE, "10");
    balancer.init(config);
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();

    // first run
    assertFalse(auditor.needsRebalancing());

    // second run
    assertFalse(auditor.needsRebalancing());
  }

  @Test
  public void testOOBWhenAboveThresholdButBelowMin() {
    final long totalSize = 1000L;

    mockContext.checking(new Expectations() {
      {
        // first run
        oneOf(mockCacheFacade).getTotalTransferSize();
        // twice threshold
        will(returnValue((AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT * totalSize / 100) * 2));

        // second run
        oneOf(mockCacheFacade).getTotalTransferSize();
        // more than total size
        will(returnValue(2 * totalSize));
      }
    });

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    Properties config = getBasicConfig();
    config.put(AutoBalancer.MINIMUM_SIZE, "" + (totalSize * 5));
    balancer.init(config);
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();

    // first run
    assertFalse(auditor.needsRebalancing());

    // second run
    assertFalse(auditor.needsRebalancing());
  }

  @Test
  public void testOOBWhenAboveThresholdAndMin() {
    final long totalSize = 1000L;

    final Map<PartitionedRegion, InternalPRInfo> details = new HashMap<>();
    mockContext.checking(new Expectations() {
      {
        allowing(mockCacheFacade).getRegionMemberDetails();
        will(returnValue(details));

        // first run
        oneOf(mockCacheFacade).getTotalDataSize(details);
        will(returnValue(totalSize));
        oneOf(mockCacheFacade).getTotalTransferSize();
        // twice threshold
        will(returnValue((AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT * totalSize / 100) * 2));

        // second run
        oneOf(mockCacheFacade).getTotalDataSize(details);
        will(returnValue(totalSize));
        oneOf(mockCacheFacade).getTotalTransferSize();
        // more than total size
        will(returnValue(2 * totalSize));
      }
    });

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    Properties config = getBasicConfig();
    config.put(AutoBalancer.MINIMUM_SIZE, "10");
    balancer.init(config);
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();

    // first run
    assertTrue(auditor.needsRebalancing());

    // second run
    assertTrue(auditor.needsRebalancing());
  }

  @Test(expected = GemFireConfigException.class)
  public void testInvalidSchedule() {
    String someSchedule = "X Y * * * *";
    Properties props = new Properties();
    props.put(AutoBalancer.SCHEDULE, someSchedule);

    AutoBalancer autoR = new AutoBalancer();
    autoR.init(props);
  }

  @Test
  public void testOOBAuditorInit() {
    AutoBalancer balancer = new AutoBalancer();
    balancer.init(getBasicConfig());
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();
    assertEquals(AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT, auditor.getSizeThreshold());
    assertEquals(AutoBalancer.DEFAULT_MINIMUM_SIZE, auditor.getSizeMinimum());

    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "17");
    props.put(AutoBalancer.MINIMUM_SIZE, "10");
    balancer = new AutoBalancer();
    balancer.init(props);
    auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();
    assertEquals(17, auditor.getSizeThreshold());
    assertEquals(10, auditor.getSizeMinimum());
  }

  @Test(expected = GemFireConfigException.class)
  public void testConfigTransferThresholdNegative() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "-1");
    balancer.init(props);
  }

  @Test(expected = GemFireConfigException.class)
  public void testConfigSizeMinNegative() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.MINIMUM_SIZE, "-1");
    balancer.init(props);
  }

  @Test(expected = GemFireConfigException.class)
  public void testConfigTransferThresholdZero() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "0");
    balancer.init(props);
  }

  @Test(expected = GemFireConfigException.class)
  public void testConfigTransferThresholdTooHigh() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "100");
    balancer.init(props);
  }

  @Test
  public void testAutoBalancerInit() {
    final String someSchedule = "1 * * * 1 *";
    final Properties props = new Properties();
    props.put(AutoBalancer.SCHEDULE, someSchedule);
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, 17);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockScheduler).init(someSchedule);
        oneOf(mockAuditor).init(props);
      }
    });

    AutoBalancer autoR = new AutoBalancer(mockScheduler, mockAuditor, null, null);
    autoR.init(props);
  }

  @Test
  public void testMinimalConfiguration() {
    AutoBalancer autoR = new AutoBalancer();
    try {
      autoR.init(null);
      fail();
    } catch (GemFireConfigException e) {
      // expected
    }

    Properties props = getBasicConfig();
    autoR.init(props);
  }

  @Test
  public void testFacadeTotalTransferSize() throws Exception {
    assertEquals(12345, getFacadeForResourceManagerOps(true).getTotalTransferSize());
  }

  @Test
  public void testFacadeRebalance() throws Exception {
    getFacadeForResourceManagerOps(false).rebalance();
  }

  private GeodeCacheFacade getFacadeForResourceManagerOps(final boolean simulate) throws Exception {
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class);
    final InternalResourceManager mockRM = mockContext.mock(InternalResourceManager.class);
    final RebalanceFactory mockRebalanceFactory = mockContext.mock(RebalanceFactory.class);
    final RebalanceOperation mockRebalanceOperation = mockContext.mock(RebalanceOperation.class);
    final RebalanceResults mockRebalanceResults = mockContext.mock(RebalanceResults.class);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).isClosed();
        will(returnValue(false));
        oneOf(mockCache).getResourceManager();
        will(returnValue(mockRM));
        oneOf(mockRM).createRebalanceFactory();
        will(returnValue(mockRebalanceFactory));
        if (simulate) {
          oneOf(mockRebalanceFactory).simulate();
        } else {
          oneOf(mockRebalanceFactory).start();
        }
        will(returnValue(mockRebalanceOperation));
        oneOf(mockRebalanceOperation).getResults();
        will(returnValue(mockRebalanceResults));
        if (simulate) {
          atLeast(1).of(mockRebalanceResults).getTotalBucketTransferBytes();
          will(returnValue(12345L));
        }
        allowing(mockRebalanceResults);
      }
    });

    GeodeCacheFacade facade = new GeodeCacheFacade(mockCache);

    return facade;
  }

  @Test
  public void testFacadeTotalBytesNoRegion() {
    CacheOperationFacade facade = new AutoBalancer().getCacheOperationFacade();

    assertEquals(0, facade.getTotalDataSize(new HashMap<PartitionedRegion, InternalPRInfo>()));
  }

  @Test
  public void testFacadeCollectMemberDetailsNoRegion() {
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).isClosed();
        will(returnValue(false));
        oneOf(mockCache).getPartitionedRegions();
        will(returnValue(new HashSet<PartitionedRegion>()));
      }
    });

    GeodeCacheFacade facade = new GeodeCacheFacade(mockCache);

    assertEquals(0, facade.getRegionMemberDetails().size());
  }

  @Test
  public void testFacadeCollectMemberDetails2Regions() {
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class);
    final InternalResourceManager mockRM = mockContext.mock(InternalResourceManager.class);
    final LoadProbe mockProbe = mockContext.mock(LoadProbe.class);

    final PartitionedRegion mockR1 = mockContext.mock(PartitionedRegion.class, "r1");
    final PartitionedRegion mockR2 = mockContext.mock(PartitionedRegion.class, "r2");
    final HashSet<PartitionedRegion> regions = new HashSet<>();
    regions.add(mockR1);
    regions.add(mockR2);

    final PRHARedundancyProvider mockRedundancyProviderR1 =
        mockContext.mock(PRHARedundancyProvider.class, "prhaR1");
    final InternalPRInfo mockR1PRInfo = mockContext.mock(InternalPRInfo.class, "prInforR1");

    final PRHARedundancyProvider mockRedundancyProviderR2 =
        mockContext.mock(PRHARedundancyProvider.class, "prhaR2");
    final InternalPRInfo mockR2PRInfo = mockContext.mock(InternalPRInfo.class, "prInforR2");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).isClosed();
        will(returnValue(false));
        oneOf(mockCache).getPartitionedRegions();
        will(returnValue(regions));
        exactly(2).of(mockCache).getResourceManager();
        will(returnValue(mockRM));
        exactly(2).of(mockRM).getLoadProbe();
        will(returnValue(mockProbe));
        allowing(mockR1).getFullPath();
        oneOf(mockR1).getRedundancyProvider();
        will(returnValue(mockRedundancyProviderR1));
        allowing(mockR2).getFullPath();
        oneOf(mockR2).getRedundancyProvider();
        will(returnValue(mockRedundancyProviderR2));

        oneOf(mockRedundancyProviderR1).buildPartitionedRegionInfo(with(true),
            with(any(LoadProbe.class)));
        will(returnValue(mockR1PRInfo));

        oneOf(mockRedundancyProviderR2).buildPartitionedRegionInfo(with(true),
            with(any(LoadProbe.class)));
        will(returnValue(mockR2PRInfo));
      }
    });

    GeodeCacheFacade facade = new GeodeCacheFacade(mockCache);

    Map<PartitionedRegion, InternalPRInfo> map = facade.getRegionMemberDetails();
    assertNotNull(map);
    assertEquals(2, map.size());
    assertEquals(map.get(mockR1), mockR1PRInfo);
    assertEquals(map.get(mockR2), mockR2PRInfo);
  }

  @Test
  public void testFacadeTotalBytes2Regions() {
    final PartitionedRegion mockR1 = mockContext.mock(PartitionedRegion.class, "r1");
    final PartitionedRegion mockR2 = mockContext.mock(PartitionedRegion.class, "r2");
    final HashSet<PartitionedRegion> regions = new HashSet<>();
    regions.add(mockR1);
    regions.add(mockR2);

    final InternalPRInfo mockR1PRInfo = mockContext.mock(InternalPRInfo.class, "prInforR1");
    final PartitionMemberInfo mockR1M1Info = mockContext.mock(PartitionMemberInfo.class, "r1M1");
    final PartitionMemberInfo mockR1M2Info = mockContext.mock(PartitionMemberInfo.class, "r1M2");
    final HashSet<PartitionMemberInfo> r1Members = new HashSet<>();
    r1Members.add(mockR1M1Info);
    r1Members.add(mockR1M2Info);

    final InternalPRInfo mockR2PRInfo = mockContext.mock(InternalPRInfo.class, "prInforR2");
    final PartitionMemberInfo mockR2M1Info = mockContext.mock(PartitionMemberInfo.class, "r2M1");
    final HashSet<PartitionMemberInfo> r2Members = new HashSet<>();
    r2Members.add(mockR2M1Info);

    final Map<PartitionedRegion, InternalPRInfo> details = new HashMap<>();
    details.put(mockR1, mockR1PRInfo);
    details.put(mockR2, mockR2PRInfo);

    mockContext.checking(new Expectations() {
      {
        allowing(mockR1).getFullPath();
        allowing(mockR2).getFullPath();

        oneOf(mockR1PRInfo).getPartitionMemberInfo();
        will(returnValue(r1Members));
        atLeast(1).of(mockR1M1Info).getSize();
        will(returnValue(123L));
        atLeast(1).of(mockR1M2Info).getSize();
        will(returnValue(74L));

        oneOf(mockR2PRInfo).getPartitionMemberInfo();
        will(returnValue(r2Members));
        atLeast(1).of(mockR2M1Info).getSize();
        will(returnValue(3475L));
      }
    });

    GeodeCacheFacade facade = new GeodeCacheFacade() {
      @Override
      public Map<PartitionedRegion, InternalPRInfo> getRegionMemberDetails() {
        return details;
      }
    };

    assertEquals(123 + 74 + 3475, facade.getTotalDataSize(details));
  }

  @Test
  public void testAuditorInvocation() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(3);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockAuditor).init(with(any(Properties.class)));
        exactly(2).of(mockAuditor).execute();
        allowing(mockClock).currentTimeMillis();
        will(new CustomAction("returnTime") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            latch.countDown();
            return 990L;
          }
        });
      }
    });

    Properties props = AutoBalancerJUnitTest.getBasicConfig();

    assertEquals(3, latch.getCount());
    AutoBalancer autoR = new AutoBalancer(null, mockAuditor, mockClock, null);
    autoR.init(props);
    assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void destroyAutoBalancer() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(2);
    final CountDownLatch timerLatch = new CountDownLatch(1);
    final int timer = 20; // simulate 20 milliseconds

    mockContext.checking(new Expectations() {
      {
        oneOf(mockAuditor).init(with(any(Properties.class)));
        allowing(mockAuditor).execute();
        allowing(mockClock).currentTimeMillis();
        will(new CustomAction("returnTime") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            latch.countDown();
            if (latch.getCount() == 0) {
              assertTrue(timerLatch.await(1, TimeUnit.SECONDS));
              // scheduler is destroyed before wait is over
              fail();
            }
            return 1000L - timer;
          }
        });
      }
    });

    Properties props = AutoBalancerJUnitTest.getBasicConfig();

    assertEquals(2, latch.getCount());
    AutoBalancer autoR = new AutoBalancer(null, mockAuditor, mockClock, null);
    autoR.init(props);
    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // after destroy no more execute will be called.
    autoR.destroy();
    timerLatch.countDown();
    TimeUnit.MILLISECONDS.sleep(2 * timer);
  }

  static Properties getBasicConfig() {
    Properties props = new Properties();
    // every second schedule
    props.put(AutoBalancer.SCHEDULE, "* 0/30 * * * ?");
    return props;
  }
}
