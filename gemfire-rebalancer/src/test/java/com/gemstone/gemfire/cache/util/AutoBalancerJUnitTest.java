package com.gemstone.gemfire.cache.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.util.AutoBalancer.AuditScheduler;
import com.gemstone.gemfire.cache.util.AutoBalancer.CacheOperationFacade;
import com.gemstone.gemfire.cache.util.AutoBalancer.GeodeCacheFacade;
import com.gemstone.gemfire.cache.util.AutoBalancer.OOBAuditor;
import com.gemstone.gemfire.cache.util.AutoBalancer.SizeBasedOOBAuditor;
import com.gemstone.gemfire.cache.util.AutoBalancer.TimeProvider;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.partitioned.InternalPRInfo;
import com.gemstone.gemfire.internal.cache.partitioned.LoadProbe;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AutoBalancerJUnitTest {
  GemFireCacheImpl cache;
  Mockery mockContext;

  @Before
  public void setupMock() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };
  }

  @After
  public void destroyCacheAndDLS() {
    if (DLockService.getServiceNamed(AutoBalancer.AUTO_BALANCER_LOCK_SERVICE_NAME) != null) {
      DLockService.destroy(AutoBalancer.AUTO_BALANCER_LOCK_SERVICE_NAME);
    }

    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
  }

  @After
  public void validateMock() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test(expected = IllegalStateException.class)
  public void testNoCacheError() {
    AutoBalancer balancer = new AutoBalancer();
    OOBAuditor auditor = balancer.getOOBAuditor();
    auditor.execute();
  }

  @Test
  public void testAutoRebalaceStatsOnLockSuccess() throws InterruptedException {
    cache = createBasicCache();

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        will(returnValue(true));
        oneOf(mockCacheFacade).incrementAttemptCounter();
        will(new CustomAction("increment stat") {
          public Object invoke(Invocation invocation) throws Throwable {
            new GeodeCacheFacade().incrementAttemptCounter();
            return null;
          }
        });
        allowing(mockCacheFacade);
      }
    });

    assertEquals(0, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
    balancer.getOOBAuditor().execute();
    assertEquals(1, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
  }

  @Test
  public void testAutoRebalaceStatsOnLockFailure() throws InterruptedException {
    cache = createBasicCache();

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        will(returnValue(false));
      }
    });

    assertEquals(0, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
    balancer.getOOBAuditor().execute();
    assertEquals(0, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
  }

  @Test
  public void testAutoBalanceStatUpdate() {
    cache = createBasicCache();
    assertEquals(0, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
    new GeodeCacheFacade().incrementAttemptCounter();
    assertEquals(1, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
  }

  @Test
  public void testLockSuccess() throws InterruptedException {
    cache = createBasicCache();

    final CountDownLatch locked = new CountDownLatch(1);
    final AtomicBoolean success = new AtomicBoolean(true);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        CacheOperationFacade cacheFacade = new GeodeCacheFacade();
        success.set(cacheFacade.acquireAutoBalanceLock());
        locked.countDown();
      }
    });
    thread.start();
    locked.await(1, TimeUnit.SECONDS);
    assertTrue(success.get());
    DistributedLockService dls = new GeodeCacheFacade().getDLS();
    assertFalse(dls.lock(AutoBalancer.AUTO_BALANCER_LOCK, 0, -1));
  }

  @Test
  public void testLockAlreadyTakenElsewhere() throws InterruptedException {
    cache = createBasicCache();

    DistributedLockService dls = new GeodeCacheFacade().getDLS();
    assertTrue(dls.lock(AutoBalancer.AUTO_BALANCER_LOCK, 0, -1));

    final AtomicBoolean success = new AtomicBoolean(true);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        CacheOperationFacade cacheFacade = new GeodeCacheFacade();
        success.set(cacheFacade.acquireAutoBalanceLock());
      }
    });
    thread.start();
    thread.join();
    assertFalse(success.get());
  }

  @Test
  public void testLockStatExecuteInSequence() throws InterruptedException {
    cache = createBasicCache();

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
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

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
    balancer.getOOBAuditor().execute();
  }

  @Test
  public void testReusePreAcquiredLock() throws InterruptedException {
    cache = createBasicCache();

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        will(returnValue(true));
        exactly(2).of(mockCacheFacade).incrementAttemptCounter();
        exactly(2).of(mockCacheFacade).getTotalTransferSize();
        will(returnValue(0L));
      }
    });

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
    balancer.getOOBAuditor().execute();
    balancer.getOOBAuditor().execute();
  }

  @Test
  public void testAcquireLockAfterReleasedRemotely() throws InterruptedException {
    cache = createBasicCache();

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
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

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
    balancer.getOOBAuditor().execute();
    balancer.getOOBAuditor().execute();
  }

  @Test
  public void testFailExecuteIfLockedElsewhere() throws InterruptedException {
    cache = createBasicCache();

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        will(returnValue(false));
        // no other methods, rebalance, will be called
      }
    });

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        AutoBalancer balancer = new AutoBalancer();
        balancer.setCacheOperationFacade(mockCacheFacade);
        balancer.getOOBAuditor().execute();
      }
    });
    thread.start();
    thread.join();
  }

  @Test
  public void testFailExecuteIfBalanced() throws InterruptedException {
    cache = createBasicCache();

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        will(returnValue(true));
        never(mockCacheFacade).rebalance();
        oneOf(mockCacheFacade).incrementAttemptCounter();
      }
    });

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);

    SizeBasedOOBAuditor auditor = balancer.new SizeBasedOOBAuditor() {
      @Override
      boolean needsRebalancing() {
        return false;
      }
    };
    balancer.setOOBAuditor(auditor);
    balancer.getOOBAuditor().execute();
  }

  @Test
  public void testOOBWhenBelowSizeThreshold() {
    final long totalSize = 1000L;

    final Map<PartitionedRegion, InternalPRInfo> details = new HashMap<>();
    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
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

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
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

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
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

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
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
    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
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

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
    Properties config = getBasicConfig();
    config.put(AutoBalancer.MINIMUM_SIZE, "10");
    balancer.init(config);
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();

    // first run
    assertTrue(auditor.needsRebalancing());

    // second run
    assertTrue(auditor.needsRebalancing());
  }

  @Test
  public void testInitializerCacheXML() {
    String configStr = "<cache xmlns=\"http://schema.pivotal.io/gemfire/cache\"                          "
        + " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"                                      "
        + " xsi:schemaLocation=\"http://schema.pivotal.io/gemfire/cache http://schema.pivotal.io/gemfire/cache/cache-9.0.xsd\""
        + " version=\"9.0\">                                                                             "
        + "   <initializer>                                                                              "
        + "     <class-name>com.gemstone.gemfire.cache.util.AutoBalancer</class-name>                    "
        + "     <parameter name=\"schedule\">                                                            "
        + "       <string>* * * * * ? </string>                                                          "
        + "     </parameter>                                                                             "
        + "   </initializer>                                                                             "
        + " </cache>";

    cache = createBasicCache();
    cache.loadCacheXml(new ByteArrayInputStream(configStr.getBytes()));
  }

  private GemFireCacheImpl createBasicCache() {
    return (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").create();
  }

  @Test(expected = GemFireConfigException.class)
  public void testInitFailOnMissingScheduleConf() {
    String configStr = "<cache xmlns=\"http://schema.pivotal.io/gemfire/cache\"                          "
        + " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"                                      "
        + " xsi:schemaLocation=\"http://schema.pivotal.io/gemfire/cache http://schema.pivotal.io/gemfire/cache/cache-9.0.xsd\""
        + " version=\"9.0\">                                                                             "
        + "   <initializer>                                                                              "
        + "     <class-name>com.gemstone.gemfire.cache.util.AutoBalancer</class-name>                    "
        + "   </initializer>                                                                             "
        + " </cache>";

    cache = createBasicCache();
    cache.loadCacheXml(new ByteArrayInputStream(configStr.getBytes()));
  }

  @Test
  public void testAuditorInvocation() throws InterruptedException {
    int count = 0;

    final OOBAuditor mockAuditor = mockContext.mock(OOBAuditor.class);
    final TimeProvider mockClock = mockContext.mock(TimeProvider.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockAuditor).init(with(any(Properties.class)));
        exactly(2).of(mockAuditor).execute();
        allowing(mockClock).currentTimeMillis();
        will(returnValue(950L));
      }
    });

    Properties props = getBasicConfig();

    assertEquals(0, count);
    AutoBalancer autoR = new AutoBalancer();
    autoR.setOOBAuditor(mockAuditor);
    autoR.setTimeProvider(mockClock);

    // the trigger should get invoked after 50 milliseconds
    autoR.init(props);

    TimeUnit.MILLISECONDS.sleep(120);
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

    final AuditScheduler mockScheduler = mockContext.mock(AuditScheduler.class);
    final OOBAuditor mockAuditor = mockContext.mock(OOBAuditor.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockScheduler).init(someSchedule);
        oneOf(mockAuditor).init(props);
      }
    });

    AutoBalancer autoR = new AutoBalancer();
    autoR.setScheduler(mockScheduler);
    autoR.setOOBAuditor(mockAuditor);

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

    GeodeCacheFacade facade = new GeodeCacheFacade() {
      @Override
      GemFireCacheImpl getCache() {
        return mockCache;
      }
    };

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
        oneOf(mockCache).getPartitionedRegions();
        will(returnValue(new HashSet<PartitionedRegion>()));
      }
    });

    GeodeCacheFacade facade = new GeodeCacheFacade() {
      @Override
      GemFireCacheImpl getCache() {
        return mockCache;
      }
    };

    assertEquals(0, facade.getRegionMemberDetails().size());
  }

  @Test
  public void testFacadeCollectMemberDetails2Regions() {
    cache = createBasicCache();

    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class);

    final PartitionedRegion mockR1 = mockContext.mock(PartitionedRegion.class, "r1");
    final PartitionedRegion mockR2 = mockContext.mock(PartitionedRegion.class, "r2");
    final HashSet<PartitionedRegion> regions = new HashSet<>();
    regions.add(mockR1);
    regions.add(mockR2);

    final PRHARedundancyProvider mockRedundancyProviderR1 = mockContext.mock(PRHARedundancyProvider.class, "prhaR1");
    final InternalPRInfo mockR1PRInfo = mockContext.mock(InternalPRInfo.class, "prInforR1");

    final PRHARedundancyProvider mockRedundancyProviderR2 = mockContext.mock(PRHARedundancyProvider.class, "prhaR2");
    final InternalPRInfo mockR2PRInfo = mockContext.mock(InternalPRInfo.class, "prInforR2");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).getPartitionedRegions();
        will(returnValue(regions));
        exactly(2).of(mockCache).getResourceManager();
        will(returnValue(cache.getResourceManager()));
        allowing(mockR1).getFullPath();
        oneOf(mockR1).getRedundancyProvider();
        will(returnValue(mockRedundancyProviderR1));
        allowing(mockR2).getFullPath();
        oneOf(mockR2).getRedundancyProvider();
        will(returnValue(mockRedundancyProviderR2));

        oneOf(mockRedundancyProviderR1).buildPartitionedRegionInfo(with(true), with(any(LoadProbe.class)));
        will(returnValue(mockR1PRInfo));

        oneOf(mockRedundancyProviderR2).buildPartitionedRegionInfo(with(true), with(any(LoadProbe.class)));
        will(returnValue(mockR2PRInfo));
      }
    });

    GeodeCacheFacade facade = new GeodeCacheFacade() {
      @Override
      GemFireCacheImpl getCache() {
        return mockCache;
      }
    };

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

  private Properties getBasicConfig() {
    Properties props = new Properties();
    // every second schedule
    props.put(AutoBalancer.SCHEDULE, "* * * * * ?");
    return props;
  }
}
