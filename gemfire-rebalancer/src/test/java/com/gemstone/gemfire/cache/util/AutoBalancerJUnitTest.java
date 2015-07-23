package com.gemstone.gemfire.cache.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.Properties;
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
import com.gemstone.gemfire.cache.util.AutoBalancer.AuditScheduler;
import com.gemstone.gemfire.cache.util.AutoBalancer.CacheOperationFacade;
import com.gemstone.gemfire.cache.util.AutoBalancer.GeodeCacheFacade;
import com.gemstone.gemfire.cache.util.AutoBalancer.OOBAuditor;
import com.gemstone.gemfire.cache.util.AutoBalancer.SizeBasedOOBAuditor;
import com.gemstone.gemfire.cache.util.AutoBalancer.TimeProvider;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AutoBalancerJUnitTest {

  // OOB > threshold && size < min
  // OOB > threshold && size < min
  // OOB critical nodes
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
  public void testAutoBalanceStatUpdate() {
    cache = createBasicCache();
    assertEquals(0, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
    new GeodeCacheFacade().incrementAttemptCounter();
    assertEquals(1, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
  }

  @Test
  public void testLockSuccess() throws InterruptedException {
    cache = createBasicCache();

    final DistributedLockService mockDLS = mockContext.mock(DistributedLockService.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockDLS).lock(AutoBalancer.AUTO_BALANCER_LOCK, 0L, -1L);
        will(new CustomAction("acquire lock") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            DistributedLockService dls = new GeodeCacheFacade().getDLS();
            return dls.lock(AutoBalancer.AUTO_BALANCER_LOCK, 0L, -1L);
          }
        });
      }
    });

    final AtomicBoolean success = new AtomicBoolean(false);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        CacheOperationFacade cacheFacade = new GeodeCacheFacade() {
          public DistributedLockService getDLS() {
            return mockDLS;
          };
        };
        success.set(cacheFacade.acquireAutoBalanceLock());
      }
    });
    thread.start();
    thread.join();
    assertTrue(success.get());
  }

  @Test
  public void testLockAlreadyTakenElsewhere() throws InterruptedException {
    cache = createBasicCache();

    DistributedLockService dls = new GeodeCacheFacade().getDLS();
    assertTrue(dls.lock(AutoBalancer.AUTO_BALANCER_LOCK, 0, -1));

    final DistributedLockService mockDLS = mockContext.mock(DistributedLockService.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockDLS).lock(AutoBalancer.AUTO_BALANCER_LOCK, 0L, -1L);
        will(new CustomAction("acquire lock") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            DistributedLockService dls = new GeodeCacheFacade().getDLS();
            return dls.lock(AutoBalancer.AUTO_BALANCER_LOCK, 0L, -1L);
          }
        });
      }
    });

    final AtomicBoolean success = new AtomicBoolean(true);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        CacheOperationFacade cacheFacade = new GeodeCacheFacade() {
          public DistributedLockService getDLS() {
            return mockDLS;
          }
        };
        success.set(cacheFacade.acquireAutoBalanceLock());
      }
    });
    thread.start();
    thread.join();
    assertFalse(success.get());
  }

  @Test
  public void testReleaseLock() throws InterruptedException {
    cache = createBasicCache();

    final AtomicBoolean success = new AtomicBoolean(false);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        DistributedLockService dls = new GeodeCacheFacade().getDLS();
        success.set(dls.lock(AutoBalancer.AUTO_BALANCER_LOCK, 0, -1));
      }
    });
    thread.start();
    thread.join();

    final DistributedLockService mockDLS = mockContext.mock(DistributedLockService.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockDLS).unlock(AutoBalancer.AUTO_BALANCER_LOCK);
        will(new CustomAction("release lock") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            DistributedLockService dls = new GeodeCacheFacade().getDLS();
            dls.unlock(AutoBalancer.AUTO_BALANCER_LOCK);
            return null;
          }
        });
      }
    });

    success.set(true);
    thread = new Thread(new Runnable() {
      @Override
      public void run() {
        CacheOperationFacade cacheFacade = new GeodeCacheFacade() {
          public DistributedLockService getDLS() {
            return mockDLS;
          }
        };
        try {
          cacheFacade.releaseAutoBalanceLock();
        } catch (Exception e) {
          success.set(false);
        }
      }
    });
    thread.start();
    thread.join();
    assertTrue(success.get());
  }

  @Test
  public void testLockSequence() throws InterruptedException {
    cache = createBasicCache();

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
    final Sequence lockingSequence = mockContext.sequence("lockingSequence");
    mockContext.checking(new Expectations() {
      {
        oneOf(mockCacheFacade).acquireAutoBalanceLock();
        inSequence(lockingSequence);
        will(returnValue(true));
        oneOf(mockCacheFacade).releaseAutoBalanceLock();
        inSequence(lockingSequence);
        allowing(mockCacheFacade);
      }
    });

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
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
        oneOf(mockCacheFacade).incrementAttemptCounter();
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
        oneOf(mockCacheFacade).releaseAutoBalanceLock();
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

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
    mockContext.checking(new Expectations() {
      {
        // first run
        oneOf(mockCacheFacade).getTotalDataSize();
        will(returnValue(totalSize));
        oneOf(mockCacheFacade).getTotalTransferSize();
        // half of threshold limit
        will(returnValue((AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT * totalSize / 100) / 2));

        // second run
        oneOf(mockCacheFacade).getTotalDataSize();
        will(returnValue(totalSize));
        oneOf(mockCacheFacade).getTotalTransferSize();
        // nothing to transfer
        will(returnValue(0L));
      }
    });

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
    balancer.init(getBasicConfig());
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();

    // first run
    assertFalse(auditor.needsRebalancing());

    // second run
    assertFalse(auditor.needsRebalancing());
  }

  @Test
  public void testOOBWhenBelowAboveThreshold() {
    final long totalSize = 1000L;

    final CacheOperationFacade mockCacheFacade = mockContext.mock(CacheOperationFacade.class);
    mockContext.checking(new Expectations() {
      {
        // first run
        oneOf(mockCacheFacade).getTotalDataSize();
        will(returnValue(totalSize));
        oneOf(mockCacheFacade).getTotalTransferSize();
        // twice threshold
        will(returnValue((AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT * totalSize / 100) * 2));

        // second run
        oneOf(mockCacheFacade).getTotalDataSize();
        will(returnValue(totalSize));
        oneOf(mockCacheFacade).getTotalTransferSize();
        // more than total size
        will(returnValue(2 * totalSize));
      }
    });

    AutoBalancer balancer = new AutoBalancer();
    balancer.setCacheOperationFacade(mockCacheFacade);
    balancer.init(getBasicConfig());
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

    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "17");
    balancer = new AutoBalancer();
    balancer.init(props);
    auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();
    assertEquals(17, auditor.getSizeThreshold());
  }

  @Test(expected = GemFireConfigException.class)
  public void testSizeThresholdNegative() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "-1");
    balancer.init(props);
  }

  @Test(expected = GemFireConfigException.class)
  public void testSizeThresholdZero() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "0");
    balancer.init(props);
  }

  @Test(expected = GemFireConfigException.class)
  public void testSizeThresholdToohigh() {
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
  
  private Properties getBasicConfig() {
    Properties props = new Properties();
    // every second schedule
    props.put(AutoBalancer.SCHEDULE, "* * * * * ?");
    return props;
  }
}
