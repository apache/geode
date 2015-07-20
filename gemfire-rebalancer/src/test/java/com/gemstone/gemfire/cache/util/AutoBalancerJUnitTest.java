package com.gemstone.gemfire.cache.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import com.gemstone.gemfire.cache.util.AutoBalancer.AuditScheduler;
import com.gemstone.gemfire.cache.util.AutoBalancer.CacheOperationFacade;
import com.gemstone.gemfire.cache.util.AutoBalancer.GeodeCacheFacade;
import com.gemstone.gemfire.cache.util.AutoBalancer.OOBAuditor;
import com.gemstone.gemfire.cache.util.AutoBalancer.TimeProvider;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AutoBalancerJUnitTest {

  // OOB > threshold
  // OOB < threshold
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

    // every second schedule
    String someSchedule = "* * * * * ?";
    Properties props = new Properties();
    props.put(AutoBalancer.SCHEDULE, someSchedule);

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
  public void testAutoBalancerInit() {
    final String someSchedule = "1 * * * 1 *";
    final Properties props = new Properties();
    props.put(AutoBalancer.SCHEDULE, someSchedule);

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
  public void testNullInitialization() {
    AutoBalancer autoR = new AutoBalancer();
    autoR.init(null);
  }
}
