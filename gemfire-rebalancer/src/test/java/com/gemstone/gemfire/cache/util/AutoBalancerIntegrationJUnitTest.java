package com.gemstone.gemfire.cache.util;

import static com.jayway.awaitility.Awaitility.*;
import static java.util.concurrent.TimeUnit.*;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jmock.Expectations;
import org.jmock.Mockery;
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
import com.gemstone.gemfire.cache.util.AutoBalancer.CacheOperationFacade;
import com.gemstone.gemfire.cache.util.AutoBalancer.GeodeCacheFacade;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.internal.HostStatSampler;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.partitioned.InternalPRInfo;
import com.gemstone.gemfire.internal.cache.partitioned.LoadProbe;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * IntegrationTests for AutoBalancer that include usage of Cache, StatSampler 
 * and DistributedLockService. Some collaborators may be mocked while others
 * are real.
 * 
 * <p>Extracted from AutoBalancerJUnitTest
 */
@Category(IntegrationTest.class)
public class AutoBalancerIntegrationJUnitTest {
  
  private static final int TIMEOUT_SECONDS = 5;

  private GemFireCacheImpl cache;
  private Mockery mockContext;

  @Before
  public void setupMock() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };
  }
  
  @Before
  public void setUpCacheAndDLS() {
    cache = createBasicCache();
  }

  @After
  public void destroyCacheAndDLS() {
    if (DLockService.getServiceNamed(AutoBalancer.AUTO_BALANCER_LOCK_SERVICE_NAME) != null) {
      DLockService.destroy(AutoBalancer.AUTO_BALANCER_LOCK_SERVICE_NAME);
    }

    if (cache != null && !cache.isClosed()) {
      try {
        final HostStatSampler statSampler = ((InternalDistributedSystem)cache.getDistributedSystem()).getStatSampler();
        cache.close();
        // wait for the stat sampler to stand down
        await().atMost(TIMEOUT_SECONDS, SECONDS).until(isAlive(statSampler), equalTo(false));
      } finally {
        cache = null;
      }
    }
  }
  
  @After
  public void validateMock() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testAutoRebalaceStatsOnLockSuccess() throws InterruptedException {
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
    assertEquals(0, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
    new GeodeCacheFacade().incrementAttemptCounter();
    
    assertEquals(1, cache.getResourceManager().getStats().getAutoRebalanceAttempts());
  }
  
  @Test
  public void testLockSuccess() throws InterruptedException {
    final AtomicBoolean acquiredAutoBalanceLock = new AtomicBoolean(true);
    
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        CacheOperationFacade cacheFacade = new GeodeCacheFacade();
        acquiredAutoBalanceLock.set(cacheFacade.acquireAutoBalanceLock());
      }
    });
    thread.start();
    
    await().atMost(TIMEOUT_SECONDS, SECONDS).untilTrue(acquiredAutoBalanceLock);
    
    DistributedLockService dls = new GeodeCacheFacade().getDLS();
    assertFalse(dls.lock(AutoBalancer.AUTO_BALANCER_LOCK, 0, -1));
  }

  @Test
  public void testLockAlreadyTakenElsewhere() throws InterruptedException {
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

    cache.loadCacheXml(new ByteArrayInputStream(configStr.getBytes()));
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

    cache.loadCacheXml(new ByteArrayInputStream(configStr.getBytes()));
  }

  @Test
  public void testFacadeCollectMemberDetails2Regions() {
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

  private GemFireCacheImpl createBasicCache() {
    return (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").create();
  }

  private Callable<Boolean> isAlive(final HostStatSampler statSampler) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return statSampler.isAlive();
      }
    };
  }
}
