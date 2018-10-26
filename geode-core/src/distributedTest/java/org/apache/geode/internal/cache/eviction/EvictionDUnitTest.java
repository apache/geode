package org.apache.geode.internal.cache.eviction;

import static java.lang.Math.abs;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryEvent;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.EvictionTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@Category({EvictionTest.class})
public class EvictionDUnitTest {

  @Parameterized.Parameters(name = "offHeap={0}")
  public static Collection booleans() {
    return Arrays.asList(true, false);
  }

  @Parameterized.Parameter
  public static boolean offHeap;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM server0, server1;

  @Before
  public void before() throws Exception {
    int locatorPort = ClusterStartupRule.getDUnitLocatorPort();
    Properties properties = new Properties();
    if (offHeap) {
      properties.setProperty(OFF_HEAP_MEMORY_SIZE, "200m");
    }
    server0 = cluster.startServerVM(0, s -> s.withNoCacheServer()
        .withProperties(properties).withConnectionToLocator(locatorPort));
    server1 = cluster.startServerVM(1, s -> s.withNoCacheServer()
        .withProperties(properties).withConnectionToLocator(locatorPort));

    VMProvider.invokeInEveryMember(() -> {
      HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
      System.setProperty("gemfire.memoryEventTolerance", "0");
      InternalCache cache = ClusterStartupRule.getCache();
      cache.getResourceManager().setEvictionHeapPercentage(85);
      if (offHeap) {
        cache.getInternalResourceManager().getOffHeapMonitor().stopMonitoring(true);
      }
    }, server0, server1);
  }

  @Test
  public void testDummyInlineNCentralizedEvictionAndPoolSize() {
    VMProvider.invokeInEveryMember(() -> {
      ServerStarterRule server = (ServerStarterRule) ClusterStartupRule.memberStarter;
      server.createPartitionRegion("PR1",
          f -> f.setOffHeap(offHeap).setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.LOCAL_DESTROY)),
          f -> f.setTotalNumBuckets(4).setRedundantCopies(0));

    }, server0, server1);

    server0.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion("PR1");
      for (int counter = 1; counter <= 50; counter++) {
        region.put(counter, new byte[1024 * 1024]);
      }
    });

    VMProvider.invokeInEveryMember(() -> {
      GemFireCacheImpl cache = (GemFireCacheImpl) ClusterStartupRule.getCache();
      HeapEvictor evictor = getEvictor(cache);
      int expectedEviction = (int) Math
          .ceil((evictor.getTotalBytesToEvict() / 2) / (double) ((1024 * 1024) + 100)) * 2;
      PartitionedRegion region = (PartitionedRegion) cache.getRegion("PR1");
      evictor.setTestAbortAfterLoopCount(1);

      InternalResourceManager irm = cache.getInternalResourceManager();
      HeapMemoryMonitor hmm = irm.getHeapMonitor();
      hmm.setTestMaxMemoryBytes(100);
      hmm.updateStateAndSendEvent(90);

      GeodeAwaitility.await()
          .until(() -> (abs(region.getTotalEvictions() - expectedEviction) <= 1));

      assertThat(region.getTotalEvictions()).isEqualTo(expectedEviction);

      ExecutorService evictorThreadPool = evictor.getEvictorThreadPool();
      if (evictorThreadPool != null) {
        long taskCount = ((ThreadPoolExecutor) evictorThreadPool).getTaskCount();
        assertThat(taskCount).isLessThanOrEqualTo(HeapEvictor.MAX_EVICTOR_THREADS);
      }

    }, server0, server1);
  }

  @Test
  public void testCheckEntryLruEvictionsIn2DataStore() {
    int maxEntries = 20;
    VMProvider.invokeInEveryMember(() -> {
      ServerStarterRule server = (ServerStarterRule) ClusterStartupRule.memberStarter;
      server.createPartitionRegion("PR1",
          f -> f.setOffHeap(offHeap)
              .setEvictionAttributes(
                  EvictionAttributes.createLRUEntryAttributes(maxEntries,
                      EvictionAction.LOCAL_DESTROY)),
          f -> f.setTotalNumBuckets(4).setRedundantCopies(0));

    }, server0, server1);

    // put 60 entries in server0's region
    server0.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion("PR1");
      for (int counter = 1; counter <= 60; counter++) {
        region.put(counter, new byte[1024 * 1024]);
      }
    });

    // validate the total # of evictions happening on both servers
    SerializableCallableIF getEvictions = () -> {
      final PartitionedRegion pr =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion("PR1");
      return pr.getTotalEvictions();
    };

    Long server0EvictionCount = (Long) server0.invoke(getEvictions);
    Long server1EvictionCount = (Long) server1.invoke(getEvictions);

    assertThat(server0EvictionCount + server1EvictionCount).isEqualTo(20);
  }

  @Test
  public void testCentralizedEvictionForDistributedRegionWithDummyEvent() {
    server0.invoke(() -> {
      GemFireCacheImpl cache = (GemFireCacheImpl) ClusterStartupRule.getCache();
      ServerStarterRule server = (ServerStarterRule) ClusterStartupRule.memberStarter;
      LocalRegion dr1 =
          (LocalRegion) server.createRegion(RegionShortcut.LOCAL, "DR1",
              f -> f.setOffHeap(offHeap).setDataPolicy(DataPolicy.NORMAL).setEvictionAttributes(
                  EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.LOCAL_DESTROY)));
      for (int counter = 1; counter <= 50; counter++) {
        dr1.put(new Integer(counter), new byte[1024 * 1024]);
      }

      HeapEvictor evictor = getEvictor(cache);
      evictor.setTestAbortAfterLoopCount(1);
      int expectedEviction = (int) Math
          .ceil((evictor.getTotalBytesToEvict() / 2) / (double) ((1024 * 1024) + 100)) * 2;

      InternalResourceManager irm = cache.getInternalResourceManager();
      HeapMemoryMonitor hmm = irm.getHeapMonitor();
      hmm.setTestMaxMemoryBytes(100);
      hmm.updateStateAndSendEvent(90);

      GeodeAwaitility.await().until(() -> (abs(dr1.getTotalEvictions() - expectedEviction) <= 1));

      assertThat(dr1.getTotalEvictions()).isEqualTo(expectedEviction);
    });
  }

  /**
   *
   * Test Case verifies:If naturally Eviction up and eviction Down events are raised. Centralized
   * and Inline eviction are happening. All this verification is done through logs. It also verifies
   * that during eviction, if one node goes down and then comes up again causing GII to take place,
   * the system doesnot throw an OOME.
   */
  @Test
  public void testEvictionWithNodeDown() throws Exception {
    SerializableRunnableIF setupVM = () -> {
      GemFireCacheImpl cache = (GemFireCacheImpl) ClusterStartupRule.getCache();
      final File[] diskDirs = new File[1];
      diskDirs[0] =
          new File("Partitioned_Region_Eviction/" + "LogFile" + "_" + OSProcess.getId());
      diskDirs[0].mkdirs();

      ServerStarterRule server = (ServerStarterRule) ClusterStartupRule.memberStarter;
      server.createPartitionRegion("PR1",
          f -> f.setOffHeap(offHeap).setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK))
              .setDiskSynchronous(true)
              .setDiskStoreName(cache.createDiskStoreFactory().setDiskDirs(diskDirs)
                  .create("EvictionTestBase").getName()),
          f -> f.setTotalNumBuckets(2).setRedundantCopies(1));

      server.createPartitionRegion("PR2",
          f -> f.setOffHeap(offHeap).setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK))
              .setDiskSynchronous(true)
              .setDiskStoreName(cache.createDiskStoreFactory().setDiskDirs(diskDirs)
                  .create("EvictionTestBase").getName()),
          f -> f.setTotalNumBuckets(2).setRedundantCopies(1));
    };

    VMProvider.invokeInEveryMember(setupVM, server0, server1);

    server0.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion("PR1");
      for (int counter = 1; counter <= 100; counter++) {
        region.put(counter, new byte[1024 * 1024]);
      }
    });

    // raise fake event
    VMProvider.invokeInEveryMember(() -> {
      GemFireCacheImpl cache = (GemFireCacheImpl) ClusterStartupRule.getCache();
      HeapEvictor evictor = getEvictor(cache);
      HeapMemoryMonitor hmm =
          ((InternalResourceManager) cache.getResourceManager()).getHeapMonitor();
      MemoryEvent event =
          new MemoryEvent(offHeap ? ResourceType.OFFHEAP_MEMORY : ResourceType.HEAP_MEMORY,
              MemoryThresholds.MemoryState.NORMAL, MemoryThresholds.MemoryState.EVICTION,
              cache.getDistributedSystem().getDistributedMember(), 90, true, hmm.getThresholds());
      evictor.onEvent(event);
    }, server0, server1);

    // stop server1
    server1.stop(false);

    // restart server1 and create the regions again and verify the data are preserved
    server1 = cluster.startServerVM(1, s -> s.withNoCacheServer()
        .withConnectionToLocator(ClusterStartupRule.getDUnitLocatorPort()));
    server1.invoke(setupVM);
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion("PR1").size()).isEqualTo(100);
      assertThat(cache.getRegion("PR2").size()).isEqualTo(0);
    });


  }

  private static HeapEvictor getEvictor(GemFireCacheImpl cache) {
    if (offHeap) {
      return cache.getOffHeapEvictor();
    } else {
      return cache.getHeapEvictor();
    }
  }
}
