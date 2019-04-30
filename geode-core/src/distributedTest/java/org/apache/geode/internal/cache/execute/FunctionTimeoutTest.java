package org.apache.geode.internal.cache.execute;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.test.dunit.DistributedTestUtils.getLocatorPort;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import util.TestException;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class FunctionTimeoutTest implements Serializable {

  private static final int TOTAL_NUM_BUCKETS = 4;
  private static final int REDUNDANT_COPIES = 1;

  private static String regionName = "FunctionTimeoutTest";

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;
  private ClientVM client;

  private Logger logger = LogService.getLogger();

  private enum RegionType {
    PARTITION, REPLICATE
  }

  private enum ExecutionTarget {
    REGION, SERVER
  }

  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0);
    server1 = startServer(1);
    server2 = startServer(2);
    server3 = startServer(3);

    client = clusterStartupRule.startClientVM(4,
        cacheRule -> cacheRule.withLocatorConnection(locator.getPort())
            .withCacheSetup(cacheFactory -> cacheFactory.setPoolReadTimeout(100)));
  }

  @After
  public void tearDown() throws Exception {

  }

  private MemberVM startServer(int vmIndex) {
    return clusterStartupRule.startServerVM(vmIndex,
        x -> x.withConnectionToLocator(locator.getPort())
            .withProperty(SERIALIZABLE_OBJECT_FILTER,
                "org.apache.geode.internal.cache.execute.FunctionTimeoutTest*"));
  }

  private void createServerRegion() {
    clusterStartupRule.getCache().createRegionFactory(PARTITION).create(regionName);
  }

  private void createClientRegion() {
    clusterStartupRule.getClientCache().createClientRegionFactory(CACHING_PROXY).create(regionName);
  }

  @Test
  public void testClientServer() {
    client.invoke(() -> createClientRegion());
    server1.invoke(() -> createServerRegion());
    server2.invoke(() -> createServerRegion());
    server3.invoke(() -> createServerRegion());

    client.invoke(() -> {
      final Region<Object, Object> region =
          clusterStartupRule.getClientCache().getRegion(regionName);
      region.put("k1", "v1");
    });

    server1.invoke(() -> {
      final Region<Object, Object> region = clusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.get("k1")).isEqualTo("v1");
    });
    server2.invoke(() -> {
      final Region<Object, Object> region = clusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.get("k1")).isEqualTo("v1");
    });
    server3.invoke(() -> {
      final Region<Object, Object> region = clusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.get("k1")).isEqualTo("v1");
    });

    try {
      logger.info("#### Executing function from client.");
      client.invoke(() -> executeFunction(ExecutionTarget.REGION, false, 1000));
    } catch (final Throwable e) {
      logger.info("#### Exception Executing function from client: " + e.getMessage());
      if (true /* expect exception */) {
        assertThat(getFunctionExecutionExceptionCause(e)).as("verify the precise exception type")
            .isExactlyInstanceOf(ServerConnectivityException.class);
      } else {
        throw new TestException("Geode threw unexpected exception", e);
      }
    }
  }

  @Test
  @Parameters({
      "false, 0, 10, false", // !isHA => no retries, timeout==0 => no timeout
      // "true, 1000, 10, false", // isHA => 1 retry, timeout >> thinkTime => no timeout
      // "true, 10, 1000, true" // isHA => 1 retry, timeout << thinkTime => timeout
  })
  public void doFoo(final boolean isHA, final int timeoutMillis,
      final int thinkTimeMillis, final boolean expectServerConnectivityException) {
    final int port = server1.invoke(() -> createServerCache(RegionType.REPLICATE));
    // client.invoke(() -> createClientCache(client.getHost().getHostName(), port, timeoutMillis));

    try {
      client.invoke(() -> executeFunction(ExecutionTarget.REGION, isHA, thinkTimeMillis));
    } catch (final Throwable e) {
      if (expectServerConnectivityException) {
        assertThat(getFunctionExecutionExceptionCause(e)).as("verify the precise exception type")
            .isExactlyInstanceOf(ServerConnectivityException.class);
      } else {
        throw new TestException("Geode threw unexpected exception", e);
      }
    }
  }

  private Throwable getFunctionExecutionExceptionCause(final Throwable fromDUnit) {
    return Optional.ofNullable(fromDUnit)
        .map(_fromDUnit -> _fromDUnit.getCause())
        .map(fromTheFunction -> fromTheFunction.getCause()).orElseGet(null);
  }

  private void createClientCache(final String hostName, final int port, final int timeout) {
    if (timeout > 0) {
      System.setProperty(GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", String.valueOf(timeout));
    }

    // final Properties config = new Properties();
    // config.setProperty(LOCATORS, "");
    // config.setProperty(MCAST_PORT, "0");

    final ClientCacheFactory clientCacheFactory = new ClientCacheFactory(/* config */);
    clientCacheFactory.addPoolServer(hostName, port);

    final InternalClientCache clientCache =
        (InternalClientCache) clientCacheFactory.create();

    final ClientRegionFactory<String, String> clientRegionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    clientRegionFactory.create(regionName);
  }

  private int createServerCache(final RegionType regionType) throws IOException {
    assertThat(regionType).isNotNull();

    final Properties config = new Properties();
    config.setProperty(LOCATORS, "localhost[" + getLocatorPort() + "]");
    config.setProperty(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.FunctionTimeoutTest*");

    final InternalCache serverCache = (InternalCache) new CacheFactory(config).create();

    final RegionFactory<String, String> regionFactory;

    switch (regionType) {
      case PARTITION:
        final PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
        paf.setRedundantCopies(REDUNDANT_COPIES);
        paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

        regionFactory = serverCache.createRegionFactory(RegionShortcut.PARTITION);
        regionFactory.setPartitionAttributes(paf.create());
        break;
      case REPLICATE:
        regionFactory = serverCache.createRegionFactory(RegionShortcut.REPLICATE);
        break;
      default:
        throw new TestException("unknown region type: " + regionType);
    }

    regionFactory.create(regionName);

    final CacheServer server = serverCache.addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void executeFunction(
      final ExecutionTarget executionTarget, final boolean isHA, final int thinkTimeMillis) {
    assertThat(executionTarget).isNotNull();

    final Function<Integer> function = new TheFunction(isHA);
    FunctionService.registerFunction(function);
    final Execution<Integer, Long, List<Long>> execution;

    switch (executionTarget) {
      case REGION:
        execution =
            FunctionService.onRegion(clusterStartupRule.getClientCache().getRegion(regionName))
                .setArguments(thinkTimeMillis);
        break;
      case SERVER:
        execution = FunctionService.onServer(clusterStartupRule.getClientCache().getDefaultPool())
            .setArguments(thinkTimeMillis);
        break;
      default:
        throw new TestException("unknown ExecutionTarget: " + executionTarget);
    }

    try {
      final ResultCollector<Long, List<Long>> resultCollector = execution.execute(function);
    } catch (final Throwable e) {
      throw new TestException("Geode threw an unexpected exception", e);
    }
  }

  private static class TheFunction implements Function<Integer> {
    // private Logger logger = LogService.getLogger();

    private boolean isHA;

    public TheFunction(boolean isHA) {
      this.isHA = isHA;
    }

    @Override
    public void execute(final FunctionContext<Integer> context) {
      System.out.println("#### Executing function on server.");
      final int thinkTimeMillis = context.getArguments();
      System.out.println("#### Function arg thinkTimeMillis: " + thinkTimeMillis);
      final long elapsed = getElapsed(() -> Thread.sleep(thinkTimeMillis));
      context.getResultSender().lastResult(elapsed);
    }

    @FunctionalInterface
    private interface Thunk {
      void apply() throws InterruptedException;
    }

    private long getElapsed(final Thunk thunk) {
      final long start = System.currentTimeMillis();
      try {
        thunk.apply();
      } catch (final InterruptedException e) {
        // TODO: do something here
      }
      final long end = System.currentTimeMillis();
      return end - start;
    }

    @Override
    public boolean isHA() {
      return isHA;
    }
  }

}
