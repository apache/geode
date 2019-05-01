package org.apache.geode.internal.cache.execute;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;

import junitparams.JUnitParamsRunner;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import util.TestException;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
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
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", "100");
    locator = clusterStartupRule.startLocatorVM(0);
    server1 = startServer(1);
    server2 = startServer(2);
    server3 = startServer(3);


    client = clusterStartupRule.startClientVM(4, cacheRule -> cacheRule
        // yes, Virginia, the only way to affect client fn timeout is via a system property!
        .withCacheSetup(_ignore -> System
            .setProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", "100"))
        .withLocatorConnection(locator.getPort()));
    /*
     * This was an attempt to limit client wait time for fn execution. It didn't work (you gotta use
     * the system property above. Keeping this snippet in case we want to use it for non-fn op
     * timeouts later.
     * .withCacheSetup(cacheFactory -> cacheFactory.setPoolReadTimeout(100)));
     */
  }

  @After
  public void tearDown() throws Exception {
    IgnoredException.removeAllExpectedExceptions();
  }

  private MemberVM startServer(int vmIndex) {
    return clusterStartupRule.startServerVM(vmIndex,
        x -> x.withConnectionToLocator(locator.getPort())
            .withProperty(SERIALIZABLE_OBJECT_FILTER,
                "org.apache.geode.internal.cache.execute.FunctionTimeoutTest*"));
  }

  private void createServerRegion() {
    final PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(0);
    paf.setTotalNumBuckets(3);
    clusterStartupRule.getCache().createRegionFactory(PARTITION)
        .setPartitionAttributes(paf.create())
        .create(regionName);
  }

  private void createClientRegion() {
    clusterStartupRule.getClientCache().createClientRegionFactory(CACHING_PROXY).create(regionName);
  }

  /*
   * @Parameters({
   * "false, 0, 10, false", // !isHA => no retries, timeout==0 => no timeout
   * // "true, 1000, 10, false", // isHA => 1 retry, timeout >> thinkTime => no timeout
   * // "true, 10, 1000, true" // isHA => 1 retry, timeout << thinkTime => timeout
   * })
   */
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

    logger.info("#### Executing function from client.");
    IgnoredException.addIgnoredException(FunctionException.class.getName());

    Function function = client.invoke(() -> executeFunction(ExecutionTarget.REGION, false, 1000));

    System.out.println("#### Number of functions executed on all servers :"
        + getNumFunctionExecutionFromAllServers(function.getId()));
    assertThat(getNumFunctionExecutionFromAllServers(function.getId())).isEqualTo(3);
  }

  private int getNumFunctionExecutionFromAllServers(String functionId) {
    return getNumFunctions(server1, functionId) +
        getNumFunctions(server2, functionId) +
        getNumFunctions(server3, functionId);
  }

  private int getNumFunctions(MemberVM vm, final String functionId) {
    return vm.invoke(() -> {
      int numExecutions = 0;
      FunctionStats functionStats = FunctionStats.getFunctionStats(functionId);
      if (functionStats != null) {
        GeodeAwaitility.await("Awaiting GatewayReceiverMXBean.isRunning(true)")
            .untilAsserted(() -> assertThat(functionStats.getFunctionExecutionsRunning()).isZero());
        numExecutions = functionStats.getFunctionExecutionCalls();
      }
      return numExecutions;
    });
  }

  private Function executeFunction(final ExecutionTarget executionTarget, final boolean isHA,
      final int thinkTimeMillis) {
    assertThat(executionTarget).isNotNull();

    Function<Integer> function = new TheFunction(isHA);
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

    ResultCollector<Long, List<Long>> resultCollector = null;
    try {
      resultCollector = execution.execute(function);
    } catch (FunctionException fe) {
      assertThat(fe.getCause()).isInstanceOf(ServerConnectivityException.class);
    }

    if (resultCollector != null) {
      try {
        resultCollector.getResult();
      } catch (Exception ex) {
        System.out.println("#### Exception while collecting the result: " + ex.getMessage());
      }
    }
    return function;
  }

  private static class TheFunction implements Function<Integer> {

    private boolean isHA;

    public TheFunction(boolean isHA) {
      this.isHA = isHA;
    }

    @Override
    public void execute(final FunctionContext<Integer> context) {
      LogService.getLogger().info("#### Executing function on server.");
      final int thinkTimeMillis = context.getArguments();
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
