package org.apache.geode.internal.cache.execute;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
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
    REGION, SERVER, REGION_WITH_FILTER
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
    client = startClient(4);
  }

  @After
  public void tearDown() throws Exception {
    IgnoredException.removeAllExpectedExceptions();
  }


  /*
   * @Parameters({
   * "false, 0, 10, false", // !isHA => no retries, timeout==0 => no timeout
   * // "true, 1000, 10, false", // isHA => 1 retry, timeout >> thinkTime => no timeout
   * // "true, 10, 1000, true" // isHA => 1 retry, timeout << thinkTime => timeout
   * })
   */


  @Test
  @Parameters({"false" /*, "true"*/})
  public void testClientServerFunction(boolean isHA) throws Exception {
    client.invoke(() -> createClientRegion());
    server1.invoke(() -> createServerRegion());
    server2.invoke(() -> createServerRegion());
    server3.invoke(() -> createServerRegion());
    Function function = new TheFunction(isHA);
    registerFunctionOnServers(function);

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

    client.invoke(() -> executeFunction(ExecutionTarget.REGION, 1000, null,
        function, true));

    System.out.println("#### Number of functions executed on all servers :"
        + getNumberOfFunctionCalls(function.getId()));
    assertThat(getNumberOfFunctionCalls(function.getId())).isEqualTo(3);
  }

  @Test
  @Parameters({
      //"false",
      "true"
  })
  public void testClientServerFunctionWithFilter(boolean isHA) throws Exception {
    final TheFunction function = new TheFunction(isHA);

    client.invoke(() -> createClientRegion());
    server1.invoke(() -> createServerRegion());
    server2.invoke(() -> createServerRegion());
    server3.invoke(() -> createServerRegion());

    client.invoke(() -> {
      final Region<Object, Object> region =
          clusterStartupRule.getClientCache().getRegion(regionName);
      /*
       with maxRetries set to 3 in ExecuteRegionFunctionOp.execute()...
       fn retries terminates w/ i < 5 but fails to terminate w/ i < 6
       */
      for (int i=1; i < 6; i++) {
//        region.put("k1", "v1");
//        region.put("k2", "v2");
//        region.put("k3", "v3");
        region.put("k" + i, "v" + i);
      }
      //FunctionService.registerFunction(function);
    });

    // not shouldn't strictly be needed in this case because we invoke fn via object ref below
    registerFunctionOnServers(function);

    logger.info("#### Executing function from client.");
    IgnoredException.addIgnoredException(FunctionException.class.getName());
    final HashSet<String> filter = new HashSet<String>(Arrays.asList("k1"));
    client.invoke(() -> {
      executeFunction(ExecutionTarget.REGION_WITH_FILTER, 200, filter,
          function, true);
    });

    System.out.println("#### Number of functions executed on all servers :"
        + getNumberOfFunctionCalls(function.getId()));
    assertThat(getNumberOfFunctionCalls(function.getId())).isEqualTo(3);
  }

  @Test
  @Parameters({"false" /*, "true"*/})
  public void testClientServerFunctionUsingId(boolean isHA) throws Exception {
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

    Function function = client.invoke(() -> executeFunction(ExecutionTarget.REGION, 1000, null,
        new TheFunction(isHA), false));

    System.out.println("#### Number of functions executed on all servers :"
        + getNumberOfFunctionCalls(function.getId()));
    assertThat(getNumberOfFunctionCalls(function.getId())).isEqualTo(3);
  }

  @Test
  @Parameters({
      //"false",
      "true"
  })
  public void testClientServerFunctionWithFilterUsingId(boolean isHA) throws Exception {
    final TheFunction function = new TheFunction(isHA);

    client.invoke(() -> createClientRegion());
    server1.invoke(() -> createServerRegion());
    server2.invoke(() -> createServerRegion());
    server3.invoke(() -> createServerRegion());

    client.invoke(() -> {
      final Region<Object, Object> region =
          clusterStartupRule.getClientCache().getRegion(regionName);
      region.put("k1", "v1");
      //FunctionService.registerFunction(function);
    });

    server1.invoke(() -> {
      final Region<Object, Object> region = clusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.get("k1")).isEqualTo("v1");
      FunctionService.registerFunction(function);
    });

    server2.invoke(() -> {
      FunctionService.registerFunction(function);
    });

    server3.invoke(() -> {
      FunctionService.registerFunction(function);
    });

    logger.info("#### Executing function from client.");
    IgnoredException.addIgnoredException(FunctionException.class.getName());
    final HashSet<String> filter = new HashSet<String>(Arrays.asList("k1"));
    client.invoke(() -> {
      executeFunction(ExecutionTarget.REGION_WITH_FILTER, 1000, filter,
          function, false);
    });

    System.out.println("#### Number of functions executed on all servers :"
        + getNumberOfFunctionCalls(function.getId()));
    assertThat(getNumberOfFunctionCalls(function.getId())).isEqualTo(1);
  }

  private int getNumberOfFunctionCalls(String functionId) {
    return getNumberOfFunctionCalls(server1, functionId) +
        getNumberOfFunctionCalls(server2, functionId) +
        getNumberOfFunctionCalls(server3, functionId);
  }

  private int getNumberOfFunctionCalls(MemberVM vm, final String functionId) {
    return vm.invoke(() -> {
      int numExecutions = 0;
      FunctionStats functionStats = FunctionStats.getFunctionStats(functionId);
      if (functionStats != null) {
        GeodeAwaitility.await("Awaiting GatewayReceiverMXBean.isRunning(true)")
            .untilAsserted(() -> assertThat(functionStats.getFunctionExecutionsRunning()).isZero());
        numExecutions = functionStats.getFunctionExecutionCalls();
      }
      logger.info("#### Number of functions completed: " + numExecutions);
      return numExecutions;
    });
  }

  private void registerFunctionOnServers(Function function) {
    SerializableRunnableIF runnable = () -> FunctionService.registerFunction(function);
    server1.invoke(runnable);
    server2.invoke(runnable);
    server3.invoke(runnable);
  }

  private Function executeFunction(final ExecutionTarget executionTarget,
                                   final int thinkTimeMillis, Set filter,
                                   Function<Integer> function, boolean register) {
    assertThat(executionTarget).isNotNull();

    if (register) {
      // This forces execution using function id
      FunctionService.registerFunction(function);
    }
    final Execution<Integer, Long, List<Long>> execution;

    switch (executionTarget) {
      case REGION:
        execution =
            FunctionService.onRegion(clusterStartupRule.getClientCache().getRegion(regionName))
                .setArguments(thinkTimeMillis);
        break;
      case REGION_WITH_FILTER:
        execution =
            FunctionService.onRegion(clusterStartupRule.getClientCache().getRegion(regionName))
                .setArguments(thinkTimeMillis).withFilter(filter);
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
      if (register) {
        resultCollector = execution.execute(function.getId());
      } else {
        resultCollector = execution.execute(function);
      }
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

  private ClientVM startClient(int vmIndex) throws Exception {
    return clusterStartupRule.startClientVM(vmIndex, cacheRule -> cacheRule
        .withCacheSetup(fnTimeOut ->
            System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", "100"))
        .withLocatorConnection(locator.getPort()));
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

  private static class TheFunction implements Function<Integer> {

    private boolean isHA;

    public TheFunction(boolean isHA) {
      this.isHA = isHA;
    }

    @Override
    public void execute(final FunctionContext<Integer> context) {
      LogService.getLogger().info("#### Function Executing on server...");
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
