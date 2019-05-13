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

  private static final int TOTAL_NUM_BUCKETS = 3;
  private static final int REDUNDANT_COPIES = 0;

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

  private enum FunctionIdentifierType {
    STRING, OBJECT_REFERENCE
  }

  private enum ClientMetadataStatus {
    CLIENT_HAS_METADATA,
    CLIENT_MISSING_METADATA
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

  @Test
  // TODO: 2 keys matching filter; redundancy 1; 2 retry attempts
  @Parameters({
      "false | CLIENT_MISSING_METADATA | REGION_WITH_FILTER | OBJECT_REFERENCE | 1",
      "false | CLIENT_MISSING_METADATA | REGION             | OBJECT_REFERENCE | 1",
      "false | CLIENT_MISSING_METADATA | SERVER             | OBJECT_REFERENCE | 1",
      "true  | CLIENT_MISSING_METADATA | REGION_WITH_FILTER | OBJECT_REFERENCE | 3",
      "true  | CLIENT_HAS_METADATA     | REGION_WITH_FILTER | OBJECT_REFERENCE | 3",
      "true  | CLIENT_MISSING_METADATA | REGION             | OBJECT_REFERENCE | 3",
      "true  | CLIENT_HAS_METADATA     | REGION             | OBJECT_REFERENCE | 3"
  })
  public void testAll(
      final boolean isHA,
      final ClientMetadataStatus clientMetadataStatus,
      final ExecutionTarget executionTarget,
      final FunctionIdentifierType functionIdentifierType,
      final int expectedCalls) throws Exception {

    final TheFunction function = new TheFunction(isHA);

    client.invoke(() -> {
      createClientRegion();
      FunctionService.registerFunction(function);
    });
    server1.invoke(() -> {
      createServerRegion();
      FunctionService.registerFunction(function);
    });
    server2.invoke(() -> {
      createServerRegion();
      FunctionService.registerFunction(function);
    });
    server3.invoke(() -> {
      createServerRegion();
      FunctionService.registerFunction(function);
    });

    client.invoke(() -> {
      final Region<Object, Object> region =
          clusterStartupRule.getClientCache().getRegion(regionName);

      /*
       Create at least one entry.

       Absence/presence of metadata in the client determines which op variant the product uses
       e.g. single-hop versus non-single-hop. Those variants have divergent (copypasta) code,
       so they must each be tested.

       We found empirically that creating more entries caused metadata to be loaded in the client.
       With few entries (empirically, 4 or fewer) metadata has not been acquired by the time the
       function is executed. With more than 4 entries, metadata has been acquired.
       */
      final int numberOfEntries;
      switch (clientMetadataStatus) {
        case CLIENT_HAS_METADATA:
          numberOfEntries = 10;
          break;
        case CLIENT_MISSING_METADATA:
          numberOfEntries = 1;
          break;
        default:
          throw new TestException("unknown ClientMetadataStatus: " + clientMetadataStatus);
      }

      for (int i = 0; i < numberOfEntries; i++) {
        region.put("k" + i, "v" + i);
      }
    });

    // TODO: remove this once this test is passing (it's here for debugging)
    IgnoredException.addIgnoredException(FunctionException.class.getName());

    client.invoke(() -> {

      assertThat(executionTarget).isNotNull();

      switch (functionIdentifierType) {
        case STRING:
          FunctionService.registerFunction(function);
          break;
        case OBJECT_REFERENCE:
          // no-op
          break;
        default:
          throw new TestException("unknown FunctionIdentifierType: " + functionIdentifierType);
      }

      final Execution<Integer, Long, List<Long>> execution;

      switch (executionTarget) {
        case REGION:
          execution =
              FunctionService.onRegion(clusterStartupRule.getClientCache().getRegion(regionName))
                  .setArguments(200);
          break;
        case REGION_WITH_FILTER:
          final HashSet<String> filter = new HashSet<String>(Arrays.asList("k0"));
          execution =
              FunctionService.onRegion(clusterStartupRule.getClientCache().getRegion(regionName))
                  .setArguments(200).withFilter(filter);
          break;
        case SERVER:
          execution = FunctionService.onServer(clusterStartupRule.getClientCache().getDefaultPool())
              .setArguments(200);
          break;
        default:
          throw new TestException("unknown ExecutionTarget: " + executionTarget);
      }

      ResultCollector<Long, List<Long>> resultCollector = null;

      try {
        switch (functionIdentifierType) {
          case STRING:
            resultCollector = execution.execute(function.getId());
            break;
          case OBJECT_REFERENCE:
            resultCollector = execution.execute(function);
            break;
          default:
            throw new TestException("unknown FunctionIdentifierType: " + functionIdentifierType);
        }
      } catch (final FunctionException e) {
        assertThat(e.getCause()).isInstanceOf(ServerConnectivityException.class);
      }

      if (resultCollector != null) {
        try {
          resultCollector.getResult();
        } catch (Exception ex) {
          System.out.println("#### Exception while collecting the result: " + ex.getMessage());
        }
      }
    });

    System.out.println("#### Number of functions executed on all servers :"
        + getNumberOfFunctionCalls(function.getId()));
    assertThat(getNumberOfFunctionCalls(function.getId())).isEqualTo(expectedCalls);
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

  private ClientVM startClient(int vmIndex) throws Exception {
    return clusterStartupRule.startClientVM(vmIndex, cacheRule -> cacheRule
        .withCacheSetup(fnTimeOut ->
            System
                .setProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", "100"))
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
    paf.setRedundantCopies(REDUNDANT_COPIES);
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);
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
        // no-op
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
