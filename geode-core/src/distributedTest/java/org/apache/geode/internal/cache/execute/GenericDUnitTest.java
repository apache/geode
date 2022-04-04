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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.ProxyClientRequestObserver;
import org.apache.geode.internal.cache.ProxyClientRequestObserverHolder;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;


@Category({WanTest.class})
@RunWith(GeodeParamsRunner.class)
public class GenericDUnitTest implements Serializable {

  private static final int MAX_THREADS = 8;

  private static final String regionName = "GenericDUnitTest";

  @ClassRule
  public static final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  @Rule
  public DistributedRule distributedRule = new DistributedRule(6);
  MemberVM locator;
  MemberVM server1;
  MemberVM server2;
  MemberVM server3;
  MemberVM server4;
  ClientVM client;

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0);
    server1 = startServer(1, MAX_THREADS);
  }

  @After
  public void tearDown() throws Exception {
    IgnoredException.removeAllExpectedExceptions();
  }

  enum Operation {
    GET, PUT, TXPUT, GETFUNCTION, TXGETFUNCTION, PUTFUNCTION, TXPUTFUNCTION
  }

  @Test
  @Parameters({"GET", "PUT", "TXPUT", "GETFUNCTION", "TXGETFUNCTION", "PUTFUNCTION",
      "TXPUTFUNCTION"})
  @TestCaseName("{method}(operation:{params})")
  public void testSuccessfulExecution(Operation operation) throws Exception {
    IgnoredException.addIgnoredException(java.lang.IllegalStateException.class);

    server1 = startServer(1, MAX_THREADS);
    server2 = startServer(2, MAX_THREADS);
    server3 = startServer(3, MAX_THREADS);
    server4 = startServer(4, MAX_THREADS);

    List<MemberVM> serversInA =
        Arrays.asList(server1, server2, server3, server4);

    // Set client connect-timeout to a very high value so that if there are no ServerConnection
    // threads available the test will time-out before the client times-out.
    int connectTimeout = (int) GeodeAwaitility.getTimeout().toMillis() * 2;

    client = startClient(5, 0, connectTimeout);

    Function testFunction = new TestFunction();

    for (MemberVM memberVM : serversInA) {
      createServerRegionAndRegisterFunction(memberVM, 1, testFunction);
    }

    client.invoke(() -> createClientRegion());

    client.invoke(() -> executeQuery(regionName));

    Object key = "key";
    Object value = "value";

    int invocationsNo = 50;
    AsyncInvocation[] invocations = new AsyncInvocation[invocationsNo];

    IntStream.range(0, invocationsNo).forEach(i -> invocations[i] = client.invokeAsync(() -> {
      String finalKey = key + "" + i;
      String finalValue = finalKey;
      try {
        switch (operation) {
          case GET: {
            doGet(finalKey, regionName);
            break;
          }
          case PUT: {
            doPut(finalKey, finalValue, regionName);
            break;
          }
          case TXPUT: {
            doTxPut(finalKey, finalValue, regionName);
            break;
          }
          case GETFUNCTION: {
            executeGetFunction(testFunction, regionName, finalKey, i, false);
            break;
          }
          case TXGETFUNCTION: {
            executeGetFunction(testFunction, regionName, finalKey, i, true);
            break;
          }
          case PUTFUNCTION: {
            executePutFunction(testFunction, regionName, finalKey, finalValue, i, false);
            break;
          }
          case TXPUTFUNCTION: {
            executePutFunction(testFunction, regionName, finalKey, finalValue, i, true);
            break;
          }
        }
      } catch (Exception e) {
        System.out.println(
            "toberal exception calling " + operation + " operation with key: " + finalKey + ", "
                + e);
      }
    }));

    // Sleep a bit to make sure that entries are replicated
    Thread.sleep(5000);

    // Run several times to make sure that the results are the same in all servers.
    IntStream.range(0, 10).forEach(x -> {
      client.invoke(() -> executeQuery(regionName));
    });
  }

  void doPut(Object key, Object value, String regionName) {
    ClientCache cache = ClusterStartupRule.getClientCache();
    Region region = cache.getRegion(regionName);
    System.out.println("toberal before put");
    region.put(key, value);
    System.out.println("toberal after put");
  }

  void doTxPut(Object key, Object value, String regionName) {
    ClientCache cache = ClusterStartupRule.getClientCache();
    Region region = cache.getRegion(regionName);
    CacheTransactionManager txManager =
        cache.getCacheTransactionManager();
    txManager.begin();
    try {
      System.out.println("toberal before put");
      region.put(key, value);
      System.out.println("toberal after put");
      System.out.println("toberal before committing: " + key);
      txManager.commit();
      System.out.println("toberal after committing: " + key);
    } catch (CommitConflictException conflict) {
      System.out.println("toberal CommitConflictException");
      // ... do necessary work for a transaction that failed on commit
    } catch (Exception e) {
      System.out.println("toberal Exception putting: " + e);
      // ... do necessary work for a transaction that failed on commit
    } finally {
      if (txManager.exists()) {
        txManager.rollback();
      }
    }
  }

  Object doGet(Object key, String regionName) {
    ClientCache cache = ClusterStartupRule.getClientCache();
    Region region = cache.getRegion(regionName);
    System.out.println("toberal before get");
    Object value = region.get(key);
    System.out.println("toberal after get: " + value);
    return value;
  }

  void executeQuery(String regionName) {
    ClientCache cache = ClusterStartupRule.getClientCache();
    String queryString = "select * from /" + regionName;

    // Get QueryService from Cache.
    QueryService queryService = cache.getQueryService();

    // Create the Query Object.
    Query query = queryService.newQuery(queryString);

    // Execute Query locally. Returns results set.
    Object[] params = new Object[0];
    SelectResults results = null;
    try {
      results = (SelectResults) query.execute(params);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("toberal query results: (" + results.size() + ") - " + results);
  }

  private Object executePutFunction(Function function, String regionName, Object key, Object value,
      int invocation, boolean useTransaction) {
    FunctionService.registerFunction(function);
    final Region<Object, Object> region =
        ClusterStartupRule.getClientCache().getRegion(regionName);

    Execution execution = FunctionService.onRegion(region);

    ResultCollector resultCollector;
    Object[] args = {useTransaction, invocation, key, value};
    Set filter = new HashSet();
    filter.add(key);
    // without filter, results are weird. Check, toberal
    resultCollector =
        execution.setArguments(args).withFilter(filter).execute(function.getId());
    Object result = resultCollector.getResult();
    return result;
  }

  private Object executeGetFunction(Function function, String regionName, Object key,
      int invocation, boolean useTransaction) {
    FunctionService.registerFunction(function);
    final Region<Object, Object> region =
        ClusterStartupRule.getClientCache().getRegion(regionName);

    Execution execution = FunctionService.onRegion(region);

    ResultCollector resultCollector;
    Object[] args = {useTransaction, invocation, key};
    Set filter = new HashSet();
    filter.add(key);
    System.out.println("toberal before executeGetFunction. key: " + key);
    resultCollector =
        execution.setArguments(args).withFilter(filter).execute(function.getId());
    Object result = resultCollector.getResult();
    System.out.println("toberal after executeGetFunction. key: " + key);
    return result;
  }

  private void createServerRegion(int redundantCopies) {
    final PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(redundantCopies);
    ClusterStartupRule.getCache().createRegionFactory(PARTITION)
        .setPartitionAttributes(paf.create())
        .create(regionName);
  }

  private void createServerRegionAndRegisterFunction(MemberVM server, int redundantCopies,
      final Function function) {
    server.invoke(() -> {
      createServerRegion(redundantCopies);
      FunctionService.registerFunction(function);
      // DistributionMessageObserver.setInstance(new MyMessageObserver());
      int maxThreadsPerDestination = MAX_THREADS / 4;
      ProxyClientRequestObserverHolder
          .setInstance(new ThreadLimitingProxyClientRequestObserver(maxThreadsPerDestination));
    });
  }

  private ClientVM startClient(final int vmIndex, final int retryAttempts, final int connectTimeout)
      throws Exception {
    return clusterStartupRule.startClientVM(
        vmIndex,
        cacheRule -> cacheRule
            .withLocatorConnection(locator.getPort())
            .withCacheSetup(cf -> cf.setPoolRetryAttempts(retryAttempts)
                .setPoolPRSingleHopEnabled(false)
                .setPoolSocketConnectTimeout(connectTimeout)));
  }

  private MemberVM startServer(final int vmIndex, int maxThreads) {
    return clusterStartupRule.startServerVM(
        vmIndex,
        cacheRule -> cacheRule
            .withProperty(SERIALIZABLE_OBJECT_FILTER,
                "org.apache.geode.internal.cache.execute.GenericDUnitTest*")
            // .withMaxThreads(maxThreads)
            .withConnectionToLocator(locator.getPort()));
  }

  private void createClientRegion() {
    ClusterStartupRule.getClientCache().createClientRegionFactory(CACHING_PROXY).create(regionName);
  }

  static class TestFunction implements Function<Object[]>, Serializable {
    public TestFunction() {
      super();
    }

    @Override
    public void execute(FunctionContext<Object[]> context) {
      final Object[] args = context.getArguments();
      if (args.length < 2) {
        throw new IllegalStateException(
            "Arguments length does not match required length.");
      }
      Object value = null;
      if (args.length == 4) {
        value = args[3];
      }
      boolean useTransaction = (boolean) args[0];
      Integer invocation = (Integer) args[1];
      Object key = args[2];

      RegionFunctionContext regionFunctionContext = (RegionFunctionContext) context;
      Region region = regionFunctionContext.getDataSet();
      Object result = null;
      try {
        CacheTransactionManager txManager =
            context.getCache().getCacheTransactionManager();
        if (useTransaction) {
          txManager.begin();
        }
        try {
          logger.info("toberal inv: {} before getting/putting: {}", invocation, key);
          if (value != null) {
            result = region.put(key, value);
          } else {
            result = region.get(key);
            // Thread.sleep(100);
          }
          logger.info("toberal inv: {} after getting/putting: {}", invocation, key);
          if (useTransaction) {
            logger.info("toberal inv: {} before committing: {}", invocation, key);
            txManager.commit();
            logger.info("toberal inv: {} after committing: {}", invocation, key);
          }
        } catch (CommitConflictException conflict) {
          logger.info("toberal inv: {} CommitConflictException: {}, invocation, key: {}",
              invocation, conflict, key);
          // ... do necessary work for a transaction that failed on commit
        } catch (Exception e) {
          logger.info("toberal inv: {} Exception: {}, invocation, key: {}", invocation, e, key, e);
          // ... do necessary work for a transaction that failed on commit
        } finally {
          if (txManager.exists()) {
            logger.info("toberal inv: {} rolling-back: {}, invocation, key: {}", invocation, key);
            txManager.rollback();
          } else {
            logger.info("toberal inv: {} not rolling-back: {}, invocation, key: {}", invocation,
                key);
          }
        }
      } catch (Exception e) {
        context.getResultSender().lastResult(e);
        logger.info("toberal inv: {} after returning last result with exception: {}", invocation,
            e);
      }
      context.getResultSender().lastResult(result);
      logger.info("toberal inv: {} after returning last result: {}", invocation, result);
    }

    public static final String ID = TestFunction.class.getName();

    private static final Logger logger = LogService.getLogger();

    @Override
    public String getId() {
      return ID;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }
  }


  public static class MyMessageObserver extends DistributionMessageObserver {
    private static final Logger logger = LogService.getLogger();

    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      logger.info("toberal beforeProcessMessage dm: {}, message: {}, sender: {}", dm, message,
          message.getSender());
    }

    /**
     * Called after the process method of the DistributionMessage is called
     *
     * @param dm the distribution manager that received the message
     * @param message The message itself
     */
    public void afterProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      logger.info("toberal afterProcessMessage dm: {}, message: {}", dm, message);
    }

    /**
     * Called just before a message is distributed.
     *
     * @param dm the distribution manager that's sending the message
     * @param message the message itself
     */
    public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
      logger.info("toberal beforeSendMessage dm: {}, message: {}", dm, message, new Exception());
    }
  }

  public static class ThreadLimitingProxyClientRequestObserver
      implements ProxyClientRequestObserver {
    private final int maxThreadsToDestination;

    public ThreadLimitingProxyClientRequestObserver(int maxThreadsToDestination) {
      this.maxThreadsToDestination = maxThreadsToDestination;
    }

    private static final Logger logger = LogService.getLogger();
    private final Map<InternalDistributedMember, Integer> threadsToDestination =
        new ConcurrentHashMap();

    @Override
    public void beforeSendRequest(Set<InternalDistributedMember> members) {
      logger.info(
          "toberal beforeSendRequest members: {}, threadsToDestination: size:{}, contents:{}",
          members, threadsToDestination.size(), threadsToDestination);
      if (Thread.currentThread().getName().startsWith("ServerConnection on")
          || Thread.currentThread().getName().startsWith("Function Execution Processor")) {
        for (InternalDistributedMember member : members) {
          if (threadsToDestination.getOrDefault(member, 0) >= maxThreadsToDestination) {
            logger.info("toberal Max number of threads reached for " + member, new Exception("kk"));
            throw new IllegalStateException("Max number of threads reached");
          }
        }
        for (InternalDistributedMember member : members) {
          threadsToDestination.merge(member, 1, Integer::sum);
        }
      }
    }

    @Override
    public void afterReceiveResponse(Set<InternalDistributedMember> members) {
      logger.info(
          "toberal afterReceiveResponse removing members: {}, threadsToDestination: size:{}, contents:{}",
          members, threadsToDestination.size(), threadsToDestination);
      for (InternalDistributedMember member : members) {
        if (Thread.currentThread().getName().startsWith("ServerConnection on")
            || Thread.currentThread().getName().startsWith("Function Execution Processor")) {
          threadsToDestination.merge(member, -1, Integer::sum);
          logger.info(
              "toberal afterReceiveResponse removing member: {}, threadsToDestination: size:{}, contents:{}",
              member, threadsToDestination.size(), threadsToDestination);
        }
      }
    }
  }
}
