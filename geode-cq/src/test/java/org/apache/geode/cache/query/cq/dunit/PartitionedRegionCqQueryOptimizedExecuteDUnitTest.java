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
package org.apache.geode.cache.query.cq.dunit;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.cq.CqServiceImpl;
import org.apache.geode.cache.query.internal.cq.CqServiceProvider;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.*;

@Category(DistributedTest.class)
public class PartitionedRegionCqQueryOptimizedExecuteDUnitTest
    extends PartitionedRegionCqQueryDUnitTest {

  public PartitionedRegionCqQueryOptimizedExecuteDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        CqServiceImpl.EXECUTE_QUERY_DURING_INIT = false;
      }
    });
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        CqServiceImpl.EXECUTE_QUERY_DURING_INIT = true;
        CqServiceProvider.MAINTAIN_KEYS = true;
      }
    });
  }

  @Test
  public void testCqExecuteWithoutQueryExecution() throws Exception {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final int numOfEntries = 10;
    final String cqName = "testCqExecuteWithoutQueryExecution_1";

    createServer(server);
    // Create values.
    createValues(server, regions[0], numOfEntries);

    final int thePort =
        server.invoke(() -> PartitionedRegionCqQueryOptimizedExecuteDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    // Create client.
    createClient(client, thePort, host0);

    /* Create CQs. */
    createCQ(client, cqName, cqs[0]);

    cqHelper.validateCQCount(client, 1);

    cqHelper.executeCQ(client, cqName, false, null);

    server.invoke(new CacheSerializableRunnable("execute cq") {
      public void run2() throws CacheException {
        assertFalse("CqServiceImpl.EXECUTE_QUERY_DURING_INIT flag should be false ",
            CqServiceImpl.EXECUTE_QUERY_DURING_INIT);
        int numOfQueryExecutions = (Integer) ((GemFireCacheImpl) getCache()).getCachePerfStats()
            .getStats().get("queryExecutions");
        assertEquals("Number of query executions for cq.execute should be 0 ", 0,
            numOfQueryExecutions);
      }
    });

    // Create more values.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = numOfEntries + 1; i <= numOfEntries * 2; i++) {
          region1.put(KEY + i, new Portfolio(i));
        }
        LogWriterUtils.getLogWriter()
            .info("### Number of Entries in Region :" + region1.keys().size());
      }
    });

    cqHelper.waitForCreated(client, cqName, KEY + numOfEntries * 2);

    cqHelper.validateCQ(client, cqName, /* resultSize: */ cqHelper.noTest,
        /* creates: */ numOfEntries, /* updates: */ 0, /* deletes; */ 0,
        /* queryInserts: */ numOfEntries, /* queryUpdates: */ 0, /* queryDeletes: */ 0,
        /* totalEvents: */ numOfEntries);

    // Update values.
    createValues(server, regions[0], 5);
    createValues(server, regions[0], 10);

    cqHelper.waitForUpdated(client, cqName, KEY + numOfEntries);


    // validate Update events.
    cqHelper.validateCQ(client, cqName, /* resultSize: */ cqHelper.noTest,
        /* creates: */ numOfEntries, /* updates: */ 15, /* deletes; */ 0,
        /* queryInserts: */ numOfEntries, /* queryUpdates: */ 15, /* queryDeletes: */ 0,
        /* totalEvents: */ numOfEntries + 15);

    // Validate delete events.
    cqHelper.deleteValues(server, regions[0], 5);
    cqHelper.waitForDestroyed(client, cqName, KEY + 5);

    cqHelper.validateCQ(client, cqName, /* resultSize: */ cqHelper.noTest,
        /* creates: */ numOfEntries, /* updates: */ 15, /* deletes; */5,
        /* queryInserts: */ numOfEntries, /* queryUpdates: */ 15, /* queryDeletes: */ 5,
        /* totalEvents: */ numOfEntries + 15 + 5);

    cqHelper.closeClient(client);
    cqHelper.closeServer(server);
  }

  @Test
  public void testCqExecuteWithoutQueryExecutionAndNoRSCaching() throws Exception {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final int numOfEntries = 10;
    final String cqName = "testCqExecuteWithoutQueryExecution_1";

    server.invoke(new CacheSerializableRunnable("execute cq") {
      public void run2() throws CacheException {
        CqServiceProvider.MAINTAIN_KEYS = false;
      }
    });

    createServer(server);
    // Create values.
    createValues(server, regions[0], numOfEntries);

    final int thePort = server.invoke(() -> PartitionedRegionCqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    // Create client.
    createClient(client, thePort, host0);

    /* Create CQs. */
    createCQ(client, cqName, cqs[0]);

    cqHelper.validateCQCount(client, 1);

    cqHelper.executeCQ(client, cqName, false, null);

    server.invoke(new CacheSerializableRunnable("execute cq") {
      public void run2() throws CacheException {
        assertFalse("CqServiceImpl.EXECUTE_QUERY_DURING_INIT flag should be false ",
            CqServiceImpl.EXECUTE_QUERY_DURING_INIT);
        assertFalse(DistributionConfig.GEMFIRE_PREFIX + "cq.MAINTAIN_KEYS flag should be false ",
            CqServiceProvider.MAINTAIN_KEYS);
        int numOfQueryExecutions = (Integer) ((GemFireCacheImpl) getCache()).getCachePerfStats()
            .getStats().get("queryExecutions");
        assertEquals("Number of query executions for cq.execute should be 0 ", 0,
            numOfQueryExecutions);
      }
    });

    // Create more values.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = numOfEntries + 1; i <= numOfEntries * 2; i++) {
          region1.put(KEY + i, new Portfolio(i));
        }
        LogWriterUtils.getLogWriter()
            .info("### Number of Entries in Region :" + region1.keys().size());
      }
    });

    cqHelper.waitForCreated(client, cqName, KEY + numOfEntries * 2);

    cqHelper.validateCQ(client, cqName, /* resultSize: */ cqHelper.noTest,
        /* creates: */ numOfEntries, /* updates: */ 0, /* deletes; */ 0,
        /* queryInserts: */ numOfEntries, /* queryUpdates: */ 0, /* queryDeletes: */ 0,
        /* totalEvents: */ numOfEntries);

    // Update values.
    createValues(server, regions[0], 5);
    createValues(server, regions[0], 10);

    cqHelper.waitForUpdated(client, cqName, KEY + numOfEntries);


    // validate Update events.
    cqHelper.validateCQ(client, cqName, /* resultSize: */ cqHelper.noTest,
        /* creates: */ numOfEntries, /* updates: */ 15, /* deletes; */ 0,
        /* queryInserts: */ numOfEntries, /* queryUpdates: */ 15, /* queryDeletes: */ 0,
        /* totalEvents: */ numOfEntries + 15);

    // Validate delete events.
    cqHelper.deleteValues(server, regions[0], 5);
    cqHelper.waitForDestroyed(client, cqName, KEY + 5);

    cqHelper.validateCQ(client, cqName, /* resultSize: */ cqHelper.noTest,
        /* creates: */ numOfEntries, /* updates: */ 15, /* deletes; */5,
        /* queryInserts: */ numOfEntries, /* queryUpdates: */ 15, /* queryDeletes: */ 5,
        /* totalEvents: */ numOfEntries + 15 + 5);

    cqHelper.closeClient(client);
    cqHelper.closeServer(server);
  }
}
