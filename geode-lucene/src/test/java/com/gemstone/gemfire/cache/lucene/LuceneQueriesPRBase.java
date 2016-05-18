/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * This test class adds more basic tests of lucene functionality
 * for partitioned regions. These tests should work across all types
 * of PRs and topologies.
 *
 */
public abstract class LuceneQueriesPRBase extends LuceneQueriesBase {

  @Test
  public void returnCorrectResultsAfterRebalance() {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex));
    accessor.invoke(() -> initAccessor(createIndex));
    putDataInRegion(accessor);
    dataStore2.invoke(() -> initDataStore(createIndex));

    rebalanceRegion(dataStore1);
    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    executeTextSearch(accessor);
  }

  private void rebalanceRegion(VM vm) {
    // Do a rebalance
    vm.invoke(() -> {
        RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();
        RebalanceResults results = op.getResults();
        assertTrue("Transferred " + results.getTotalBucketTransfersCompleted(), 1 < results.getTotalBucketTransfersCompleted());
    });
  }

}
