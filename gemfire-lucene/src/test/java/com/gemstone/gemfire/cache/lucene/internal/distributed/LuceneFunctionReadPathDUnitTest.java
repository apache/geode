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

package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import org.junit.Assert;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class LuceneFunctionReadPathDUnitTest extends CacheTestCase {
  private static final String INDEX_NAME = "index";

  private static final long serialVersionUID = 1L;

  private VM server1;
  private VM server2;

  public LuceneFunctionReadPathDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
  }

  public void testEnd2EndFunctionExecution() {
    e2eTextSearchForRegionType(RegionShortcut.PARTITION);
    e2eTextSearchForRegionType(RegionShortcut.PARTITION_PERSISTENT);
    e2eTextSearchForRegionType(RegionShortcut.PARTITION_OVERFLOW);
    e2eTextSearchForRegionType(RegionShortcut.PARTITION_PERSISTENT_OVERFLOW);
  }

  private void e2eTextSearchForRegionType(RegionShortcut type) {
    final String regionName = type.toString();
    createPartitionRegionAndIndex(server1, regionName, type);
    putDataInRegion(server1, regionName);
    createPartitionRegionAndIndex(server2, regionName, type);
    // Make sure we can search from both members
    executeTextSearch(server1, regionName);
    executeTextSearch(server2, regionName);

    rebalanceRegion(server1);
    // Make sure the search still works
    executeTextSearch(server1, regionName);
    executeTextSearch(server2, regionName);
    destroyRegion(server2, regionName);
  }

  private void rebalanceRegion(VM vm) {
    // Do a rebalance
    vm.invoke(new SerializableCallable<Object>() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws CancellationException, InterruptedException {
        RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();
        RebalanceResults results = op.getResults();
        assertTrue(1 < results.getTotalBucketTransfersCompleted());
        return null;
      }
    });
  }

  private void executeTextSearch(VM vm, final String regionName) {
    SerializableCallable<Object> executeSearch = new SerializableCallable<Object>("executeSearch") {
      private static final long serialVersionUID = 1L;

      public Object call() throws Exception {
        Cache cache = getCache();
        assertNotNull(cache);
        Region<Object, Object> region = cache.getRegion(regionName);
        Assert.assertNotNull(region);

        LuceneService service = LuceneServiceProvider.get(cache);
        LuceneQuery<Integer, TestObject> query;
        query = service.createLuceneQueryFactory().create(INDEX_NAME, regionName, "text:world");
        LuceneQueryResults<Integer, TestObject> results = query.search();
        assertEquals(3, results.size());
        List<LuceneResultStruct<Integer, TestObject>> page = results.getNextPage();

        Map<Integer, TestObject> data = new HashMap<Integer, TestObject>();
        for (LuceneResultStruct<Integer, TestObject> row : page) {
          data.put(row.getKey(), row.getValue());
        }

        assertEquals(data, region);
        return null;
      }
    };

    vm.invoke(executeSearch);
  }

  private void putDataInRegion(VM vm, final String regionName) {
    SerializableCallable<Object> createSomeData = new SerializableCallable<Object>("putDataInRegion") {
      private static final long serialVersionUID = 1L;

      public Object call() throws Exception {
        final Cache cache = getCache();
        Region<Object, Object> region = cache.getRegion(regionName);
        assertNotNull(region);
        region.put(1, new TestObject("hello world"));
        region.put(113, new TestObject("hi world"));
        region.put(2, new TestObject("goodbye world"));

        return null;
      }
    };

    vm.invoke(createSomeData);
  }

  private void createPartitionRegionAndIndex(VM vm, final String regionName, final RegionShortcut type) {
    SerializableCallable<Object> createPartitionRegion = new SerializableCallable<Object>("createRegionAndIndex") {
      private static final long serialVersionUID = 1L;

      public Object call() throws Exception {
        final Cache cache = getCache();
        assertNotNull(cache);
        LuceneService service = LuceneServiceProvider.get(cache);
        service.createIndex(INDEX_NAME, regionName, "text");
        RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(type);
        if (regionName.contains("OVERFLOW")) {
          System.out.println("yello");
          EvictionAttributesImpl evicAttr = new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
          evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
          regionFactory.setEvictionAttributes(evicAttr);
        }
        regionFactory.create(regionName);
        return null;
      }
    };
    vm.invoke(createPartitionRegion);
  }

  private void destroyRegion(VM vm, final String regionName) {
    SerializableCallable<Object> createPartitionRegion = new SerializableCallable<Object>("destroyRegion") {
      private static final long serialVersionUID = 1L;

      public Object call() throws Exception {
        final Cache cache = getCache();
        assertNotNull(cache);
        String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, regionName);
        PartitionedRegion chunkRegion = (PartitionedRegion) cache.getRegion(aeqId + ".chunks");
        assertNotNull(chunkRegion);
        chunkRegion.destroyRegion();
        PartitionedRegion fileRegion = (PartitionedRegion) cache.getRegion(aeqId + ".files");
        assertNotNull(fileRegion);
        fileRegion.destroyRegion();
        Region<Object, Object> region = cache.getRegion(regionName);
        assertNotNull(region);
        region.destroyRegion();
        return null;
      }
    };
    vm.invoke(createPartitionRegion);
  }

  private static class TestObject implements Serializable {
    private static final long serialVersionUID = 1L;
    private String text;

    public TestObject(String text) {
      this.text = text;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((text == null) ? 0 : text.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TestObject other = (TestObject) obj;
      if (text == null) {
        if (other.text != null)
          return false;
      } else if (!text.equals(other.text))
        return false;
      return true;
    }
  }
}
