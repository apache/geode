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
package org.apache.geode.cache.lucene;


import static org.apache.geode.cache.lucene.test.IndexRepositorySpy.doOnce;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.DEFAULT_FIELD;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.lucene.internal.LuceneIndexFactorySpy;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.test.IndexRepositorySpy;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketResponse;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

public class LuceneQueriesAccessorBase extends LuceneDUnitTest {

  protected VM accessor;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    accessor = Host.getHost(0).getVM(3);
  }

  protected void putDataInRegion(VM vm) {
    vm.invoke(() -> {
      final Cache cache = getCache();
      Region<Object, Object> region = cache.getRegion(REGION_NAME);
      region.put(1, new TestObject("hello world"));
      region.put(113, new TestObject("hi world"));
      region.put(2, new TestObject("goodbye world"));
    });
  }

  protected boolean waitForFlushBeforeExecuteTextSearch(VM vm, int ms) {
    return vm.invoke(() -> {
      Cache cache = getCache();

      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneIndexImpl index = (LuceneIndexImpl) service.getIndex(INDEX_NAME, REGION_NAME);

      return service.waitUntilFlushed(INDEX_NAME, REGION_NAME, ms, TimeUnit.MILLISECONDS);
    });
  }

  protected void executeTextSearch(VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<Object, Object> region = cache.getRegion(REGION_NAME);

      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneQuery<Integer, TestObject> query;
      query = service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "text:world",
          DEFAULT_FIELD);
      PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
      assertEquals(3, results.size());
      List<LuceneResultStruct<Integer, TestObject>> page = results.next();

      Map<Integer, TestObject> data = new HashMap<>();
      for (LuceneResultStruct<Integer, TestObject> row : page) {
        data.put(row.getKey(), row.getValue());
      }

      assertEquals(new HashMap(region), data);
      return null;
    });
  }

  protected void executeTextSearch(VM vm, String queryString, String defaultField,
      int expectedResultsSize) {
    vm.invoke(() -> {
      Cache cache = getCache();

      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneQuery<Integer, TestObject> query;
      query = service.createLuceneQueryFactory().setLimit(1000).setPageSize(1000).create(INDEX_NAME,
          REGION_NAME, queryString, defaultField);
      Collection<?> results = query.findKeys();

      assertEquals(expectedResultsSize, results.size());
    });
  }

  protected void executeTextSearchWithExpectedException(VM vm, String queryString,
      String defaultField, Class expctedExceptionClass) {
    vm.invoke(() -> {
      Cache cache = getCache();

      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneQuery<Integer, TestObject> query;
      query = service.createLuceneQueryFactory().setLimit(1000).setPageSize(1000).create(INDEX_NAME,
          REGION_NAME, queryString, defaultField);
      try {
        Collection<?> results = query.findKeys();
        fail("Query " + defaultField + ":" + queryString + " should not have succeeded");
      } catch (Exception e) {
        assertEquals(expctedExceptionClass, e.getClass());
      }
    });
  }

  protected void addCallbackToTriggerRebalance(VM vm) {
    vm.invoke(() -> {
      IndexRepositorySpy spy = IndexRepositorySpy.injectSpy();

      spy.beforeWriteIndexRepository(doOnce(key -> rebalanceRegion(vm)));
    });
  }

  protected void addCallbackToMoveBucket(VM vm, final DistributedMember destination) {
    vm.invoke(() -> {
      IndexRepositorySpy spy = IndexRepositorySpy.injectSpy();

      spy.beforeWriteIndexRepository(doOnce(key -> moveBucket(destination, key)));
    });
  }

  protected void addCallbackToMovePrimary(VM vm, final DistributedMember destination) {
    vm.invoke(() -> {
      IndexRepositorySpy spy = IndexRepositorySpy.injectSpy();

      spy.beforeWriteIndexRepository(doOnce(key -> movePrimary(destination, key)));
    });
  }

  protected void addCallbackToMovePrimaryOnQuery(VM vm, final DistributedMember destination) {
    vm.invoke(() -> {
      LuceneIndexFactorySpy factorySpy = LuceneIndexFactorySpy.injectSpy();

      factorySpy.setGetRespositoryConsumer(doOnce(key -> moveBucket(destination, key)));
    });
  }

  private void moveBucket(final DistributedMember destination, final Object key) {
    Region<Object, Object> region = getCache().getRegion(REGION_NAME);
    DistributedMember source = getCache().getDistributedSystem().getDistributedMember();
    PartitionRegionHelper.moveBucketByKey(region, source, destination, key);
  }

  private void movePrimary(final DistributedMember destination, final Object key) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(REGION_NAME);

    BecomePrimaryBucketResponse response =
        BecomePrimaryBucketMessage.send((InternalDistributedMember) destination, region,
            region.getKeyInfo(key).getBucketId(), true);
    assertNotNull(response);
    assertTrue(response.waitForResponse());
  }

  protected void removeCallback(VM vm) {
    vm.invoke(() -> {
      IndexRepositorySpy.remove();
      InitialImageOperation.resetAllGIITestHooks();
      LuceneIndexFactorySpy.remove();
    });
  }

  protected void rebalanceRegion(VM vm) {
    // Do a rebalance
    vm.invoke(() -> {
      RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();
      RebalanceResults results = op.getResults();
    });
  }

  protected static class TestObject implements DataSerializable {
    private static final long serialVersionUID = 1L;
    private String text;

    public TestObject() {}

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
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TestObject other = (TestObject) obj;
      if (text == null) {
        return other.text == null;
      } else
        return text.equals(other.text);
    }

    @Override
    public String toString() {
      return "TestObject[" + text + "]";
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeUTF(text);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      text = in.readUTF();
    }
  }
}
