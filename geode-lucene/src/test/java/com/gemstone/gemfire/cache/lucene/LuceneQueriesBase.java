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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

/**
  * This test class is intended to contain basic integration tests
  * of the lucene query class that should be executed against a number
  * of different regions types and topologies.
  *
  */
public abstract class LuceneQueriesBase extends LuceneDUnitTest {

  private static final long serialVersionUID = 1L;
  protected VM accessor;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    accessor = Host.getHost(0).getVM(3);
  }

  protected abstract void initAccessor(SerializableRunnableIF createIndex) throws Exception;

  @Test
  public void returnCorrectResultsFromStringQueryWithDefaultAnalyzer() {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex));
    dataStore2.invoke(() -> initDataStore(createIndex));
    accessor.invoke(() -> initAccessor(createIndex));

    putDataInRegion(accessor);
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));
    executeTextSearch(accessor);
  }

  @Test
  public void defaultFieldShouldPropogateCorrectlyThroughFunction() {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex));
    dataStore2.invoke(() -> initDataStore(createIndex));
    accessor.invoke(() -> initAccessor(createIndex));
    putDataInRegion(accessor);
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));
    executeTextSearch(accessor, "world", "text", 3);
    executeTextSearch(accessor, "world", "noEntriesMapped", 0);
  }

  @Test
  public void canQueryWithCustomLuceneQueryObject() {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex));
    dataStore2.invoke(() -> initDataStore(createIndex));
    accessor.invoke(() -> initAccessor(createIndex));
    putDataInRegion(accessor);
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    //Execute a query with a custom lucene query object
    accessor.invoke(() -> {
      Cache cache = getCache();
      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneQuery query = service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, index ->  {
        return new TermQuery(new Term("text", "world"));
      });
      final PageableLuceneQueryResults results = query.findPages();
      assertEquals(3, results.size());
    });
  }

  protected boolean waitForFlushBeforeExecuteTextSearch(VM vm, int ms) {
    return vm.invoke(() -> {
      Cache cache = getCache();

      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneIndexImpl index = (LuceneIndexImpl)service.getIndex(INDEX_NAME, REGION_NAME);

      return index.waitUntilFlushed(ms);
    });
  }

  protected void executeTextSearch(VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<Object, Object> region = cache.getRegion(REGION_NAME);

      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneQuery<Integer, TestObject> query;
      query = service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "text:world", DEFAULT_FIELD);
      PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
      assertEquals(3, results.size());
      List<LuceneResultStruct<Integer, TestObject>> page = results.next();

      Map<Integer, TestObject> data = new HashMap<Integer, TestObject>();
      for (LuceneResultStruct<Integer, TestObject> row : page) {
        data.put(row.getKey(), row.getValue());
      }

      assertEquals(new HashMap(region),data);
      return null;
    });
  }

  protected void executeTextSearch(VM vm, String queryString, String defaultField, int expectedResultsSize) {
    vm.invoke(() -> {
      Cache cache = getCache();

      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneQuery<Integer, TestObject> query;
      query = service.createLuceneQueryFactory()
        .setResultLimit(1000)
        .setPageSize(1000)
        .create(INDEX_NAME, REGION_NAME, queryString, defaultField);
      Collection<?> results = query.findKeys();

      assertEquals(expectedResultsSize, results.size());
    });
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

  protected static class TestObject implements Serializable {
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

    @Override
    public String toString() {
      return "TestObject[" + text + "]";
    }
  }
}
