/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.query.dunit;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.util.test.TestUtil;

@Category(DistributedTest.class)
public class PartitionedRegionCompactRangeIndexDUnitTest extends JUnit4DistributedTestCase {

  private Properties getSystemProperties(String cacheXML) {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    props.setProperty(CACHE_XML_FILE, TestUtil.getResourcePath(getClass(), cacheXML));
    return props;
  }
  
  public void postSetUp() throws Exception {
    disconnectAllFromDS();
  }
  
  @Override
  public void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    QueryTestUtils.closeCacheInVM(vm0);
    QueryTestUtils.closeCacheInVM(vm1);
    QueryTestUtils.closeCacheInVM(vm2);
  }
  
  @Test
  public void testGIIUpdateWithIndexDoesNotDuplicateEntryInIndexWhenAlreadyRecoveredFromPersistence() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    //Adding due to known race condition for creation of partitioned indexes via cache.xml
    IgnoredException.addIgnoredException("IndexNameConflictException");
    
    String regionName = "persistentTestRegion"; //this region is created via cache.xml
    String idQuery = "select * from /" + regionName + " p where p.ID = 1";
    int idQueryExpectedSize = 1;
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    Map<String, Portfolio> newEntries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> newEntries.put("key-" + i,  new Portfolio(i + 1)));

    File rootDiskStore1 = QueryTestUtils.createRootDiskStoreInVM(vm0, "diskDir-PersistentPartitionWithIndexDiskStore");
    File rootDiskStore2 = QueryTestUtils.createRootDiskStoreInVM(vm1, "diskDir-PersistentPartitionWithIndexDiskStore");
    File rootDiskStore3 = QueryTestUtils.createRootDiskStoreInVM(vm2, "diskDir-PersistentPartitionWithIndexDiskStore");

    QueryTestUtils.createCacheInVM(vm0, getSystemProperties("PersistentPartitionWithIndex.xml"));
    QueryTestUtils.createCacheInVM(vm1, getSystemProperties("PersistentPartitionWithIndex.xml"));
    QueryTestUtils.createCacheInVM(vm2, getSystemProperties("PersistentPartitionWithIndex.xml"));
    QueryTestUtils.populateRegion(vm2, regionName, entries);
    
    vm1.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
    QueryTestUtils.closeCacheInVM(vm1);
    
    //update entries
    QueryTestUtils.populateRegion(vm0, regionName, entries);
    QueryTestUtils.closeCacheInVM(vm0);

    //restart 2 nodes
    QueryTestUtils.createCacheInVM(vm0, getSystemProperties("PersistentPartitionWithIndex.xml"));
    QueryTestUtils.createCacheInVM(vm1, getSystemProperties("PersistentPartitionWithIndex.xml"));

    vm2.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
    vm1.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
    vm0.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
  }
  
  private SerializableRunnable verifyQueryResultsSize(String query, int expectedSize) {
    return new SerializableRunnable() {
      public void run() {
        try {
          QueryService qs = QueryTestUtils.getInstance().getQueryService();
          Query q = qs.newQuery(query);
          SelectResults sr = (SelectResults) q.execute();
          assertEquals(expectedSize, sr.size());
        }
        catch (Exception e) {
          fail("Exception occurred when executing verifyQueryResultsSize for query:" + query);
        }
      }
    };
  }
}
