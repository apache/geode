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
package org.apache.geode.cache.lucene.internal.management;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import com.jayway.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.management.ObjectName;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.junit.Assert.*;

@Category(DistributedTest.class)
public class LuceneManagementDUnitTest extends ManagementTestBase {

  @Test
  public void testMBeanAndProxiesCreated() throws Exception {
    initManagement(true);

    // Verify MBean is created in each managed node
    for (VM vm : getManagedNodeList()) {
      vm.invoke(() -> verifyMBean());
    }

    // Verify MBean proxies are created in the managing node
    getManagingNode().invoke(() -> verifyMBeanProxies());
  }

  @Test
  public void testMBeanIndexMetricsCreatedOneRegion() throws Exception {
    initManagement(true);
    String regionName = getName();
    int numIndexes = 5;
    for (VM vm : getManagedNodeList()) {
      // Create indexes and region
      createIndexesAndRegion(regionName, numIndexes, vm);

      // Verify index metrics
      vm.invoke(() -> verifyAllMBeanIndexMetrics(regionName, numIndexes, numIndexes));
    }

    // Verify MBean proxies are created in the managing node
    getManagingNode().invoke(() -> verifyAllMBeanProxyIndexMetrics(regionName, numIndexes, numIndexes));
  }

  @Test
  public void testMBeanIndexMetricsCreatedTwoRegions() throws Exception {
    initManagement(true);
    String baseRegionName = getName();
    int numIndexes = 5;
    int numRegions = 2;
    for (VM vm : getManagedNodeList()) {
      // Create indexes and regions the managed VM
      for (int i=0; i<numRegions; i++) {
        String regionName = baseRegionName+i;
        createIndexesAndRegion(regionName, numIndexes, vm);
      }

      // Verify index metrics in the managed VM
      for (int i=0; i<numRegions; i++) {
        String regionName = baseRegionName+i;
        vm.invoke(() -> verifyAllMBeanIndexMetrics(regionName, numIndexes, numIndexes*numRegions));
      }
    }

    // Verify index metrics in the managing node
    for (int i=0; i<numRegions; i++) {
      String regionName = baseRegionName+i;
      getManagingNode().invoke(() -> verifyAllMBeanProxyIndexMetrics(regionName, numIndexes, numIndexes*numRegions));
    }
  }

  @Test
  public void testMBeanIndexMetricsValues() throws Exception {
    initManagement(true);
    String regionName = getName();
    int numIndexes = 1;
    for (VM vm : getManagedNodeList()) {
      // Create indexes and region
      createIndexesAndRegion(regionName, numIndexes, vm);
    }

    // Put objects with field0
    int numPuts = 10;
    getManagedNodeList().get(0).invoke(() -> putEntries(regionName, numPuts));

    // Query objects with field0
    String indexName = INDEX_NAME+"_"+0;
    getManagedNodeList().get(0).invoke(() -> queryEntries(regionName, indexName));

    // Wait for the managed members to be updated a few times in the manager node
    getManagingNode().invoke(() -> waitForMemberProxiesToRefresh(2));

    // Verify index metrics
    getManagingNode().invoke(() -> verifyMBeanIndexMetricsValues(regionName, indexName, numPuts,
        113/*1 query per bucket*/, 1/*1 result*/));
  }

  private static void waitForMemberProxiesToRefresh(int refreshCount) {
    Set<DistributedMember> members = GemFireCacheImpl.getInstance().getDistributionManager()
        .getOtherNormalDistributionManagerIds();
    // Currently, the LuceneServiceMBean is not updated in the manager since it has no getters,
    // so use the MemberMBean instead.
    for (DistributedMember member : members) {
      ObjectName memberMBeanName = getManagementService().getMemberMBeanName(member);
      // Wait for the MemberMBean proxy to be created
      waitForProxy(memberMBeanName, MemberMXBean.class);
      // Wait for the MemberMBean proxy to be updated refreshCount times.
      waitForRefresh(refreshCount, memberMBeanName);
    }
  }

  private static void verifyMBean() {
    getMBean();
  }

  private static LuceneServiceMXBean getMBean() {
    ObjectName objectName = MBeanJMXAdapter.getCacheServiceMBeanName(ds.getDistributedMember(), "LuceneService");
    assertNotNull(getManagementService().getMBeanInstance(objectName, LuceneServiceMXBean.class));
    return getManagementService().getMBeanInstance(objectName, LuceneServiceMXBean.class);
  }

  private static void verifyMBeanProxies() {
    Set<DistributedMember> members = GemFireCacheImpl.getInstance().getDistributionManager()
        .getOtherNormalDistributionManagerIds();
    for (DistributedMember member : members) {
      getMBeanProxy(member);
    }
  }

  private static LuceneServiceMXBean getMBeanProxy(DistributedMember member) {
    SystemManagementService service = (SystemManagementService) getManagementService();
    ObjectName objectName = MBeanJMXAdapter.getCacheServiceMBeanName(member, "LuceneService");
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> assertNotNull(service.getMBeanProxy(objectName, LuceneServiceMXBean.class)));
    return service.getMBeanProxy(objectName, LuceneServiceMXBean.class);
  }

  private void createIndexesAndRegion(String regionName, int numIndexes, VM vm) {
    // Create indexes
    vm.invoke(() -> createIndexes(regionName, numIndexes));

    // Create region
    createPartitionRegion(vm, regionName);
  }

  private static void createIndexes(String regionName, int numIndexes) {
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    for (int i=0; i<numIndexes; i++) {
      luceneService.createIndex(INDEX_NAME+"_"+i, regionName, "field"+i);
    }
  }

  private static void verifyAllMBeanIndexMetrics(String regionName, int numRegionIndexes, int numTotalIndexes) {
    LuceneServiceMXBean mbean = getMBean();
    verifyMBeanIndexMetrics(mbean, regionName, numRegionIndexes, numTotalIndexes);
  }

  private static void verifyAllMBeanProxyIndexMetrics(String regionName, int numRegionIndexes, int numTotalIndexes) {
    Set<DistributedMember> members = GemFireCacheImpl.getInstance().getDistributionManager()
        .getOtherNormalDistributionManagerIds();
    for (DistributedMember member : members) {
      LuceneServiceMXBean mbean = getMBeanProxy(member);
      verifyMBeanIndexMetrics(mbean, regionName, numRegionIndexes, numTotalIndexes);
    }
  }

  private static void verifyMBeanIndexMetrics(LuceneServiceMXBean mbean, String regionName, int numRegionIndexes, int numTotalIndexes) {
    assertEquals(numTotalIndexes, mbean.listIndexMetrics().length);
    assertEquals(numRegionIndexes, mbean.listIndexMetrics(regionName).length);
    for (int i=0; i<numRegionIndexes; i++) {
      assertNotNull(mbean.listIndexMetrics(regionName, INDEX_NAME+"_"+i));
    }
  }

  private static void putEntries(String regionName, int numEntries) {
    for (int i=0; i<numEntries; i++) {
      Region region = cache.getRegion(regionName);
      String key = String.valueOf(i);
      Object value = new TestObject(key);
      region.put(key, value);
    }
  }

  private static void queryEntries(String regionName, String indexName) throws LuceneQueryException {
    LuceneService service = LuceneServiceProvider.get(cache);
    LuceneQuery query = service.createLuceneQueryFactory().create(indexName, regionName, "field0:0", null);
    query.findValues();
  }

  private void verifyMBeanIndexMetricsValues(String regionName, String indexName, int expectedPuts,
      int expectedQueries, int expectedHits) {
    // Get index metrics from all members
    Set<DistributedMember> members = GemFireCacheImpl.getInstance().getDistributionManager()
        .getOtherNormalDistributionManagerIds();
    int totalCommits=0, totalUpdates=0, totalDocuments=0, totalQueries=0, totalHits=0;
    for (DistributedMember member : members) {
      LuceneServiceMXBean mbean = getMBeanProxy(member);
      LuceneIndexMetrics metrics = mbean.listIndexMetrics(regionName, indexName);
      assertNotNull(metrics);
      totalCommits += metrics.getCommits();
      totalUpdates += metrics.getUpdates();
      totalDocuments += metrics.getDocuments();
      totalQueries += metrics.getQueryExecutions();
      totalHits += metrics.getQueryExecutionTotalHits();
    }

    // Verify index metrics counts
    assertEquals(expectedPuts, totalCommits);
    assertEquals(expectedPuts, totalUpdates);
    assertEquals(expectedPuts, totalDocuments);
    assertEquals(expectedQueries, totalQueries);
    assertEquals(expectedHits, totalHits);
  }

  protected static class TestObject implements Serializable {
    private static final long serialVersionUID = 1L;
    private String field0;

    public TestObject(String value) {
      this.field0 = value;
    }

    public String toString() {
      return new StringBuilder()
          .append(getClass().getSimpleName())
          .append("[")
          .append("field0=")
          .append(this.field0)
          .append("]")
          .toString();
    }
  }
}
