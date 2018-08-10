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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.test.dunit.Assert.fail;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.functional.OrderByPartitionedJUnitTestBase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class OrderByPartitionedDUnitTest extends JUnit4CacheTestCase {

  private OrderByPartitionedJUnitTestBase createTestInstance() {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);

    OrderByPartitionedJUnitTestBase test = new OrderByPartitionedJUnitTestBase() {
      @Override
      public Region createRegion(String regionName, Class valueConstraint) {
        // TODO Auto-generated method stub
        Region rgn = createAccessor(regionName, valueConstraint);
        createPR(vm1, regionName, valueConstraint);
        createPR(vm2, regionName, valueConstraint);
        createPR(vm3, regionName, valueConstraint);
        return rgn;
      }

      @Override
      public Index createIndex(String indexName, String indexedExpression, String regionPath)
          throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
          RegionNotFoundException, UnsupportedOperationException {
        Index indx = createIndexOnAccessor(indexName, indexedExpression, regionPath);
        return indx;
      }

      @Override
      public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
          String fromClause) throws IndexInvalidException, IndexNameConflictException,
          IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
        Index indx = createIndexOnAccessor(indexName, indexType, indexedExpression, fromClause);
        return indx;
      }

      @Override
      public boolean assertIndexUsedOnQueryNode() {
        return false;
      }
    };
    return test;
  }

  @Test
  public void testOrderByWithIndexResultDefaultProjection() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testOrderByWithIndexResultDefaultProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testOrderByWithIndexResultWithProjection() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testOrderByWithIndexResultWithProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testMultiColOrderByWithIndexResultDefaultProjection() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testMultiColOrderByWithIndexResultDefaultProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testMultiColOrderByWithIndexResultWithProjection() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testMultiColOrderByWithIndexResultWithProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testMultiColOrderByWithMultiIndexResultDefaultProjection() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testMultiColOrderByWithMultiIndexResultDefaultProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testMultiColOrderByWithMultiIndexResultProjection() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testMultiColOrderByWithMultiIndexResultProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testLimitNotAppliedIfOrderByNotUsingIndex() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testLimitNotAppliedIfOrderByNotUsingIndex();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testOrderByWithNullValuesUseIndex() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testOrderByWithNullValuesUseIndex();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testOrderByForUndefined() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testOrderByForUndefined();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testOrderedResultsPartitionedRegion_Bug43514_1() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testOrderedResultsPartitionedRegion_Bug43514_1();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testOrderedResultsPartitionedRegion_Bug43514_2() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testOrderedResultsPartitionedRegion_Bug43514_2();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testOrderByWithNullValues() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    OrderByPartitionedJUnitTestBase test = createTestInstance();
    test.testOrderByWithNullValues();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  private void createBuckets(VM vm) {
    vm.invoke(new SerializableRunnable("create accessor") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        for (int i = 0; i < 10; i++) {
          region.put(i, i);
        }
      }
    });
  }

  private void createPR(VM vm, final String regionName, final Class valueConstraint) {
    vm.invoke(new SerializableRunnable("create data store") {
      public void run() {
        Cache cache = getCache();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(10);
        cache.createRegionFactory(RegionShortcut.PARTITION).setValueConstraint(valueConstraint)
            .setPartitionAttributes(paf.create()).create(regionName);
      }
    });
  }

  private Region createAccessor(String regionName, Class valueConstraint) {
    Cache cache = getCache();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(10);
    paf.setLocalMaxMemory(0);
    return cache.createRegionFactory(RegionShortcut.PARTITION_PROXY)
        .setValueConstraint(valueConstraint).setPartitionAttributes(paf.create())
        .create(regionName);
  }

  private void createIndex(VM vm, final String indexName, final String indexedExpression,
      final String regionPath) {
    vm.invoke(new SerializableRunnable("create index") {
      public void run() {
        try {
          Cache cache = getCache();
          cache.getQueryService().createIndex(indexName, indexedExpression, regionPath);
        } catch (RegionNotFoundException e) {
          fail(e.toString());
        } catch (IndexExistsException e) {
          fail(e.toString());
        } catch (IndexNameConflictException e) {
          fail(e.toString());
        }
      }
    });
  }

  private void createIndex(VM vm, final String indexName, IndexType indexType,
      final String indexedExpression, final String fromClause) {
    int indxTypeCode = -1;
    if (indexType.equals(IndexType.FUNCTIONAL)) {
      indxTypeCode = 0;
    } else if (indexType.equals(IndexType.PRIMARY_KEY)) {
      indxTypeCode = 1;
    } else if (indexType.equals(IndexType.HASH)) {
      indxTypeCode = 2;
    }
    final int finalIndxTypeCode = indxTypeCode;
    vm.invoke(new SerializableRunnable("create index") {
      public void run() {
        try {
          Cache cache = getCache();
          IndexType indxType = null;
          if (finalIndxTypeCode == 0) {
            indxType = IndexType.FUNCTIONAL;
          } else if (finalIndxTypeCode == 1) {
            indxType = IndexType.PRIMARY_KEY;
          } else if (finalIndxTypeCode == 2) {
            indxType = IndexType.HASH;
          }
          cache.getQueryService().createIndex(indexName, indxType, indexedExpression, fromClause);
        } catch (RegionNotFoundException e) {
          fail(e.toString());
        } catch (IndexExistsException e) {
          fail(e.toString());
        } catch (IndexNameConflictException e) {
          fail(e.toString());
        }
      }
    });
  }

  private Index createIndexOnAccessor(final String indexName, final String indexedExpression,
      final String regionPath) {
    try {
      Cache cache = getCache();
      return cache.getQueryService().createIndex(indexName, indexedExpression, regionPath);
    } catch (RegionNotFoundException e) {
      fail(e.toString());
    } catch (IndexExistsException e) {
      fail(e.toString());
    } catch (IndexNameConflictException e) {
      fail(e.toString());
    }
    return null;
  }

  private Index createIndexOnAccessor(final String indexName, IndexType indexType,
      final String indexedExpression, final String fromClause) {
    try {
      Cache cache = getCache();
      return cache.getQueryService().createIndex(indexName, indexType, indexedExpression,
          fromClause);
    } catch (RegionNotFoundException e) {
      fail(e.toString());
    } catch (IndexExistsException e) {
      fail(e.toString());
    } catch (IndexNameConflictException e) {
      fail(e.toString());
    }
    return null;
  }

  private void closeCache(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableRunnable() {
        public void run() {
          getCache().close();
        }
      });
    }
  }



}
