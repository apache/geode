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
package org.apache.geode.cache.query.dunit;

import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.functional.GroupByTestImpl;
import org.apache.geode.cache.query.functional.GroupByTestInterface;
import org.apache.geode.cache.query.functional.NonDistinctOrderByTestImplementation;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * 
 *
 */
public abstract class GroupByDUnitImpl extends JUnit4CacheTestCase implements GroupByTestInterface{


  protected abstract GroupByTestInterface createTestInstance();

  @Override
  @Test
  public void testAggregateFuncAvg() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncAvg();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testAggregateFuncAvgDistinct() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncAvgDistinct();;
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testAggregateFuncCountDistinctStar_1()
      throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncCountDistinctStar_1();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testAggregateFuncCountDistinctStar_2()
      throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncCountDistinctStar_2();;
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testAggregateFuncCountStar()
      throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncCountStar();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testAggregateFuncMax()
      throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncMax();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testAggregateFuncMin() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncMin();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testAggregateFuncNoGroupBy() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncNoGroupBy();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testAggregateFuncSum() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncSum();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testAggregateFuncSumDistinct() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncSumDistinct();
    this.closeCache(vm0, vm1, vm2, vm3);
  }
  
  @Override
  @Test
  public void testConvertibleGroupByQuery_1() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testConvertibleGroupByQuery_1();
    this.closeCache(vm0, vm1, vm2, vm3);
  }
  
  @Override
  @Test
  public void testConvertibleGroupByQuery_refer_column() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testConvertibleGroupByQuery_refer_column();
    this.closeCache(vm0, vm1, vm2, vm3);
  }
  
  @Override
  @Test
  public void testConvertibleGroupByQuery_refer_column_alias_Bug520141() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testConvertibleGroupByQuery_refer_column_alias_Bug520141();
    this.closeCache(vm0, vm1, vm2, vm3);
  }
  
  @Override
  @Test
  public void testSumWithMultiColumnGroupBy() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testSumWithMultiColumnGroupBy();
    this.closeCache(vm0, vm1, vm2, vm3);
  }
  
  @Override
  @Test
  public void testComplexValueAggregateFuncAvgDistinct() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testComplexValueAggregateFuncAvgDistinct();
    this.closeCache(vm0, vm1, vm2, vm3);
  }
  
  @Override
  @Test
  public void testAggregateFuncWithOrderBy() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testAggregateFuncWithOrderBy();
    this.closeCache(vm0, vm1, vm2, vm3);
  }
  
  @Override
  @Test
  public void testCompactRangeIndex() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testCompactRangeIndex();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Override
  @Test
  public void testDistinctCountWithoutGroupBy() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testDistinctCountWithoutGroupBy();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  public void testLimitWithGroupBy() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    GroupByTestInterface test = createTestInstance();
    test.testLimitWithGroupBy();
    this.closeCache(vm0, vm1, vm2, vm3);
  }
  
  protected void createIndex(VM vm, final String indexName,
      final String indexedExpression, final String regionPath) {
    vm.invoke(new SerializableRunnable("create index") {
      public void run() {
        try {
          Cache cache = getCache();
          cache.getQueryService().createIndex(indexName, indexedExpression,
              regionPath);
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
