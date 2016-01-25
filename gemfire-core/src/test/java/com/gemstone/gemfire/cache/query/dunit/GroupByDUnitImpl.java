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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.functional.GroupByTestImpl;
import com.gemstone.gemfire.cache.query.functional.GroupByTestInterface;
import com.gemstone.gemfire.cache.query.functional.NonDistinctOrderByTestImplementation;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * @author ashahid
 *
 */
public abstract class GroupByDUnitImpl extends CacheTestCase implements GroupByTestInterface{


  public GroupByDUnitImpl(String name) {
    super(name);
  }

  protected abstract GroupByTestInterface createTestInstance();

  @Override
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
