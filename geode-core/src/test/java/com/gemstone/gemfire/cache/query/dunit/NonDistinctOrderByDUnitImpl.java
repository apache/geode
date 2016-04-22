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
import com.gemstone.gemfire.cache.query.functional.NonDistinctOrderByTestImplementation;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public abstract class NonDistinctOrderByDUnitImpl extends CacheTestCase {

  public NonDistinctOrderByDUnitImpl(String name) {
    super(name);
  }

  protected abstract NonDistinctOrderByTestImplementation createTestInstance();

  public void testOrderByWithIndexResultDefaultProjection() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testOrderByWithIndexResultDefaultProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  public void testOrderByWithIndexResultWithProjection() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testOrderByWithIndexResultWithProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  public void testMultiColOrderByWithIndexResultDefaultProjection()
      throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testMultiColOrderByWithIndexResultDefaultProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  public void testMultiColOrderByWithIndexResultWithProjection()
      throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testMultiColOrderByWithIndexResultWithProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  public void testMultiColOrderByWithMultiIndexResultDefaultProjection()
      throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testMultiColOrderByWithMultiIndexResultDefaultProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  public void testMultiColOrderByWithMultiIndexResultProjection()
      throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testMultiColOrderByWithMultiIndexResultProjection();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  public void testLimitNotAppliedIfOrderByNotUsingIndex() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testLimitNotAppliedIfOrderByNotUsingIndex();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  public void testOrderByWithNullValues() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testOrderByWithNullValues();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  public void testOrderByWithNullValuesUseIndex() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testOrderByWithNullValuesUseIndex();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  public void testOrderByForUndefined() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    NonDistinctOrderByTestImplementation test = createTestInstance();
    test.testOrderByForUndefined();
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

  protected void createIndex(VM vm, final String indexName,
      IndexType indexType, final String indexedExpression,
      final String fromClause) {
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
          cache.getQueryService().createIndex(indexName, indxType,
              indexedExpression, fromClause);
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

  protected Index createIndexOnAccessor(final String indexName,
      final String indexedExpression, final String regionPath) {

    try {
      Cache cache = getCache();
      return cache.getQueryService().createIndex(indexName, indexedExpression,
          regionPath);
    } catch (RegionNotFoundException e) {
      fail(e.toString());
    } catch (IndexExistsException e) {
      fail(e.toString());
    } catch (IndexNameConflictException e) {
      fail(e.toString());
    }
    return null;

  }

  protected Index createIndexOnAccessor(final String indexName,
      IndexType indexType, final String indexedExpression,
      final String fromClause) {

    try {
      Cache cache = getCache();
      return cache.getQueryService().createIndex(indexName, indexType,
          indexedExpression, fromClause);
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
