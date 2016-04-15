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

import org.junit.Test;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.functional.GroupByTestImpl;
import com.gemstone.gemfire.cache.query.functional.GroupByTestInterface;
import com.gemstone.gemfire.cache.query.functional.NonDistinctOrderByTestImplementation;
import com.gemstone.gemfire.cache.query.functional.UDATestInterface;
import com.gemstone.gemfire.cache30.CacheTestCase;

import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * @author ashahid
 *
 */
public abstract class UDADUnitImpl extends CacheTestCase implements UDATestInterface{


  public UDADUnitImpl(String name) {
    super(name);
  }

  protected abstract UDATestInterface createTestInstance();

  @Test
  @Override
  public void testUDANoGroupBy() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    UDATestInterface test = createTestInstance();
    test.testUDANoGroupBy();
    this.closeCache(vm0, vm1, vm2, vm3);
  }

  @Test
  @Override
  public void testUDAWithGroupBy() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = this.getCache();
    UDATestInterface test = createTestInstance();
    test.testUDAWithGroupBy();;
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

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
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
