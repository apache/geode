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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.Iterator;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * 
 *
 */
@Category(DistributedTest.class)
public class GroupByQueryDUnitTest extends JUnit4CacheTestCase {

  @Test
  public void testConvertibleGroupByNoIndex() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    createAccessor(vm0);
    createPR(vm1);
    createPR(vm2);
    createPR(vm3);
    this.runQuery(vm0);
    this.closeCache(vm0, vm1, vm2, vm3);

  }

  private void runQuery(VM queryVM) throws Exception {
    // createIndex(vm0, "compactRangeIndex", "entry.value",
    // "/region.entrySet entry");

    // Do Puts
    queryVM.invoke(new SerializableRunnable("putting data") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("portfolio");
        for (int i = 1; i < 200; ++i) {
          Portfolio pf = new Portfolio(i);
          pf.shortID = (short) ((short) i / 5);
          region.put("" + i, pf);
        }
      }
    });

    queryVM.invoke(new SerializableRunnable("query") {
      public void run() {
        try {
          QueryService qs = getCache().getQueryService();
          String queryStr = "select  p.shortID as short_id  from /portfolio p where p.ID >= 0 group by short_id ";
          Query query = qs.newQuery(queryStr);
          SelectResults<Struct> results = (SelectResults<Struct>) query
              .execute();
          Iterator<Struct> iter = results.iterator();
          int counter = 0;
          while (iter.hasNext()) {
            Struct str = iter.next();
            assertEquals(counter++, ((Short) str.get("short_id")).intValue());
          }
          assertEquals(39, counter - 1);
        } catch (QueryInvocationTargetException e) {
          fail(e.toString());
        } catch (NameResolutionException e) {
          fail(e.toString());

        } catch (TypeMismatchException e) {
          fail(e.toString());

        } catch (FunctionDomainException e) {
          fail(e.toString());

        }

      }
    });
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

  private void createPR(VM vm) {
    vm.invoke(new SerializableRunnable("create data store") {
      public void run() {
        Cache cache = getCache();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(10);
        cache.createRegionFactory(RegionShortcut.PARTITION)
            .setPartitionAttributes(paf.create()).create("portfolio");
      }
    });
  }

  private void createAccessor(VM vm) {
    vm.invoke(new SerializableRunnable("create accessor") {

      public void run() {
        Cache cache = getCache();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(10);
        paf.setLocalMaxMemory(0);
        cache.createRegionFactory(RegionShortcut.PARTITION_PROXY)
            .setPartitionAttributes(paf.create()).create("portfolio");
      }
    });
  }

  private void createIndex(VM vm, final String indexName,
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
