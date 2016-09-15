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
import org.apache.geode.cache.query.functional.GroupByTestImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * 
 *
 */
@Category(DistributedTest.class)
public class GroupByPartitionedQueryDUnitTest extends GroupByDUnitImpl {

  @Override
  protected GroupByTestImpl createTestInstance() {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);

    GroupByTestImpl test = new GroupByTestImpl() {

      @Override
      public Region createRegion(String regionName, Class valueConstraint) {
        // TODO Auto-generated method stub
        Region rgn = createAccessor(regionName, valueConstraint);
        createPR(vm1, regionName, valueConstraint);
        createPR(vm2, regionName, valueConstraint);
        createPR(vm3, regionName, valueConstraint);
        return rgn;
      }
    };
    return test;
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

  private void createPR(VM vm, final String regionName,
      final Class valueConstraint) {
    vm.invoke(new SerializableRunnable("create data store") {
      public void run() {
        Cache cache = getCache();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(10);
        cache.createRegionFactory(RegionShortcut.PARTITION)
            .setValueConstraint(valueConstraint)
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
        .setValueConstraint(valueConstraint)
        .setPartitionAttributes(paf.create()).create(regionName);
  }


}
