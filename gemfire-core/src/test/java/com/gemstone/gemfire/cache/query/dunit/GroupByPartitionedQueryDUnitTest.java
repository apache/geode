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

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.functional.GroupByTestImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * 
 * @author ashahid
 *
 */
@Category(DistributedTest.class)
public class GroupByPartitionedQueryDUnitTest extends GroupByDUnitImpl {

  public GroupByPartitionedQueryDUnitTest(String name) {
    super(name);
  }

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
